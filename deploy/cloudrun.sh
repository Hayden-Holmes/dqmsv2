#!/usr/bin/env bash
# Deploy dqms to Google Cloud Run, then print its URL and an API key.
#
# Run from the repo folder in Google Cloud Shell (it already has gcloud, your login and openssl):
#     bash deploy/cloudrun.sh
#
# Optional settings (put them in front of the command):
#     PROJECT=my-proj            project the service runs in            (default: your selected project)
#     DATA_PROJECTS="a b"        BigQuery projects it may read          (default: PROJECT)
#     REGION=europe-west1        where it runs                          (default: us-central1)
#     MAX_BYTES=10000000000      most one BigQuery profiling query may bill (default: 10 GB)
#     MAX_DATASETS=100           most tables one run may analyze         (default: 100)
#
# Re-running makes a NEW key (the old one stops working) and redeploys the latest code.
set -euo pipefail

PROJECT="${PROJECT:-$(gcloud config get-value project 2>/dev/null || true)}"
DATA_PROJECTS="${DATA_PROJECTS:-$PROJECT}"
REGION="${REGION:-us-central1}"
SERVICE="${SERVICE:-dqms}"
MAX_BYTES="${MAX_BYTES:-10000000000}"
MAX_DATASETS="${MAX_DATASETS:-100}"
SECRET="${SECRET:-dqms-keys}"
SA_NAME="dqms-runner"

if [[ -z "$PROJECT" || "$PROJECT" == "(unset)" ]]; then
  echo "No project selected. Run:  gcloud config set project YOUR-PROJECT-ID   (then run this again)" >&2
  exit 1
fi
if [[ ! -f Dockerfile || ! -d src/dqms ]]; then
  echo "Run this from the dqmsv2 folder (the one containing Dockerfile and src/)." >&2
  exit 1
fi
SA="$SA_NAME@$PROJECT.iam.gserviceaccount.com"
KEYFILE="$(mktemp)"
trap 'rm -f "$KEYFILE"' EXIT   # the key file never stays on disk, and is never inside the repo

step() { printf '\n==> %s\n' "$*"; }

step "1/6 Enabling the Google Cloud services this needs (first time takes a minute)"
gcloud services enable run.googleapis.com cloudbuild.googleapis.com secretmanager.googleapis.com \
  artifactregistry.googleapis.com dataplex.googleapis.com --project "$PROJECT"

step "2/6 Creating a service account that can only read BigQuery and its catalog"
gcloud iam service-accounts describe "$SA" --project "$PROJECT" >/dev/null 2>&1 \
  || gcloud iam service-accounts create "$SA_NAME" --display-name "dqms service" --project "$PROJECT"
for p in $DATA_PROJECTS; do
  for role in roles/bigquery.dataViewer roles/bigquery.jobUser roles/dataplex.catalogViewer; do
    gcloud projects add-iam-policy-binding "$p" --member "serviceAccount:$SA" --role "$role" \
      --condition=None --quiet >/dev/null
  done
  echo "    can read BigQuery in: $p"
done

step "3/6 Creating your API key (only its fingerprint is stored)"
KEY="dqms_$(openssl rand -base64 36 | tr '+/' '-_' | tr -d '=\n')"
HASH="$(printf %s "$KEY" | { sha256sum 2>/dev/null || shasum -a 256; } | cut -d' ' -f1)"
SOURCES=""
for p in $DATA_PROJECTS; do SOURCES+="${SOURCES:+, }\"bigquery://$p/*\""; done
cat > "$KEYFILE" <<YAML
keys:
  - name: "default"
    sha256: "$HASH"
    sources: [$SOURCES]
YAML

step "4/6 Storing the key file in Secret Manager"
if gcloud secrets describe "$SECRET" --project "$PROJECT" >/dev/null 2>&1; then
  gcloud secrets versions add "$SECRET" --data-file "$KEYFILE" --project "$PROJECT" >/dev/null
else
  gcloud secrets create "$SECRET" --data-file "$KEYFILE" --replication-policy automatic --project "$PROJECT" >/dev/null
fi
gcloud secrets add-iam-policy-binding "$SECRET" --member "serviceAccount:$SA" \
  --role roles/secretmanager.secretAccessor --project "$PROJECT" --quiet >/dev/null

step "5/6 Building and deploying (the first build takes a few minutes)"
# Cloud Build builds your uploaded source as the Compute Engine default service account. On many
# projects that account has no permissions yet, which shows up as "does not have storage.objects.get
# access to the Google Cloud Storage object". Grant it what a source build needs.
PROJECT_NUMBER="$(gcloud projects describe "$PROJECT" --format 'value(projectNumber)')"
BUILD_SA="$PROJECT_NUMBER-compute@developer.gserviceaccount.com"
for role in roles/cloudbuild.builds.builder roles/run.builder; do
  gcloud projects add-iam-policy-binding "$PROJECT" --member "serviceAccount:$BUILD_SA" --role "$role" \
    --condition=None --quiet >/dev/null 2>&1 || echo "    (could not grant $role to the build account; continuing)"
done
# The uploaded source lives in this bucket. Create it now so the build account can read it on the first try.
SOURCE_BUCKET="run-sources-$PROJECT-$REGION"
gcloud storage buckets describe "gs://$SOURCE_BUCKET" --project "$PROJECT" >/dev/null 2>&1 \
  || gcloud storage buckets create "gs://$SOURCE_BUCKET" --location "$REGION" --project "$PROJECT" >/dev/null
gcloud storage buckets add-iam-policy-binding "gs://$SOURCE_BUCKET" --member "serviceAccount:$BUILD_SA" \
  --role roles/storage.objectViewer --project "$PROJECT" >/dev/null || true
# One always-on instance: jobs run in the background after the reply and keep their state in memory.
gcloud run deploy "$SERVICE" --source . --project "$PROJECT" --region "$REGION" --quiet \
  --service-account "$SA" --allow-unauthenticated \
  --min-instances 1 --max-instances 1 --no-cpu-throttling --memory 1Gi \
  --set-secrets "/etc/dqms/keys.yaml=$SECRET:latest" \
  --set-env-vars "DQMS_AUTH_FILE=/etc/dqms/keys.yaml,DQMS_BQ_MAX_BYTES_BILLED=$MAX_BYTES,DQMS_MAX_DATASETS=$MAX_DATASETS,DQMS_LLM_MODE=off"

URL="$(gcloud run services describe "$SERVICE" --project "$PROJECT" --region "$REGION" --format 'value(status.url)')"

step "6/6 Checking it works"
if curl -fsS "$URL/healthz" >/dev/null && curl -fsS -H "X-API-Key: $KEY" "$URL/v1/explorer/roots"; then
  echo
else
  cat >&2 <<NOTE

The service deployed but did not answer from the public internet. If the error above says 403 or
"Forbidden", your organisation blocks public Cloud Run services. Use it privately from Cloud Shell:
    gcloud run services proxy $SERVICE --project $PROJECT --region $REGION --port 8080
then open Web Preview on port 8080. (Everything below still applies, with that address as the URL.)
NOTE
fi

cat <<DONE

=====================================================================
 dqms is running.

 URL      $URL
 API key  $KEY
          ^ shown once. Save it in a password manager or your pipeline's secret store.

 In a browser:  open the URL, paste the key into "API key", pick your tables.

 From code or a pipeline (any machine, no install needed):
   curl -s -X POST "$URL/v1/sources/analyze" \\
     -H "X-API-Key: \$DQMS_KEY" -H "content-type: application/json" \\
     -d '{"source": "bigquery://$PROJECT/MY_DATASET/MY_TABLE"}'

 Remove it:  gcloud run services delete $SERVICE --project $PROJECT --region $REGION
=====================================================================
DONE
