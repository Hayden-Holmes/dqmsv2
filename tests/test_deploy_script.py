"""The Cloud Run script generates its API key in plain bash. Prove that key file really works."""

import re
import shutil
import subprocess
from pathlib import Path

import pytest

from dqms.auth import AuthError, Authenticator
from dqms.logging_config import JsonFormatter

SCRIPT = Path(__file__).parent.parent / "deploy" / "cloudrun.sh"
bash = shutil.which("bash")


def keygen_block() -> str:
    text = SCRIPT.read_text()
    start = text.index('step "3/6')
    end = text.index('step "4/6')
    return text[start:end]


@pytest.mark.skipif(bash is None, reason="needs bash")
def test_script_syntax_is_valid():
    assert subprocess.run([bash, "-n", str(SCRIPT)]).returncode == 0


@pytest.mark.skipif(bash is None, reason="needs bash")
def test_bash_generated_key_file_is_accepted_by_the_service(tmp_path):
    keyfile = tmp_path / "keys.yaml"
    program = f'''
        set -euo pipefail
        step() {{ :; }}
        DATA_PROJECTS="proj-one proj-two"
        KEYFILE="{keyfile}"
        {keygen_block()}
        printf %s "$KEY"
    '''
    key = subprocess.run([bash, "-c", program], capture_output=True, text=True, check=True).stdout
    assert re.fullmatch(r"dqms_[A-Za-z0-9_-]{40,}", key), key

    auth = Authenticator(Authenticator.load_keys(keyfile))
    who = auth.authenticate(f"Bearer {key}")
    assert who.name == "default" and who.sources == ("bigquery://proj-one/*", "bigquery://proj-two/*")
    assert who.can_read("bigquery://proj-one/sales/orders") and not who.can_read("bigquery://other/ds/t")
    with pytest.raises(AuthError):
        auth.authenticate("Bearer " + key[:-1] + ("A" if key[-1] != "A" else "B"))
    assert key not in keyfile.read_text()  # only the fingerprint is ever written down


def test_script_never_writes_the_key_into_the_repo_or_leaves_the_key_file():
    text = SCRIPT.read_text()
    assert "mktemp" in text and "trap 'rm -f" in text
    assert "--allow-unauthenticated" in text and "--no-cpu-throttling" in text and "--max-instances 1" in text
    assert "DQMS_LLM_MODE=off" in text
    # a source build needs the build account to be able to read the uploaded source
    assert "roles/cloudbuild.builds.builder" in text and "roles/storage.objectViewer" in text


def test_logs_carry_the_severity_field_cloud_logging_reads():
    import json
    import logging

    record = logging.LogRecord("dqms", logging.WARNING, "f", 1, "careful", (), None)
    assert json.loads(JsonFormatter().format(record))["severity"] == "WARNING"


def test_script_enables_dataplex_and_grants_catalog_read_access():
    text = SCRIPT.read_text()
    assert "dataplex.googleapis.com" in text and "roles/dataplex.catalogViewer" in text
