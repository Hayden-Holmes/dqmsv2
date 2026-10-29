from pathlib import Path
import requests
import json

TEST_DIR = Path("./tests")
NORMALIZED_DIR = Path("./test_results")
EVAL_DIR = Path("./evaluation_results")

EVAL_DIR.mkdir(exist_ok=True)

URL = "http://127.0.0.1:8001/evaluate_normalization"

summary = []


for source_file in TEST_DIR.iterdir():

    if not source_file.is_file():
        continue

    normalized_file = (
        NORMALIZED_DIR /
        f"{source_file.stem}_normalized.json"
    )

    if not normalized_file.exists():
        print(f"Missing normalized file: {normalized_file}")
        continue

    try:

        print(f"Evaluating {source_file.name}")

        source_text = source_file.read_text(
            encoding="utf-8"
        )

        normalized_metadata = json.loads(
            normalized_file.read_text(
                encoding="utf-8"
            )
        )

        payload = {
            "source_metadata": source_text,
            "normalized_metadata": normalized_metadata
        }

        response = requests.post(
            URL,
            json=payload,
            timeout=300
        )

        print(response.status_code)

        response.raise_for_status()

        result = response.json()

        output_file = (
            EVAL_DIR /
            f"{source_file.stem}_evaluation.json"
        )

        with open(
            output_file,
            "w",
            encoding="utf-8"
        ) as f:
            json.dump(
                result,
                f,
                indent=2
            )

        summary.append({
            "file": source_file.name,
            "overall_score": result.get(
                "overall_score"
            ),
            "preservation_rate": result.get(
                "preservation_rate"
            ),
            "completeness_score": result.get(
                "scores",
                {}
            ).get(
                "completeness_score"
            ),
            "rule_generation_readiness_score": result.get(
                "scores",
                {}
            ).get(
                "rule_generation_readiness_score"
            )
        })

        print(f"Saved: {output_file}")

    except Exception as e:

        error_file = (
            EVAL_DIR /
            f"{source_file.stem}_error.txt"
        )

        with open(
            error_file,
            "w",
            encoding="utf-8"
        ) as f:
            f.write(str(e))

        print(f"Failed: {source_file.name}")
        print(e)


summary_file = (
    EVAL_DIR /
    "evaluation_summary.json"
)

with open(
    summary_file,
    "w",
    encoding="utf-8"
) as f:
    json.dump(
        {
            "results": summary
        },
        f,
        indent=2
    )

print(f"\nSummary saved: {summary_file}")
