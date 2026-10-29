from pathlib import Path
from urllib import response
import requests
import json

TEST_DIR = Path("./tests")
OUTPUT_DIR = Path("./test_results")

OUTPUT_DIR.mkdir(exist_ok=True)

URL = "http://127.0.0.1:8000/normalize_metadata"


def run_tests():
    for file in TEST_DIR.iterdir():

        if not file.is_file():
            continue

        try:
            print(f"\nTesting {file.name}")

            payload = file.read_text(encoding="utf-8")

            response = requests.post(
                URL,
                params={"request": payload}
            )
            print(response.status_code)
            print(response.text)

            response.raise_for_status()

            output_file = OUTPUT_DIR / f"{file.stem}_normalized.json"

            try:
                result = response.json()

                with open(output_file, "w", encoding="utf-8") as f:
                    json.dump(result, f, indent=2)

            except Exception:
                with open(output_file, "w", encoding="utf-8") as f:
                    f.write(response.text)

            print(f"Saved: {output_file}")

        except Exception as e:

            error_file = OUTPUT_DIR / f"{file.stem}_error.txt"

            with open(error_file, "w", encoding="utf-8") as f:
                f.write(str(e))

            print(f"Failed: {file.name}")
            print(e)


if __name__ == "__main__":
    run_tests()