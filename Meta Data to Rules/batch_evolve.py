from pathlib import Path
import requests
import json

EVAL_DIR = Path("./evaluation_results")

URL = "http://127.0.0.1:8001/optimize_prompt"

# Load current prompt
with open("prompts/normalize_metadata.txt", "r", encoding="utf-8") as f:
    current_prompt = f.read()

# Load all evaluations
evaluations = []

for file in EVAL_DIR.glob("*_evaluation.json"):
    with open(file, "r", encoding="utf-8") as f:
        evaluations.append(json.load(f))

payload = {
    "current_prompt": current_prompt,
    "evaluations": evaluations
}

response = requests.post(
    URL,
    json=payload,
    timeout=300
)

response.raise_for_status()

result = response.json()

with open(
    "prompt_optimization.json",
    "w",
    encoding="utf-8"
) as f:
    json.dump(result, f, indent=2)

print("Saved prompt_optimization.json")