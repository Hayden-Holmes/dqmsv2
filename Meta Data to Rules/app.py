import json
import os

from azure.identity import DefaultAzureCredential, get_bearer_token_provider
from fastapi import FastAPI
from pathlib import Path

PROMPTS_DIR = Path("prompts")
EXAMPLES_DIR = Path("examples")
BASE_URL=os.getenv("BASE_URL")

prompts = {
    "gen_rules": (PROMPTS_DIR / "gen_rules.txt").read_text(),
    "normalize_metadata": (PROMPTS_DIR / "normalize_metadata-v3.txt").read_text(),
    "normalize_profile": (PROMPTS_DIR / "normalize_profile.txt").read_text(),
}

examples = {
    "gen_rules": (EXAMPLES_DIR / "gen_rules_examples.txt").read_text(),
    # "normalize_profile": (EXAMPLES_DIR / "normalize_profile_examples.txt").read_text(),
}


app = FastAPI()

"""
Accepts metadata in any format and converts it into a
canonical metadata model using an LLM.
"""
@app.post("/normalize_metadata")
def normalize_metadata(request: str):
    print("normalizing", request)
    prompt = prompts["normalize_metadata"] + request

    return call_llm(prompt)



"""
Normalize profiling results from external profiling input

Responsibilities:
- Normalize metrics
- Standardize column statistics

"""
@app.post("/normalize_profile")
def normalize_profile(request: str):

    print("Normalize Profile Request Received")

    prompt = prompts["normalize_profile"] + request
    

    response = call_llm(prompt)
    return response


"""
Generate platform-neutral data quality rules from normalized
metadata and optional profiling statistics.

The LLM never receives raw data records.

Inputs:
- Normalized metadata (required)
- Normalized profile (optional)

Responsibilities:
- Understand column semantics
- Analyze business context
- Analyze profiling metrics
- Generate validation rules
- Generate rule rationale
"""


@app.post("/generate_rules")
def generate_rules(request: str):

    print("Generate Rules Request Received")

    prompt = prompts["gen_rules"]+examples["gen_rules"] + request

    response = call_llm(prompt)

    return response


import json
from openai import OpenAI

@app.post("/generate_rules_from_source")
def generate_rules_from_source(request: str):

    return orchestrate_rule_generation(
        metadata=request
    )

def orchestrate_rule_generation(
    metadata: str,
    profile: str | None = None
):

    normalized_metadata = normalize_metadata(metadata)

    normalized_profile = None

    if profile:
        normalized_profile = normalize_profile(profile)

    payload = {
        "metadata": normalized_metadata,
        "profile": normalized_profile
    }

    return generate_rules(
        json.dumps(payload)
    )


token_provider = get_bearer_token_provider(
    DefaultAzureCredential(),
    "https://cognitiveservices.azure.com/.default"
)

client = OpenAI(
    base_url=BASE_URL,
    api_key=token_provider
)

DEPLOYMENT_NAME = "gpt-4.1-nano"
def call_llm(prompt: str) -> dict:
    print("Calling llm")

    response = client.chat.completions.create(
        model=DEPLOYMENT_NAME,
        messages=[
            {
                "role": "system",
                "content": "You are a data quality expert."
            },
            {
                "role": "user",
                "content": prompt
            }
        ],
        temperature=0
    )
    content = response.choices[0].message.content
    print("output\n",content)

    if content is None:
        raise ValueError("Model returned no content")

    content = content.replace("```json", "")
    content = content.replace("```", "")
    content = content.strip()
    return json.loads(content)