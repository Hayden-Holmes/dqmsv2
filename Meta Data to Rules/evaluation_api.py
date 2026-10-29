import os

from fastapi import FastAPI
from pydantic import BaseModel
from azure.identity import DefaultAzureCredential, get_bearer_token_provider
from openai import OpenAI
import json

BASE_URL=os.getenv("BASE_URL")

app = FastAPI()

# Azure OpenAI Setup

token_provider = get_bearer_token_provider(
    DefaultAzureCredential(),
    "https://cognitiveservices.azure.com/.default"
)

client = OpenAI(
    base_url=BASE_URL,
    api_key=token_provider
)

DEPLOYMENT_NAME = "gpt-4.1-nano"


# Request Model

class EvaluationRequest(BaseModel):
    source_metadata: str
    normalized_metadata: dict


# LLM Helper

def call_llm(prompt: str) -> dict:
    print("Calling evaluator LLM...")

    response = client.chat.completions.create(
        model=DEPLOYMENT_NAME,
        messages=[
            {
                "role": "system",
                "content": "You are a metadata normalization evaluator."
            },
            {
                "role": "user",
                "content": prompt
            }
        ],
        temperature=0
    )

    content = response.choices[0].message.content

    if content is None:
        raise ValueError("Model returned no content")

    content = content.replace("```json", "")
    content = content.replace("```", "")
    content = content.strip()

    try:
        return json.loads(content)
    except Exception:
        print("\n===== INVALID JSON =====")
        print(content)
        print("===== END INVALID JSON =====\n")
        raise



# Endpoint

@app.post("/evaluate_normalization")
def evaluate_normalization(request: EvaluationRequest):

    prompt = f"""
You are a Metadata Normalization Evaluator.

Evaluate whether the normalized metadata accurately preserves and standardizes the source metadata.

Source Metadata:
{request.source_metadata}

Normalized Metadata:
{json.dumps(request.normalized_metadata, indent=2)}

Evaluation Criteria

1. Completeness
- Were all important metadata elements preserved?
- Tables
- Columns
- Datatypes
- Constraints
- PK/FK relationships
- Comments
- Glossary terms
- Profiling data
- Ownership
- Lineage
- Classifications
- Semantic information

2. Fidelity
- Does the normalized metadata accurately represent the source?

3. Information Preservation
- Was any useful metadata lost?

4. Normalization Quality
- Is information consistently represented?
- Are duplicate concepts consolidated?

5. Semantic Extraction
- Were useful semantic meanings extracted correctly?

6. Rule Generation Readiness
- Could a downstream rule-generation agent derive:
  - NOT NULL rules
  - Uniqueness rules
  - Referential integrity rules
  - Length rules
  - Datatype rules
  - Enumerations
  - Cross-column validations

Estimate:

- source_elements
- preserved_elements
- lost_elements

Calculate:

preservation_rate =
(preserved_elements / source_elements) * 100

Return ONLY valid JSON:

{{
  "overall_score": 0,
  "source_elements": 0,
  "preserved_elements": 0,
  "lost_elements": 0,
  "preservation_rate": 0,
  "scores": {{
    "completeness_score": 0,
    "fidelity_score": 0,
    "information_preservation_score": 0,
    "normalization_quality_score": 0,
    "semantic_extraction_score": 0,
    "rule_generation_readiness_score": 0
  }},
  "strengths": [],
  "weaknesses": [],
  "missing_information": [],
  "incorrect_transformations": [],
  "rule_generation_gaps": [],
  "summary": ""
}}
"""

    try:
        result = call_llm(prompt)
        return result

    except Exception as e:
        return {
            "error": str(e)
        }
    
class PromptOptimizerRequest(BaseModel):
    current_prompt: str
    evaluations: list


@app.post("/optimize_prompt")
def optimize_prompt(request: PromptOptimizerRequest):

    prompt = f"""
    You are a Metadata Normalization Prompt Optimization Expert.

    Your job is to improve metadata normalization prompts.

    You will receive:

    1. The current prompt.
    2. Evaluation reports from multiple normalization tests.

    Analyze the evaluations and identify:

    - Common failures
    - Information loss patterns
    - Incorrect transformations
    - Missing metadata categories
    - Semantic extraction weaknesses
    - Rule generation gaps

    Focus on recurring issues across evaluations.

    Determine:

    1. What the prompt does well.
    2. What the prompt does poorly.
    3. Why the failures occurred.
    4. What changes would improve normalization quality.
    5. Produce an improved prompt.

    The revised prompt should:

    - Preserve all strengths.
    - Fix identified weaknesses.
    - Improve metadata preservation.
    - Improve semantic extraction.
    - Improve rule-generation readiness.

    Return ONLY valid JSON matching this schema:

    {{
    "summary": "",
    "root_causes": [],
    "common_failures": [],
    "recommended_changes": [],
    "revised_prompt": ""
    }}

    Current Prompt:

    {request.current_prompt}

    Evaluation Reports:

    {json.dumps(request.evaluations, indent=2)}
    """
    try:
        return call_llm(prompt)
    except Exception as e:
        print("LLM FAILURE")
        raise e