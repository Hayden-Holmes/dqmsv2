from google.cloud import bigquery


def extract_gcp_context(
    project_id: str,
    dataset_id: str,
    table_id: str
):
    """
    Build a complete context package for a BigQuery table.

    Returns:
    - Metadata
    - Governance Metadata
    - Glossary Terms
    - Lineage
    - Profile Information
    """

    metadata = get_bigquery_metadata(
        project_id,
        dataset_id,
        table_id
    )

    governance = get_governance_metadata(
        project_id,
        dataset_id,
        table_id
    )

    glossary = get_glossary_terms(
        project_id,
        dataset_id,
        table_id
    )

    lineage = get_lineage(
        project_id,
        dataset_id,
        table_id
    )

    profile = get_profile(
        project_id,
        dataset_id,
        table_id
    )

    return {
        "metadata": metadata,
        "governance": governance,
        "glossary": glossary,
        "lineage": lineage,
        "profile": profile
    }

def get_bigquery_metadata(
    project_id: str,
    dataset_id: str,
    table_id: str
):

    client = bigquery.Client(
        project=project_id
    )

    table = client.get_table(
        f"{project_id}.{dataset_id}.{table_id}"
    )

    columns = []

    for field in table.schema:

        columns.append(
            {
                "name": field.name,
                "datatype": field.field_type,
                "mode": field.mode,
                "description": field.description
            }
        )

    return {
        "table_name": table.table_id,
        "description": table.description,
        "columns": columns
    }

def get_governance_metadata(
    project_id: str,
    dataset_id: str,
    table_id: str
):

    return {
        "owner": None,
        "steward": None,
        "classification": None,
        "criticality": None
    }

def get_glossary_terms(
    project_id: str,
    dataset_id: str,
    table_id: str
):

    return []

def get_lineage(
    project_id: str,
    dataset_id: str,
    table_id: str
):

    return {
        "upstream": [],
        "downstream": []
    }

def get_profile(
    project_id: str,
    dataset_id: str,
    table_id: str
):

    return {
        "status": "not_implemented"
    }