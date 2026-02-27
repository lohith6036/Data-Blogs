"""
Lambda — Natural Language to Athena SQL Handler
Author: Lohith Kumar V

Converts plain English data questions into validated Athena SQL queries,
executes them, and returns structured results to the Bedrock agent.
"""

import boto3
import json
import time
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

athena = boto3.client("athena")
glue = boto3.client("glue")
bedrock = boto3.client("bedrock-runtime", region_name="us-east-1")

ATHENA_OUTPUT = "s3://my-athena-results/agent-queries/"
DEFAULT_DATABASE = "data_warehouse"
MODEL_ID = "anthropic.claude-3-5-sonnet-20241022-v2:0"

# Statements that must never be executed via this action group
BLOCKED_KEYWORDS = ["DROP", "DELETE", "INSERT", "UPDATE", "ALTER", "TRUNCATE", "CREATE"]


def lambda_handler(event, context):
    action_group = event["actionGroup"]
    function_name = event["function"]
    params = {p["name"]: p["value"] for p in event.get("parameters", [])}

    if function_name == "execute_nl_query":
        result = execute_nl_query(
            question=params["question"],
            database=params.get("database", DEFAULT_DATABASE),
        )
    else:
        result = {"error": f"Unknown function: {function_name}"}

    return {
        "messageVersion": "1.0",
        "response": {
            "actionGroup": action_group,
            "function": function_name,
            "functionResponse": {
                "responseBody": {"TEXT": {"body": json.dumps(result)}}
            },
        },
    }


def execute_nl_query(question: str, database: str = DEFAULT_DATABASE) -> dict:
    """
    Full pipeline: NL question → SQL generation → safety check → Athena execution → results
    """
    logger.info(f"NL query received: {question}")

    # 1. Get schema context from Glue Data Catalog
    schema_context = get_catalog_schema(database)

    # 2. Generate SQL with Claude
    sql_query = generate_sql(question, database, schema_context)
    logger.info(f"Generated SQL: {sql_query}")

    # 3. Safety validation
    safety_check = validate_sql(sql_query)
    if not safety_check["safe"]:
        return {"error": f"Query blocked: {safety_check['reason']}", "sql": sql_query}

    # 4. Execute on Athena
    query_id = run_athena_query(sql_query, database)

    # 5. Poll for completion
    state, stats = poll_query(query_id)
    if state != "SUCCEEDED":
        return {"error": f"Athena query {state}", "sql": sql_query, "query_id": query_id}

    # 6. Fetch and return results
    return fetch_results(query_id, sql_query, stats)


def get_catalog_schema(database: str) -> str:
    """Pull table schemas from Glue Data Catalog for SQL context."""
    try:
        tables = glue.get_tables(DatabaseName=database)["TableList"]
        schema_lines = []
        for table in tables[:10]:  # Limit context size
            cols = ", ".join(
                f"{c['Name']} ({c['Type']['Name']})"
                for c in table.get("StorageDescriptor", {}).get("Columns", [])
            )
            schema_lines.append(f"  TABLE {table['Name']}: {cols}")
        return "\n".join(schema_lines) or "No tables found in catalog"
    except Exception as e:
        logger.warning(f"Could not fetch schema: {e}")
        return "Schema unavailable — generate best-effort SQL"


def generate_sql(question: str, database: str, schema_context: str) -> str:
    """Use Claude to convert NL question into Athena-compatible SQL."""
    prompt = f"""Generate a single valid Athena SQL (Presto dialect) query to answer this question.

Database: {database}
Schema:
{schema_context}

Question: {question}

Rules:
- Return ONLY the raw SQL query, no explanation, no markdown backticks
- Use Athena/Presto syntax
- Add LIMIT 1000 unless the question is an aggregation
- Handle NULL values with COALESCE where appropriate
- Use date_trunc or date_format for date grouping questions
"""

    response = bedrock.invoke_model(
        modelId=MODEL_ID,
        body=json.dumps(
            {
                "anthropic_version": "bedrock-2023-05-31",
                "max_tokens": 512,
                "messages": [{"role": "user", "content": prompt}],
            }
        ),
    )
    return json.loads(response["body"].read())["content"][0]["text"].strip()


def validate_sql(sql: str) -> dict:
    """Block any destructive SQL statements."""
    upper = sql.upper()
    for kw in BLOCKED_KEYWORDS:
        if kw in upper:
            return {"safe": False, "reason": f"Blocked keyword detected: {kw}"}
    if not upper.strip().startswith("SELECT") and not upper.strip().startswith("WITH"):
        return {"safe": False, "reason": "Only SELECT / WITH queries are permitted"}
    return {"safe": True}


def run_athena_query(sql: str, database: str) -> str:
    """Submit query to Athena and return execution ID."""
    execution = athena.start_query_execution(
        QueryString=sql,
        QueryExecutionContext={"Database": database},
        ResultConfiguration={"OutputLocation": ATHENA_OUTPUT},
        WorkGroup="primary",
    )
    return execution["QueryExecutionId"]


def poll_query(query_id: str, timeout_seconds: int = 60):
    """Poll Athena until query finishes or times out."""
    for _ in range(timeout_seconds // 2):
        response = athena.get_query_execution(QueryExecutionId=query_id)
        execution = response["QueryExecution"]
        state = execution["Status"]["State"]
        if state in ("SUCCEEDED", "FAILED", "CANCELLED"):
            stats = execution.get("Statistics", {})
            return state, stats
        time.sleep(2)
    return "TIMEOUT", {}


def fetch_results(query_id: str, sql: str, stats: dict) -> dict:
    """Retrieve and format Athena query results."""
    results = athena.get_query_results(QueryExecutionId=query_id, MaxResults=100)
    rows = results["ResultSet"]["Rows"]

    if not rows:
        return {"sql_generated": sql, "columns": [], "rows": [], "row_count": 0}

    headers = [c.get("VarCharValue", "") for c in rows[0]["Data"]]
    data = [
        [c.get("VarCharValue", "") for c in row["Data"]] for row in rows[1:]
    ]

    data_scanned_mb = stats.get("DataScannedInBytes", 0) / (1024 * 1024)

    return {
        "sql_generated": sql,
        "columns": headers,
        "rows": data,
        "row_count": len(data),
        "data_scanned_mb": round(data_scanned_mb, 3),
        "execution_time_ms": stats.get("TotalExecutionTimeInMillis", 0),
    }
