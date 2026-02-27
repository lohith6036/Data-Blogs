# Agentic Data Engineering on AWS

**Author: Lohith Kumar V**

An autonomous data engineering platform built on Amazon Bedrock Agents, AWS Glue, Step Functions, Lambda, and Athena. The system uses LLM-powered agents to monitor, diagnose, and self-heal data pipelines — reducing mean time to resolution from hours to minutes.

---

## Project Structure

```
agentic-de-project/
├── agents/
│   ├── create_agent.py              # Bedrock Agent setup + action groups
│   └── self_healing_pipeline.py     # EventBridge → Agent self-healing trigger
├── lambda/
│   ├── pipeline_manager/
│   │   └── handler.py               # Glue job control + S3 quarantine
│   └── nl_to_sql/
│       └── handler.py               # Natural language → Athena SQL
├── step_functions/
│   └── agentic_pipeline.asl.json    # Multi-agent orchestration workflow
├── glue/
│   └── sales_transform_job.py       # ETL job with dynamic schema mapping
├── infrastructure/
│   └── guardrails_and_iam.py        # Bedrock Guardrails + IAM roles
├── scripts/
│   ├── deploy.sh                    # One-shot deployment script
│   └── test_agent.py                # Integration test suite
└── agentic-data-engineering-blog.html
```

---

## Architecture

```
EventBridge (Glue failure) 
    → Lambda (self_healing_pipeline.py)
        → Bedrock Agent (Claude 3.5 Sonnet)
            → Action Group: PipelineManagement (pipeline_manager/handler.py)
                → Glue: StartJobRun / GetJobRun / UpdateJob
                → S3: Quarantine bad records
            → Action Group: DataQualityRemediation (nl_to_sql/handler.py)  
                → Athena: Execute NL-generated SQL queries
        → Step Functions (agentic_pipeline.asl.json)
            → Parallel: DQ Agent + Anomaly Agent
            → Human approval gate (SQS waitForTaskToken)
```

---

## Quick Start

### Prerequisites
- AWS CLI configured (`aws configure`)
- Python 3.12+
- Boto3 (`pip install boto3`)
- Bedrock model access enabled for `claude-3-5-sonnet-20241022-v2:0`

### Deploy

```bash
# 1. Set up IAM roles and guardrails
python3 infrastructure/guardrails_and_iam.py

# 2. Deploy all components
chmod +x scripts/deploy.sh
./scripts/deploy.sh dev

# 3. Update AGENT_ID in test script, then run tests
python3 scripts/test_agent.py
```

### Test the agent manually

```python
import boto3

client = boto3.client("bedrock-agent-runtime", region_name="us-east-1")
response = client.invoke_agent(
    agentId="YOUR_AGENT_ID",
    agentAliasId="PROD",
    sessionId="manual-test-01",
    inputText="Show me total sales revenue by region for last month",
)
for event in response["completion"]:
    if "chunk" in event:
        print(event["chunk"]["bytes"].decode())
```

---

## Key Features

| Feature | Implementation |
|---|---|
| Self-healing pipelines | `agents/self_healing_pipeline.py` + EventBridge |
| Multi-agent orchestration | `step_functions/agentic_pipeline.asl.json` |
| NL to SQL | `lambda/nl_to_sql/handler.py` + Athena |
| Schema drift auto-fix | `lambda/pipeline_manager/handler.py` → `patch_schema_mapping` |
| Record quarantine | `lambda/pipeline_manager/handler.py` → `quarantine_records` |
| Safety guardrails | `infrastructure/guardrails_and_iam.py` |
| Human-in-the-loop | Step Functions `waitForTaskToken` pattern |

---

## Monitored Metrics (CloudWatch namespace: `AgenticDE/SelfHealing`)

- `AutoRemediationAttempt` — count of agent healing triggers
- `AgentTraceSteps` — reasoning steps per resolution
- Target: **>80% auto-resolution rate**, **<10 min P95 MTTR**

---

## License

MIT — Lohith Kumar V
