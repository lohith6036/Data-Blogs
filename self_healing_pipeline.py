"""
Self-Healing Pipeline — EventBridge → Lambda → Bedrock Agent
Author: Lohith Kumar V

Triggered automatically when a Glue job enters FAILED state.
The Bedrock agent diagnoses the error, determines if it can
self-remediate, and either fixes + re-runs or escalates to humans.
"""

import boto3
import json
import logging

logger = logging.getLogger()
logger.setLevel(logging.INFO)

bedrock_runtime = boto3.client("bedrock-agent-runtime", region_name="us-east-1")
cloudwatch = boto3.client("cloudwatch")
logs_client = boto3.client("logs")
sns = boto3.client("sns")

AGENT_ID = "YOUR_AGENT_ID"
AGENT_ALIAS_ID = "PROD"
ESCALATION_TOPIC_ARN = "arn:aws:sns:us-east-1:YOUR_ACCOUNT_ID:data-engineering-alerts"


def lambda_handler(event, context):
    """
    EventBridge rule: source=aws.glue, detail-type=Glue Job State Change,
    detail.state=FAILED
    """
    detail = event.get("detail", {})
    job_name = detail.get("jobName", "unknown")
    run_id = detail.get("jobRunId", "unknown")
    error_msg = detail.get("message", "No error message provided")

    logger.info(f"Pipeline failure detected: {job_name} / {run_id}")

    # Gather rich context for the agent
    log_context = get_cloudwatch_logs(job_name, run_id)
    recent_runs = get_recent_run_history(job_name)

    prompt = build_prompt(job_name, run_id, error_msg, log_context, recent_runs)

    # Invoke agent with full trace enabled
    agent_response, traces = invoke_agent(prompt, session_id=f"heal-{run_id}")

    logger.info(f"Agent response: {agent_response}")

    # Emit custom metrics
    emit_metrics(job_name, traces)

    # If agent couldn't resolve, send SNS alert
    if "ESCALATE" in agent_response.upper() or "HUMAN" in agent_response.upper():
        escalate_to_human(job_name, run_id, error_msg, agent_response)

    return {
        "job_name": job_name,
        "run_id": run_id,
        "agent_steps": len(traces),
        "resolved": "ESCALATE" not in agent_response.upper(),
        "summary": agent_response[:500],
    }


def build_prompt(job_name, run_id, error_msg, log_context, recent_runs) -> str:
    return f"""
PIPELINE FAILURE ALERT

Job: {job_name}
Run ID: {run_id}
Error: {error_msg}

Recent error logs:
{log_context}

Last 3 run states:
{json.dumps(recent_runs, indent=2)}

Your task:
1. Diagnose the root cause (schema drift, data issue, transient AWS error, config problem)
2. Determine if this is auto-remediable with confidence > 80%
3. If yes: take corrective action and re-trigger the job
4. If no: respond with ESCALATE and summarize what a human needs to investigate
5. Always explain your reasoning step by step
"""


def invoke_agent(prompt: str, session_id: str):
    """Stream agent response and collect traces."""
    response = bedrock_runtime.invoke_agent(
        agentId=AGENT_ID,
        agentAliasId=AGENT_ALIAS_ID,
        sessionId=session_id,
        inputText=prompt,
        enableTrace=True,
    )

    full_text = ""
    traces = []

    for chunk in response["completion"]:
        if "chunk" in chunk:
            full_text += chunk["chunk"]["bytes"].decode("utf-8")
        elif "trace" in chunk:
            trace = chunk["trace"].get("trace", {})
            if "orchestrationTrace" in trace:
                traces.append(trace["orchestrationTrace"])

    return full_text, traces


def get_cloudwatch_logs(job_name: str, run_id: str) -> str:
    """Fetch the last 20 error log lines for this job run."""
    try:
        response = logs_client.filter_log_events(
            logGroupName="/aws-glue/jobs/error",
            filterPattern=run_id,
            limit=20,
        )
        lines = [e["message"] for e in response.get("events", [])]
        return "\n".join(lines) if lines else "No log events found"
    except Exception as e:
        return f"Could not retrieve logs: {e}"


def get_recent_run_history(job_name: str) -> list:
    """Get the last 3 job run states for pattern detection."""
    try:
        glue = boto3.client("glue")
        runs = glue.get_job_runs(JobName=job_name, MaxResults=3)["JobRuns"]
        return [
            {
                "run_id": r["Id"],
                "state": r["JobRunState"],
                "started": r["StartedOn"].isoformat(),
                "error": r.get("ErrorMessage", ""),
            }
            for r in runs
        ]
    except Exception:
        return []


def emit_metrics(job_name: str, traces: list):
    """Push custom CloudWatch metrics for the agentic healing operation."""
    cloudwatch.put_metric_data(
        Namespace="AgenticDE/SelfHealing",
        MetricData=[
            {
                "MetricName": "AutoRemediationAttempt",
                "Dimensions": [{"Name": "JobName", "Value": job_name}],
                "Value": 1,
                "Unit": "Count",
            },
            {
                "MetricName": "AgentTraceSteps",
                "Dimensions": [{"Name": "JobName", "Value": job_name}],
                "Value": len(traces),
                "Unit": "Count",
            },
        ],
    )


def escalate_to_human(job_name, run_id, error_msg, agent_summary):
    """Send SNS notification when the agent cannot auto-resolve."""
    sns.publish(
        TopicArn=ESCALATION_TOPIC_ARN,
        Subject=f"[AgenticDE] Manual intervention needed: {job_name}",
        Message=json.dumps(
            {
                "alert": "Agent could not auto-remediate",
                "job_name": job_name,
                "run_id": run_id,
                "error": error_msg,
                "agent_analysis": agent_summary,
                "action_required": "Please review and re-trigger manually",
            },
            indent=2,
        ),
    )
    logger.warning(f"Escalated {job_name}/{run_id} to human via SNS")
