"""
Agentic Data Engineering — Bedrock Agent Setup
Author: Lohith Kumar V

Creates the Bedrock supervisor agent with two action groups:
  1. PipelineManagement  — trigger/monitor/repair Glue jobs
  2. DataQualityRemediation — quarantine bad records, patch schemas
"""

import boto3
import json

bedrock_agent = boto3.client("bedrock-agent", region_name="us-east-1")


def create_de_agent():
    response = bedrock_agent.create_agent(
        agentName="DataEngineeringAgent",
        foundationModel="anthropic.claude-3-5-sonnet-20241022-v2:0",
        instruction="""
        You are an autonomous data engineering agent. Your responsibilities:

        1. Monitor AWS Glue pipeline health and data quality scores
        2. Diagnose root causes of pipeline failures using CloudWatch logs
        3. Execute remediation: re-run failed jobs, quarantine bad records,
           patch schema mappings, or escalate to humans when confidence < 80%
        4. Answer natural language queries about pipeline data via Athena
        5. Log all decisions with reasoning and confidence scores

        Always explain your reasoning before taking any action.
        Never make irreversible changes without logging intent first.
        """,
        idleSessionTTLInSeconds=3600,
        agentResourceRoleArn="arn:aws:iam::YOUR_ACCOUNT_ID:role/BedrockAgentRole",
    )

    agent_id = response["agent"]["agentId"]
    print(f"✅ Agent created: {agent_id}")
    return agent_id


def add_pipeline_action_group(agent_id: str):
    bedrock_agent.create_agent_action_group(
        agentId=agent_id,
        agentVersion="DRAFT",
        actionGroupName="PipelineManagement",
        actionGroupExecutor={
            "lambda": "arn:aws:lambda:us-east-1:YOUR_ACCOUNT_ID:function:pipeline-manager"
        },
        functionSchema={
            "functions": [
                {
                    "name": "trigger_glue_job",
                    "description": "Trigger or re-run a Glue ETL job by name",
                    "parameters": {
                        "job_name": {"type": "string", "required": True},
                        "arguments": {"type": "object", "required": False},
                    },
                },
                {
                    "name": "get_job_status",
                    "description": "Get status, duration, and error logs for a Glue job run",
                    "parameters": {
                        "job_name": {"type": "string", "required": True},
                        "run_id": {"type": "string", "required": False},
                    },
                },
                {
                    "name": "quarantine_records",
                    "description": "Move bad records to quarantine S3 prefix for human review",
                    "parameters": {
                        "source_path": {"type": "string", "required": True},
                        "reason": {"type": "string", "required": True},
                    },
                },
                {
                    "name": "patch_schema_mapping",
                    "description": "Update a Glue job's schema mapping when column names change",
                    "parameters": {
                        "job_name": {"type": "string", "required": True},
                        "old_column": {"type": "string", "required": True},
                        "new_column": {"type": "string", "required": True},
                    },
                },
            ]
        },
    )
    print("✅ PipelineManagement action group added")


def add_dq_action_group(agent_id: str):
    bedrock_agent.create_agent_action_group(
        agentId=agent_id,
        agentVersion="DRAFT",
        actionGroupName="DataQualityRemediation",
        actionGroupExecutor={
            "lambda": "arn:aws:lambda:us-east-1:YOUR_ACCOUNT_ID:function:dq-remediator"
        },
        functionSchema={
            "functions": [
                {
                    "name": "run_dq_check",
                    "description": "Run Glue Data Quality rules on a dataset and return scores",
                    "parameters": {
                        "database": {"type": "string", "required": True},
                        "table": {"type": "string", "required": True},
                    },
                },
                {
                    "name": "execute_nl_query",
                    "description": "Convert a natural language question to Athena SQL and execute it",
                    "parameters": {
                        "question": {"type": "string", "required": True},
                        "database": {"type": "string", "required": False},
                    },
                },
            ]
        },
    )
    print("✅ DataQualityRemediation action group added")


def prepare_and_deploy(agent_id: str):
    bedrock_agent.prepare_agent(agentId=agent_id)

    alias = bedrock_agent.create_agent_alias(
        agentId=agent_id,
        agentAliasName="PROD",
        description="Production alias for DataEngineeringAgent",
    )
    print(f"✅ Agent deployed. Alias: {alias['agentAlias']['agentAliasId']}")


if __name__ == "__main__":
    agent_id = create_de_agent()
    add_pipeline_action_group(agent_id)
    add_dq_action_group(agent_id)
    prepare_and_deploy(agent_id)
