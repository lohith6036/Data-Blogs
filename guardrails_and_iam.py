"""
Bedrock Guardrails + IAM — Infrastructure Setup
Author: Lohith Kumar V

Creates safety guardrails for the DE agent and sets up
the minimum IAM permissions needed for each component.
"""

import boto3
import json

bedrock = boto3.client("bedrock", region_name="us-east-1")
iam = boto3.client("iam")


# ─── Guardrails ──────────────────────────────────────────────────────────────

def create_guardrail():
    """
    Guardrail that:
    - Blocks topics outside data engineering scope
    - Anonymises PII in agent responses
    - Prevents destructive SQL or shell commands
    """
    response = bedrock.create_guardrail(
        name="DataEngineeringAgentGuardrail",
        description="Safety controls for the autonomous DE agent",

        topicPolicyConfig={
            "topicsConfig": [
                {
                    "name": "non-de-topics",
                    "definition": (
                        "Any topic not related to data pipelines, ETL jobs, "
                        "data quality, AWS data services, or SQL queries"
                    ),
                    "examples": [
                        "Write me a poem",
                        "Tell me a joke",
                        "What is the stock price of AWS",
                    ],
                    "type": "DENY",
                }
            ]
        },

        sensitiveInformationPolicyConfig={
            "piiEntitiesConfig": [
                {"type": "EMAIL", "action": "ANONYMIZE"},
                {"type": "AWS_ACCESS_KEY", "action": "BLOCK"},
                {"type": "AWS_SECRET_KEY", "action": "BLOCK"},
                {"type": "CREDIT_DEBIT_CARD_NUMBER", "action": "ANONYMIZE"},
                {"type": "US_SOCIAL_SECURITY_NUMBER", "action": "BLOCK"},
            ]
        },

        wordPolicyConfig={
            "wordsConfig": [
                {"text": "drop table"},
                {"text": "drop database"},
                {"text": "delete from"},
                {"text": "truncate table"},
                {"text": "rm -rf"},
                {"text": "format c:"},
            ],
            "managedWordListsConfig": [{"type": "PROFANITY"}],
        },

        contentPolicyConfig={
            "filtersConfig": [
                {"type": "HATE", "inputStrength": "HIGH", "outputStrength": "HIGH"},
                {"type": "VIOLENCE", "inputStrength": "MEDIUM", "outputStrength": "HIGH"},
            ]
        },

        blockedInputMessaging="This request is outside the data engineering agent's scope.",
        blockedOutputsMessaging="Response blocked by data governance policy.",
    )

    guardrail_id = response["guardrailId"]
    print(f"✅ Guardrail created: {guardrail_id}")
    return guardrail_id


# ─── IAM Policies ────────────────────────────────────────────────────────────

BEDROCK_AGENT_POLICY = {
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "InvokeFoundationModel",
            "Effect": "Allow",
            "Action": ["bedrock:InvokeModel"],
            "Resource": "arn:aws:bedrock:us-east-1::foundation-model/anthropic.claude-3-5-sonnet-20241022-v2:0",
        },
        {
            "Sid": "InvokeLambdaActionGroups",
            "Effect": "Allow",
            "Action": ["lambda:InvokeFunction"],
            "Resource": [
                "arn:aws:lambda:us-east-1:YOUR_ACCOUNT_ID:function:pipeline-manager",
                "arn:aws:lambda:us-east-1:YOUR_ACCOUNT_ID:function:dq-remediator",
                "arn:aws:lambda:us-east-1:YOUR_ACCOUNT_ID:function:nl-to-sql",
            ],
        },
        {
            "Sid": "KnowledgeBaseAccess",
            "Effect": "Allow",
            "Action": ["bedrock:Retrieve"],
            "Resource": "arn:aws:bedrock:us-east-1:YOUR_ACCOUNT_ID:knowledge-base/*",
        },
    ],
}

LAMBDA_ACTION_GROUP_POLICY = {
    "Version": "2012-10-17",
    "Statement": [
        {
            "Sid": "GlueOperations",
            "Effect": "Allow",
            "Action": [
                "glue:StartJobRun",
                "glue:GetJobRun",
                "glue:GetJobRuns",
                "glue:UpdateJob",
                "glue:GetJob",
                "glue:StartCrawler",
                "glue:GetCrawler",
                "glue:GetTables",
                "glue:GetTable",
            ],
            "Resource": "*",
        },
        {
            "Sid": "S3DataLake",
            "Effect": "Allow",
            "Action": [
                "s3:GetObject",
                "s3:PutObject",
                "s3:DeleteObject",
                "s3:CopyObject",
                "s3:ListBucket",
            ],
            "Resource": [
                "arn:aws:s3:::my-data-lake-bucket",
                "arn:aws:s3:::my-data-lake-bucket/*",
                "arn:aws:s3:::my-athena-results",
                "arn:aws:s3:::my-athena-results/*",
            ],
        },
        {
            "Sid": "AthenaQueries",
            "Effect": "Allow",
            "Action": [
                "athena:StartQueryExecution",
                "athena:GetQueryExecution",
                "athena:GetQueryResults",
                "athena:StopQueryExecution",
            ],
            "Resource": "*",
        },
        {
            "Sid": "BedrockInvoke",
            "Effect": "Allow",
            "Action": ["bedrock:InvokeModel"],
            "Resource": "arn:aws:bedrock:us-east-1::foundation-model/anthropic.claude-3-5-sonnet-20241022-v2:0",
        },
        {
            "Sid": "SSMAuditLog",
            "Effect": "Allow",
            "Action": ["ssm:PutParameter", "ssm:GetParameter"],
            "Resource": "arn:aws:ssm:us-east-1:YOUR_ACCOUNT_ID:parameter/agentic-de/*",
        },
        {
            "Sid": "CloudWatchMetrics",
            "Effect": "Allow",
            "Action": ["cloudwatch:PutMetricData"],
            "Resource": "*",
        },
        {
            "Sid": "LogsAccess",
            "Effect": "Allow",
            "Action": ["logs:FilterLogEvents", "logs:GetLogEvents"],
            "Resource": "arn:aws:logs:us-east-1:YOUR_ACCOUNT_ID:log-group:/aws-glue/jobs/*",
        },
        {
            "Sid": "SNSEscalation",
            "Effect": "Allow",
            "Action": ["sns:Publish"],
            "Resource": "arn:aws:sns:us-east-1:YOUR_ACCOUNT_ID:data-engineering-alerts",
        },
    ],
}


def create_iam_roles():
    # Bedrock Agent Role
    iam.create_role(
        RoleName="BedrockDataEngineeringAgentRole",
        AssumeRolePolicyDocument=json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": {"Service": "bedrock.amazonaws.com"},
                        "Action": "sts:AssumeRole",
                    }
                ],
            }
        ),
        Description="IAM role for the autonomous DE Bedrock Agent",
    )

    iam.put_role_policy(
        RoleName="BedrockDataEngineeringAgentRole",
        PolicyName="BedrockAgentPolicy",
        PolicyDocument=json.dumps(BEDROCK_AGENT_POLICY),
    )
    print("✅ BedrockDataEngineeringAgentRole created")

    # Lambda Action Group Role
    iam.create_role(
        RoleName="DEAgentLambdaRole",
        AssumeRolePolicyDocument=json.dumps(
            {
                "Version": "2012-10-17",
                "Statement": [
                    {
                        "Effect": "Allow",
                        "Principal": {"Service": "lambda.amazonaws.com"},
                        "Action": "sts:AssumeRole",
                    }
                ],
            }
        ),
        Description="IAM role for Lambda action group functions",
    )

    iam.put_role_policy(
        RoleName="DEAgentLambdaRole",
        PolicyName="DEAgentLambdaPolicy",
        PolicyDocument=json.dumps(LAMBDA_ACTION_GROUP_POLICY),
    )
    print("✅ DEAgentLambdaRole created")


if __name__ == "__main__":
    create_guardrail()
    create_iam_roles()
