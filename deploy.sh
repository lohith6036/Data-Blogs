#!/bin/bash
# deploy.sh — Agentic Data Engineering Project
# Author: Lohith Kumar V
# Usage: ./scripts/deploy.sh [dev|prod]

set -euo pipefail

ENV=${1:-dev}
REGION="us-east-1"
ACCOUNT_ID=$(aws sts get-caller-identity --query Account --output text)
BUCKET_PREFIX="agentic-de-${ENV}"

echo "🚀 Deploying Agentic DE Project — env: $ENV | account: $ACCOUNT_ID"

# ─── Step 1: Create S3 buckets ───────────────────────────────────────────────
echo "📦 Creating S3 buckets..."
aws s3 mb s3://${BUCKET_PREFIX}-data-lake --region $REGION 2>/dev/null || true
aws s3 mb s3://${BUCKET_PREFIX}-athena-results --region $REGION 2>/dev/null || true
aws s3 mb s3://${BUCKET_PREFIX}-glue-scripts --region $REGION 2>/dev/null || true

# ─── Step 2: Upload Glue scripts ─────────────────────────────────────────────
echo "📤 Uploading Glue scripts..."
aws s3 cp glue/sales_transform_job.py \
    s3://${BUCKET_PREFIX}-glue-scripts/scripts/sales_transform_job.py

# ─── Step 3: Package and deploy Lambda functions ─────────────────────────────
echo "⚡ Packaging Lambda functions..."

for fn in pipeline_manager nl_to_sql; do
    echo "  → Packaging $fn..."
    cd lambda/$fn
    zip -q -r /tmp/${fn}.zip .
    cd ../..

    aws lambda update-function-code \
        --function-name ${fn//_/-} \
        --zip-file fileb:///tmp/${fn}.zip \
        --region $REGION \
        2>/dev/null || \
    aws lambda create-function \
        --function-name ${fn//_/-} \
        --runtime python3.12 \
        --role arn:aws:iam::${ACCOUNT_ID}:role/DEAgentLambdaRole \
        --handler handler.lambda_handler \
        --zip-file fileb:///tmp/${fn}.zip \
        --timeout 120 \
        --memory-size 512 \
        --region $REGION
    echo "  ✅ $fn deployed"
done

# ─── Step 4: Deploy Step Functions state machine ─────────────────────────────
echo "🔄 Deploying Step Functions state machine..."

# Replace placeholders with real values
sed "s/YOUR_ACCOUNT_ID/$ACCOUNT_ID/g" \
    step_functions/agentic_pipeline.asl.json > /tmp/pipeline_resolved.json

aws stepfunctions update-state-machine \
    --state-machine-arn "arn:aws:states:$REGION:$ACCOUNT_ID:stateMachine:agentic-de-pipeline" \
    --definition file:///tmp/pipeline_resolved.json \
    2>/dev/null || \
aws stepfunctions create-state-machine \
    --name agentic-de-pipeline \
    --definition file:///tmp/pipeline_resolved.json \
    --role-arn arn:aws:iam::${ACCOUNT_ID}:role/StepFunctionsRole \
    --region $REGION

echo "  ✅ State machine deployed"

# ─── Step 5: Create Bedrock Agent ────────────────────────────────────────────
echo "🤖 Setting up Bedrock Agent..."
python3 agents/create_agent.py

# ─── Step 6: Create EventBridge rule for self-healing ────────────────────────
echo "📡 Creating EventBridge rule for pipeline failure detection..."

aws events put-rule \
    --name "GlueJobFailureDetector" \
    --event-pattern '{
        "source": ["aws.glue"],
        "detail-type": ["Glue Job State Change"],
        "detail": {"state": ["FAILED"]}
    }' \
    --state ENABLED \
    --region $REGION

aws events put-targets \
    --rule GlueJobFailureDetector \
    --targets "Id=SelfHealingAgent,Arn=arn:aws:lambda:$REGION:$ACCOUNT_ID:function:self-healing-agent" \
    --region $REGION

echo ""
echo "✅ Deployment complete!"
echo "   Data Lake:      s3://${BUCKET_PREFIX}-data-lake"
echo "   Athena Results: s3://${BUCKET_PREFIX}-athena-results"
echo "   Glue Scripts:   s3://${BUCKET_PREFIX}-glue-scripts"
echo ""
echo "📊 Test the agent:"
echo "   python3 scripts/test_agent.py"
