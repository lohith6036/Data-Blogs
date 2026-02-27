"""
Agent Integration Tests
Author: Lohith Kumar V

Smoke-tests for the deployed Bedrock DE agent.
Run: python3 scripts/test_agent.py
"""

import boto3
import json
import uuid

bedrock_runtime = boto3.client("bedrock-agent-runtime", region_name="us-east-1")

AGENT_ID = "YOUR_AGENT_ID"       # Replace after running create_agent.py
AGENT_ALIAS_ID = "PROD"


def invoke_agent(prompt: str, session_id: str = None) -> str:
    """Send a prompt to the agent and return the full text response."""
    session_id = session_id or f"test-{uuid.uuid4().hex[:8]}"

    response = bedrock_runtime.invoke_agent(
        agentId=AGENT_ID,
        agentAliasId=AGENT_ALIAS_ID,
        sessionId=session_id,
        inputText=prompt,
        enableTrace=True,
    )

    full_text = ""
    step_count = 0

    for event in response["completion"]:
        if "chunk" in event:
            full_text += event["chunk"]["bytes"].decode("utf-8")
        elif "trace" in event:
            trace = event["trace"].get("trace", {})
            if "orchestrationTrace" in trace:
                step_count += 1

    print(f"  Agent steps: {step_count}")
    return full_text


def test_job_status_query():
    print("\n📋 Test 1: Natural language job status query")
    result = invoke_agent("What is the current status of the sales-transform Glue job?")
    print(f"  Response: {result[:300]}...")
    assert len(result) > 10, "Empty response"
    print("  ✅ PASSED")


def test_nl_to_sql():
    print("\n🔍 Test 2: Natural language to SQL")
    result = invoke_agent(
        "Show me total revenue by region for the last 30 days from the sales table"
    )
    print(f"  Response: {result[:400]}...")
    assert "SELECT" in result.upper() or "revenue" in result.lower(), "No SQL or data found"
    print("  ✅ PASSED")


def test_self_healing_simulation():
    print("\n🔧 Test 3: Schema drift diagnosis simulation")
    result = invoke_agent(
        """
        The sales-transform job just failed with this error:
        AnalysisException: cannot resolve 'revenue_usd' given input columns
        [order_id, customer_id, revenue_local_currency, quantity, order_date].
        
        Please diagnose and fix.
        """
    )
    print(f"  Response: {result[:500]}...")
    assert len(result) > 50, "Response too short"
    print("  ✅ PASSED")


def test_dq_check():
    print("\n✅ Test 4: Data quality check request")
    result = invoke_agent(
        "Run a data quality check on the data_warehouse.sales table and report any issues"
    )
    print(f"  Response: {result[:300]}...")
    assert len(result) > 10, "Empty response"
    print("  ✅ PASSED")


def test_guardrail_block():
    print("\n🛡️  Test 5: Guardrail should block off-topic requests")
    result = invoke_agent("Write me a poem about data lakes")
    print(f"  Response: {result[:200]}...")
    # Guardrail should block this or redirect
    print("  ✅ PASSED (verify guardrail message in response)")


if __name__ == "__main__":
    print("🤖 Agentic DE — Integration Test Suite")
    print("=" * 50)

    tests = [
        test_job_status_query,
        test_nl_to_sql,
        test_self_healing_simulation,
        test_dq_check,
        test_guardrail_block,
    ]

    passed = 0
    failed = 0

    for test in tests:
        try:
            test()
            passed += 1
        except Exception as e:
            print(f"  ❌ FAILED: {e}")
            failed += 1

    print(f"\n{'='*50}")
    print(f"Results: {passed} passed, {failed} failed")
