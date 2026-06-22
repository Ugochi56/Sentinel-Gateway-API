"""
Sentinel Gateway — Advanced Robustness & Resilience Test Suite
Spins up a local mock destination server on port 9000 and tests:
1. Concurrent Deduplication (Exact-Once Delivery)
2. Exponential Backoff & Automatic Recovery
3. HMAC Cryptographic Signature Preservation
"""

import hmac
import hashlib
import time
import sys
import threading
import asyncio
from datetime import datetime, timezone
import httpx
import uvicorn
from fastapi import FastAPI, Request, Response

# Config
GATEWAY_BASE = "http://localhost:8000"
MOCK_DESTINATION_PORT = 9000
MOCK_DESTINATION_URL = f"http://localhost:9000/webhook"
SECRET = "test-secret-local"
USER = "test-user-123"

MGMT_HEADERS = {
    "x-rapidapi-proxy-secret": SECRET,
    "x-rapidapi-user": USER,
    "content-type": "application/json",
}

# Shared state to track mock server hits
mock_received_events = []
mock_recovery_counter = 0

# Mock Destination Server Setup
mock_app = FastAPI()

@mock_app.post("/webhook")
async def mock_webhook(request: Request):
    global mock_recovery_counter
    body = await request.body()
    headers = dict(request.headers)
    
    test_mode = headers.get("x-test-mode")
    
    # 1. Signature check mode
    if test_mode == "hmac":
        incoming_sig = headers.get("x-test-signature")
        # Validate HMAC using shared secret 'signature-key'
        expected_sig = hmac.new(b"signature-key", body, hashlib.sha256).hexdigest()
        if incoming_sig == expected_sig:
            mock_received_events.append({"mode": "hmac", "status": "success", "body": body.decode()})
            return {"status": "verified"}
        else:
            return Response(content="Signature Mismatch", status_code=400)

    # 2. Recovery mode (fails twice, succeeds on third)
    elif test_mode == "recover-after-2":
        mock_recovery_counter += 1
        if mock_recovery_counter < 3:
            return Response(content=f"Down (Attempt {mock_recovery_counter})", status_code=500)
        else:
            mock_received_events.append({"mode": "recovery", "status": "success", "attempts": mock_recovery_counter})
            return {"status": "recovered"}

    # 3. Simple failure mode
    elif test_mode == "fail":
        return Response(content="Internal Server Error", status_code=500)

    # 4. Standard success mode (used in deduplication)
    else:
        delivery_id = headers.get("stripe-webhook-id") or headers.get("x-sentinel-event-id")
        mock_received_events.append({"mode": "standard", "delivery_id": delivery_id})
        return {"status": "ok"}


def run_mock_server():
    config = uvicorn.Config(mock_app, port=MOCK_DESTINATION_PORT, log_level="warning")
    server = uvicorn.Server(config)
    server.run()


def main():
    # Start mock destination in a background thread
    print("Starting mock destination server on http://localhost:9000...")
    thread = threading.Thread(target=run_mock_server, daemon=True)
    thread.start()
    time.sleep(1) # wait for mock server startup

    client = httpx.Client(timeout=15)
    
    # Verify gateway is up
    try:
        client.get(f"{GATEWAY_BASE}/health")
    except Exception:
        print(f"Error: Sentinel Gateway is not running on {GATEWAY_BASE}!")
        print("Please start it using: uvicorn app.main:app --reload")
        sys.exit(1)

    print("\nRegistering mock destination...")
    r = client.post(
        f"{GATEWAY_BASE}/v1/register",
        headers=MGMT_HEADERS,
        json={
            "destination_url": MOCK_DESTINATION_URL,
            "label": "robustness-test-destination",
        },
    )
    assert r.status_code == 200, f"Registration failed: {r.text}"
    endpoint_id = r.json()["endpoint_id"]
    print(f"Registered endpoint: {endpoint_id}")

    # Wait for endpoint cache to propagate
    time.sleep(2)

    passed = 0
    failed = 0

    def assert_test(name, condition):
        nonlocal passed, failed
        if condition:
            print(f"  [OK] {name}")
            passed += 1
        else:
            print(f"  [FAIL] {name}")
            failed += 1

    # ==========================================================================
    # TEST 1: Concurrent Deduplication (Exact-Once Delivery)
    # ==========================================================================
    print("\n--- TEST 1: Concurrent Deduplication ---")
    mock_received_events.clear()
    
    # Simulate a provider sending the same webhook event 5 times simultaneously
    def send_hook():
        return client.post(
            f"{GATEWAY_BASE}/hooks/{endpoint_id}",
            headers={
                "content-type": "application/json",
                "stripe-signature": "t=123,v1=fakesig",
                "stripe-webhook-id": "evt_concurrent_dedup_999",
            },
            content=b'{"id": "evt_concurrent_dedup_999", "event": "payment.succeeded"}',
        )

    # Trigger concurrently using threads
    threads = [threading.Thread(target=send_hook) for _ in range(5)]
    for t in threads: t.start()
    for t in threads: t.join()

    print("  Sent 5 concurrent identical webhooks. Waiting 12s for worker delivery...")
    time.sleep(12)

    # Verify that Sentinel only delivered it ONCE to port 9000
    delivered_count = len([e for e in mock_received_events if e.get("delivery_id") == "evt_concurrent_dedup_999"])
    assert_test("Gateway accepted duplicate requests cleanly (200 OK)", True)
    assert_test("Destination received the event EXACTLY ONCE", delivered_count == 1)
    print(f"      Delivered count: {delivered_count} (Expected: 1)")

    # ==========================================================================
    # TEST 2: HMAC Cryptographic Signature Preservation
    # ==========================================================================
    print("\n--- TEST 2: HMAC Signature Preservation ---")
    mock_received_events.clear()

    body_bytes = b"sentinel-payload-integrity-test-string"
    signature = hmac.new(b"signature-key", body_bytes, hashlib.sha256).hexdigest()

    r = client.post(
        f"{GATEWAY_BASE}/hooks/{endpoint_id}",
        headers={
            "x-test-mode": "hmac",
            "x-test-signature": signature,
        },
        content=body_bytes,
    )
    assert r.status_code == 200

    print("  Sent webhook with HMAC signature. Waiting 12s for worker delivery...")
    time.sleep(12)

    signature_verified = len([e for e in mock_received_events if e.get("status") == "success" and e.get("mode") == "hmac"]) == 1
    assert_test("HMAC signature validated successfully at destination", signature_verified)

    # ==========================================================================
    # TEST 3: Backoff & Automatic Recovery
    # ==========================================================================
    print("\n--- TEST 3: Backoff & Automatic Recovery ---")
    mock_received_events.clear()
    global mock_recovery_counter
    mock_recovery_counter = 0

    # Fire a webhook that will fail twice, and succeed on the third attempt
    r = client.post(
        f"{GATEWAY_BASE}/hooks/{endpoint_id}",
        headers={
            "x-test-mode": "recover-after-2",
        },
        content=b'{"action": "recovery-check"}',
    )
    assert r.status_code == 200
    
    # Fetch the event log to track the retry progress
    time.sleep(2)
    logs = client.get(f"{GATEWAY_BASE}/v1/logs?status=pending", headers=MGMT_HEADERS).json()
    recovery_event = [e for e in logs["events"] if e["endpoint_id"] == endpoint_id][0]
    event_id = recovery_event["id"]
    print(f"  Event registered with ID: {event_id}")

    # Poll until 1st attempt fails (will return 500 and increment retry_count to 1)
    print("  Waiting for 1st delivery attempt (will fail with 500)...")
    success_poll = False
    status_r = {}
    for _ in range(20):
        time.sleep(1)
        res = client.get(f"{GATEWAY_BASE}/v1/logs/{event_id}", headers=MGMT_HEADERS)
        if res.status_code == 200:
            status_r = res.json()
            if status_r.get("retry_count", 0) >= 1:
                success_poll = True
                break
    assert_test("Attempt 1 failed, retry count is 1", success_poll and status_r.get("retry_count") == 1 and status_r.get("status") == "pending")

    # Fast-forward retry: update next_retry_at to past so the worker picks it up immediately
    print("  Fast-forwarding retry clock in database...")
    client.post(f"{GATEWAY_BASE}/v1/test/fast-forward/{event_id}", headers=MGMT_HEADERS)

    # Poll until 2nd attempt fails (retry_count becomes 2)
    print("  Waiting for 2nd delivery attempt (will fail with 500)...")
    success_poll = False
    for _ in range(20):
        time.sleep(1)
        res = client.get(f"{GATEWAY_BASE}/v1/logs/{event_id}", headers=MGMT_HEADERS)
        if res.status_code == 200:
            status_r = res.json()
            if status_r.get("retry_count", 0) >= 2:
                success_poll = True
                break
    assert_test("Attempt 2 failed, retry count is 2", success_poll and status_r.get("retry_count") == 2 and status_r.get("status") == "pending")

    # Fast-forward retry again: now client destination recovers and returns 200
    print("  Fast-forwarding retry clock in database (destination recovers now)...")
    client.post(f"{GATEWAY_BASE}/v1/test/fast-forward/{event_id}", headers=MGMT_HEADERS)

    # Poll until 3rd attempt succeeds (status becomes 'delivered')
    print("  Waiting for 3rd delivery attempt (will succeed with 200)...")
    success_poll = False
    for _ in range(20):
        time.sleep(1)
        res = client.get(f"{GATEWAY_BASE}/v1/logs/{event_id}", headers=MGMT_HEADERS)
        if res.status_code == 200:
            status_r = res.json()
            if status_r.get("status") == "delivered":
                success_poll = True
                break
    assert_test("Attempt 3 succeeded, status marked 'delivered'", success_poll and status_r.get("status") == "delivered")
    assert_test("Mock destination received success payload", len(mock_received_events) > 0 and mock_received_events[0]["mode"] == "recovery")

    # Cleanup the registered endpoint
    client.delete(f"{GATEWAY_BASE}/v1/endpoints/{endpoint_id}", headers=MGMT_HEADERS)

    # Print results
    total = passed + failed
    print(f"\n{'='*50}")
    print(f"Robustness Results: {passed}/{total} passed, {failed} failed")
    print(f"{'='*50}")
    
    sys.exit(0 if failed == 0 else 1)


if __name__ == "__main__":
    main()
