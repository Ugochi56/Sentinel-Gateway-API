"""
Sentinel Gateway — Subscription Tiers & Usage Limits Test Suite
Validates endpoint registration quotas, dynamic payload size limits, and max retries per tier.
Uses pure HTTP endpoints to avoid direct database connections from the test client.
"""

import time
import sys
import threading
import httpx
import uvicorn
from fastapi import FastAPI, Request, Response

GATEWAY_BASE = "http://localhost:8000"
MOCK_DESTINATION_PORT = 9500
MOCK_DESTINATION_URL = f"http://localhost:9500/webhook"
SECRET = "test-secret-local"
USER_FREE = "test-user-tiers-free"
USER_BASIC = "test-user-tiers-basic"

# Mock server setup
mock_app = FastAPI()
mock_received_events = []

@mock_app.post("/webhook")
async def mock_webhook(request: Request):
    headers = dict(request.headers)
    body = await request.body()
    mock_received_events.append({"headers": headers, "body_size": len(body)})
    
    test_mode = headers.get("x-test-mode")
    if test_mode == "fail":
        return Response(content="Down", status_code=500)
    return {"status": "ok"}

def run_mock_server():
    config = uvicorn.Config(mock_app, port=MOCK_DESTINATION_PORT, log_level="warning")
    server = uvicorn.Server(config)
    server.run()


def main():
    # Start mock destination
    print("Starting mock destination server on http://localhost:9500...")
    thread = threading.Thread(target=run_mock_server, daemon=True)
    thread.start()
    time.sleep(1) # wait for startup

    client = httpx.Client(timeout=15)
    
    # 1. Clean up any existing endpoints from previous runs for these test users via API
    print("Cleaning up old test endpoints via management API...")
    for user_key in (USER_FREE, USER_BASIC):
        r_list = client.get(
            f"{GATEWAY_BASE}/v1/endpoints",
            headers={
                "x-rapidapi-proxy-secret": SECRET,
                "x-rapidapi-user": user_key,
            }
        )
        if r_list.status_code == 200:
            for ep in r_list.json().get("endpoints", []):
                client.delete(
                    f"{GATEWAY_BASE}/v1/endpoints/{ep['id']}",
                    headers={
                        "x-rapidapi-proxy-secret": SECRET,
                        "x-rapidapi-user": user_key,
                    }
                )

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
    # TEST 1: Endpoint Count Limit Enforcement (Free Tier max = 5)
    # ==========================================================================
    print("\n--- TEST 1: Endpoint Count Limit (Free Tier max = 5) ---")
    
    # Register 5 endpoints successfully
    free_endpoint_ids = []
    for i in range(5):
        r = client.post(
            f"{GATEWAY_BASE}/v1/register",
            headers={
                "x-rapidapi-proxy-secret": SECRET,
                "x-rapidapi-user": USER_FREE,
                "x-rapidapi-plan": "Basic",
                "content-type": "application/json",
            },
            json={"destination_url": MOCK_DESTINATION_URL, "label": f"free-endpoint-{i}"}
        )
        assert r.status_code == 200, f"Registration failed at index {i}: {r.text}"
        free_endpoint_ids.append(r.json()["endpoint_id"])

    assert_test("Successfully registered 5 endpoints on the Basic plan (Free limits)", len(free_endpoint_ids) == 5)

    # Try to register the 6th endpoint (must be blocked)
    r = client.post(
        f"{GATEWAY_BASE}/v1/register",
        headers={
            "x-rapidapi-proxy-secret": SECRET,
            "x-rapidapi-user": USER_FREE,
            "x-rapidapi-plan": "Basic",
            "content-type": "application/json",
        },
        json={"destination_url": MOCK_DESTINATION_URL, "label": "free-endpoint-6"}
    )
    assert_test("6th endpoint registration rejected with 403 Forbidden", r.status_code == 403)
    if r.status_code == 403:
        print(f"      Response: {r.json()['detail']}")

    # ==========================================================================
    # TEST 2: Payload Size Limit Enforcements (Free: 1MB, Basic: 5MB)
    # ==========================================================================
    print("\n--- TEST 2: Payload Size Limits ---")
    
    free_ep = free_endpoint_ids[0]
    time.sleep(2) # let cache invalidate and refresh

    # Free Plan (1MB)
    # Send 500KB payload (should succeed)
    r = client.post(f"{GATEWAY_BASE}/hooks/{free_ep}", content=b"x" * 500_000)
    assert_test("Basic Plan (Free limits): 500 KB payload accepted (200 OK)", r.status_code == 200)

    # Send 1.5MB payload (should be blocked)
    r = client.post(f"{GATEWAY_BASE}/hooks/{free_ep}", content=b"x" * 1_500_000)
    assert_test("Basic Plan (Free limits): 1.5 MB payload rejected (413 Content Too Large)", r.status_code == 413)

    # Register an endpoint on Basic Plan (5MB limit)
    r = client.post(
        f"{GATEWAY_BASE}/v1/register",
        headers={
            "x-rapidapi-proxy-secret": SECRET,
            "x-rapidapi-user": USER_BASIC,
            "x-rapidapi-plan": "Pro",
            "content-type": "application/json",
        },
        json={"destination_url": MOCK_DESTINATION_URL, "label": "basic-endpoint"}
    )
    assert r.status_code == 200
    basic_ep = r.json()["endpoint_id"]
    time.sleep(2) # cache update

    # Basic Plan: Send 1.5MB payload (should succeed!)
    r = client.post(f"{GATEWAY_BASE}/hooks/{basic_ep}", content=b"x" * 1_500_000)
    assert_test("Pro Plan (Basic limits): 1.5 MB payload accepted (200 OK)", r.status_code == 200)

    # Basic Plan: Send 6MB payload (should fail!)
    r = client.post(f"{GATEWAY_BASE}/hooks/{basic_ep}", content=b"x" * 6_000_000)
    assert_test("Pro Plan (Basic limits): 6 MB payload rejected (413 Content Too Large)", r.status_code == 413)

    # ==========================================================================
    # TEST 3: Max Retries (Free Plan = 5 attempts max)
    # ==========================================================================
    print("\n--- TEST 3: Plan Max Retries (Free Plan max = 5) ---")
    mock_received_events.clear()

    # Trigger a failing event on Free plan endpoint
    r = client.post(
        f"{GATEWAY_BASE}/hooks/{free_ep}",
        headers={"x-test-mode": "fail"},
        content=b"fail-me"
    )
    assert r.status_code == 200

    # Wait for the first attempt to fail
    time.sleep(13)
    
    # Get the event ID
    logs = client.get(f"{GATEWAY_BASE}/v1/logs?status=pending", headers={
        "x-rapidapi-proxy-secret": SECRET,
        "x-rapidapi-user": USER_FREE,
    }).json()
    failing_event = logs["events"][0]
    event_id = failing_event["id"]
    print(f"  Failing event registered: {event_id}")

    # Fast-forward retry loops 1 to 5
    for attempt in range(1, 6):
        print(f"  Fast-forwarding retry clock in database (Attempt {attempt} completed)...")
        # Call the server's test-backdoor to fast-forward the retry clock safely
        client.post(
            f"{GATEWAY_BASE}/v1/test/fast-forward/{event_id}",
            headers={
                "x-rapidapi-proxy-secret": SECRET,
                "x-rapidapi-user": USER_FREE,
            }
        )
        
        # Robust polling: wait up to 20 seconds for the worker to process this attempt
        target_retry = attempt + 1
        success_poll = False
        status_res = {}
        for poll_sec in range(20):
            time.sleep(1)
            r_status = client.get(f"{GATEWAY_BASE}/v1/logs/{event_id}", headers={
                "x-rapidapi-proxy-secret": SECRET,
                "x-rapidapi-user": USER_FREE,
            })
            if r_status.status_code == 200:
                status_res = r_status.json()
                # If retry count reached target, or event is permanently failed, we are done waiting
                if status_res.get("retry_count", 0) >= target_retry or status_res.get("status") == "failed_permanently":
                    success_poll = True
                    break
        
        if not success_poll:
            print(f"      [Warning] Polling timed out waiting for attempt {attempt} to be processed.")
        print(f"      Event retry count: {status_res.get('retry_count')}, Status: {status_res.get('status')}")

    # Fetch final status for verification
    r_status = client.get(f"{GATEWAY_BASE}/v1/logs/{event_id}", headers={
        "x-rapidapi-proxy-secret": SECRET,
        "x-rapidapi-user": USER_FREE,
    })
    status_res = r_status.json()
    assert_test("Basic Plan (Free limits): Event marked 'failed_permanently' after 5 retry attempts", status_res["status"] == "failed_permanently")

    # ==========================================================================
    # CLEANUP
    # ==========================================================================
    print("\nCleaning up database records via API...")
    for user_key in (USER_FREE, USER_BASIC):
        r_list = client.get(
            f"{GATEWAY_BASE}/v1/endpoints",
            headers={
                "x-rapidapi-proxy-secret": SECRET,
                "x-rapidapi-user": user_key,
            }
        )
        if r_list.status_code == 200:
            for ep in r_list.json().get("endpoints", []):
                client.delete(
                    f"{GATEWAY_BASE}/v1/endpoints/{ep['id']}",
                    headers={
                        "x-rapidapi-proxy-secret": SECRET,
                        "x-rapidapi-user": user_key,
                    }
                )

    # Summary
    total = passed + failed
    print(f"\n{'='*50}")
    print(f"Tiers Results: {passed}/{total} passed, {failed} failed")
    print(f"{'='*50}")
    
    sys.exit(0 if failed == 0 else 1)


if __name__ == "__main__":
    main()
