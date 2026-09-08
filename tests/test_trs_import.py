import pytest
import asyncio
import httpx
from src.app.main import app

# Let's write a fully compliant integration test for the TRS to 12306 schedule publishing flow.
# We utilize the same synchronous-bridge pattern designed for this workspace to avoid event loop conflicts.

def test_trs_schedule_publishing_and_cache_preheating(db_session, event_loop):
    """
    Test EPIC-08: Verify that TRS authority publishing syncs database segments and
    automatically pre-heats the 12306 Redis high-concurrency query cache.
    """
    loop = event_loop
    
    # 1. Define the authoritative TRS publishing payload for G999
    trs_payload = {
        "train_code": "G999",
        "train_name": "复兴号 G999 次跨局专列",
        "service_date": "2026-12-25",
        "schedule_id": 99,
        "stations": [
            {"name": "北京", "sequence": 1},
            {"name": "天津", "sequence": 2},
            {"name": "济南", "sequence": 3},
            {"name": "上海", "sequence": 4}
        ],
        "carriage_seats": [
            {
                "carriage": "01",
                "seat_class": "BUSINESS",
                "seats": ["03A", "03C", "03D"] # 3 newly published seats
            }
        ]
    }

    async def run_test():
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as ac:
            # Step A: Perform TRS Import POST API Call
            import_response = await ac.post("/api/v1/ops/trs/import-schedule", json=trs_payload)
            assert import_response.status_code == 200, f"Import failed: {import_response.text}"
            
            import_data = import_response.json()
            assert import_data["status"] == "SUCCESS"
            assert import_data["schedule_id"] == 99
            assert import_data["seats_created"] == 3
            assert import_data["segments_created"] == 9 # 3 seats * 3 segments

            # Step B: Perform instant Query to verify Redis pre-heating occurred
            query_url = "/api/v1/query?schedule_id=99&from_station_seq=1&to_station_seq=2&seat_class=BUSINESS"
            query_response = await ac.get(query_url)
            assert query_response.status_code == 200
            
            query_data = query_response.json()
            # The count must be exactly 3, read directly from Redis because of the auto-preheat trigger!
            assert query_data["available_seats"] == 3, "Redis cache was not pre-heated by TRS import!"

    loop.run_until_complete(run_test())
