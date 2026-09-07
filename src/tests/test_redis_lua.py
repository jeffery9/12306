import pytest
from src.app.redis_client import get_redis, reserve_seat_lua, release_seat_lua

def test_lua_reservation_flow(event_loop):
    """Verify bitmask-based Lua scripts for atomic seat locking, overlap detection, and release."""
    async def _impl():
        redis_conn = get_redis()
        seat_key = "r:1:seat:101"
        res_key = "r:1:reservation:R1"
        
        # 1. Clean any leftover keys
        await redis_conn.delete(seat_key, res_key)
        
        # 2. First reservation should succeed for mask 3 (binary 0011, segments 0 and 1)
        res = await reserve_seat_lua(redis_conn, seat_key, res_key, mask=3, res_id="R1", ttl=60)
        assert res == 1
        
        # 3. Overlapping reservation for mask 2 (binary 0010, segment 1) on the same seat key should fail
        res2 = await reserve_seat_lua(redis_conn, seat_key, "r:1:reservation:R2", mask=2, res_id="R2", ttl=60)
        assert res2 == 0
        
        # 4. Non-overlapping reservation for mask 4 (binary 0100, segment 2) should succeed on the same seat
        res3 = await reserve_seat_lua(redis_conn, seat_key, "r:1:reservation:R3", mask=4, res_id="R3", ttl=60)
        assert res3 == 1
        
        # 5. Clean up R1 reservation
        rel = await release_seat_lua(redis_conn, seat_key, res_key, mask=3, res_id="R1")
        assert rel == 1
        
        # 6. Trying to release again should return 0 (not owned or already released)
        rel2 = await release_seat_lua(redis_conn, seat_key, res_key, mask=3, res_id="R1")
        assert rel2 == 0

        # Clean keys
        await redis_conn.delete(seat_key, res_key, "r:1:reservation:R3")
        
    event_loop.run_until_complete(_impl())
