import redis.asyncio as aioredis
from src.app.config import settings

# Create a global asynchronous Redis client
_redis_client = aioredis.from_url(settings.REDIS_URL, decode_responses=True)

def get_redis():
    return _redis_client

# Redis Lua Script: Atomic check-and-lock with interval occupancy bitmap
LUA_RESERVE = """
local seat_key = KEYS[1]
local res_key = KEYS[2]
local mask = tonumber(ARGV[1])
local res_id = ARGV[2]
local ttl = tonumber(ARGV[3])

-- Get current seat occupancy bitmap
local occupied = redis.call("GET", seat_key)
if occupied == false then
    occupied = 0
else
    occupied = tonumber(occupied)
end

-- Bitwise AND of current occupancy with requested mask
if bit.band(occupied, mask) ~= 0 then
    return 0 -- Conflict: already occupied
end

-- Bitwise OR to book requested segments
local new_occupied = bit.bor(occupied, mask)
redis.call("SET", seat_key, new_occupied)

-- Store reservation metadata with state HELD and apply TTL
redis.call("HSET", res_key, "reservation_id", res_id, "mask", mask, "state", "HELD")
redis.call("EXPIRE", res_key, ttl)

return 1 -- Success
"""

# Redis Lua Script: Atomic unlock with ownership verification
LUA_RELEASE = """
local seat_key = KEYS[1]
local res_key = KEYS[2]
local mask = tonumber(ARGV[1])
local res_id = ARGV[2]

-- Verify reservation ownership before unlocking to prevent race conditions
local stored_res_id = redis.call("HGET", res_key, "reservation_id")
if stored_res_id ~= res_id then
    return 0 -- Ownership mismatch or already deleted
end

-- Get current seat occupancy bitmap
local occupied = redis.call("GET", seat_key)
if occupied == false then
    occupied = 0
else
    occupied = tonumber(occupied)
end

-- Bitwise clear: new = occupied & ~mask
local clean_mask = bit.bnot(mask)
local new_occupied = bit.band(occupied, clean_mask)

-- Save new bitmap and remove reservation hash
redis.call("SET", seat_key, new_occupied)
redis.call("DEL", res_key)

return 1 -- Success
"""

async def reserve_seat_lua(redis_conn, seat_key: str, res_key: str, mask: int, res_id: str, ttl: int) -> int:
    # Register and run the atomic reserve script
    script = redis_conn.register_script(LUA_RESERVE)
    return await script(keys=[seat_key, res_key], args=[mask, res_id, ttl])

async def release_seat_lua(redis_conn, seat_key: str, res_key: str, mask: int, res_id: str) -> int:
    # Register and run the atomic release script
    script = redis_conn.register_script(LUA_RELEASE)
    return await script(keys=[seat_key, res_key], args=[mask, res_id])
