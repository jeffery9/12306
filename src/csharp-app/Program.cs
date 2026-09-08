using System.Data;
using System.Text.Json;
using System.Threading.Channels;
using Microsoft.AspNetCore.Mvc;
using Npgsql;
using StackExchange.Redis;

var builder = WebApplication.CreateBuilder(args);

// ============================================================================
// 1. 核心高并发组件与连接池配置 (Dependency Injection Sandbox)
// ============================================================================

var postgresUrl = Environment.GetEnvironmentVariable("DATABASE_URL") 
    ?? "Host=localhost;Port=5432;Database=ticketing_db;Username=postgres;Password=postgres;Maximum Pool Size=100;Minimum Pool Size=10";

var redisUrl = Environment.GetEnvironmentVariable("REDIS_URL") ?? "localhost:6379";

// 注册高能 Npgsql 数据源与 Redis 客户端连接
builder.Services.AddSingleton(sp => new NpgsqlDataSourceBuilder(postgresUrl).Build());
builder.Services.AddSingleton<IConnectionMultiplexer>(sp => ConnectionMultiplexer.Connect(redisUrl));

// 进程内高并发无锁事件管道 (Equivalent to Go Channels)
var eventChannel = Channel.CreateBounded<OutboxEvent>(new BoundedChannelOptions(1000)
{
    SingleWriter = false,
    SingleReader = true,
    FullMode = BoundedChannelFullMode.Wait
});
builder.Services.AddSingleton(eventChannel);

// 注册发件箱轮询与余票缓存最终一致投影后台托管服务
builder.Services.AddHostedService<OutboxPublisherBackgroundWorker>();
builder.Services.AddHostedService<CQRSProjectorBackgroundWorker>();

var app = builder.Build();

// ============================================================================
// 2. Redis 内存预占原子 Lua 脚本定义 (LUA Scripts)
// ============================================================================

const string LUA_RESERVE = @"
    local seat_key = KEYS[1]
    local res_key = KEYS[2]
    local mask = tonumber(ARGV[1])
    local res_id = ARGV[2]
    local ttl = tonumber(ARGV[3])

    local current_mask = tonumber(redis.call('GET', seat_key) or '0')
    if bit.band(current_mask, mask) == 0 then
        local new_mask = bit.bor(current_mask, mask)
        redis.call('SET', seat_key, new_mask)
        redis.call('SET', res_key, res_id, 'EX', ttl)
        return 1
    else
        return 0
    end";

const string LUA_RELEASE = @"
    local seat_key = KEYS[1]
    local res_key = KEYS[2]
    local mask = tonumber(ARGV[1])
    local res_id = ARGV[2]

    local locked_id = redis.call('GET', res_key)
    if locked_id == res_id then
        local current_mask = tonumber(redis.call('GET', seat_key) or '0')
        local new_mask = bit.bxor(current_mask, mask)
        redis.call('SET', seat_key, new_mask)
        redis.call('DEL', res_key)
        return 1
    else
        return 0
    end";

// 预加载 Lua SHA 校验码
var redisClient = app.Services.GetRequiredService<IConnectionMultiplexer>();
var redisDb = redisClient.GetDatabase();
var server = redisClient.GetServer(redisClient.GetEndPoints()[0]);

var reserveSha = server.ScriptLoad(LUA_RESERVE);
var releaseSha = server.ScriptLoad(LUA_RELEASE);

// ============================================================================
// 3. 核心 API 路由与 Minimal 端点实现 (Minimal APIs)
// ============================================================================

// A. 最终一致性高并发可用余票查询 (CQRS Query - GET)
app.MapGet("/api/v1/query", async (
    [FromQuery] int schedule_id, 
    [FromQuery] int from_station_seq, 
    [FromQuery] int to_station_seq, 
    [FromQuery] string seat_class,
    NpgsqlDataSource dataSource, 
    IConnectionMultiplexer redis) =>
{
    var db = redis.GetDatabase();
    string cacheKey = $"q:availability:{schedule_id}:{seat_class}";
    string field = $"{from_station_seq}-{to_station_seq}";

    var cacheVal = await db.HashGetAsync(cacheKey, field);
    if (cacheVal.IsNullOrEmpty)
    {
        // 触发冷启动重算并投影
        await TicketingEngine.RecalculateAndProjectAsync(dataSource, db, schedule_id, seat_class);
        cacheVal = await db.HashGetAsync(cacheKey, field);
    }

    int availableCount = cacheVal.HasValue ? int.Parse(cacheVal.ToString()) : 0;
    return Results.Ok(new { available_seats = availableCount });
});

// B. 原子位图锁加 PostgreSQL 升序互斥锁区间预占 (Command Reserve - POST)
app.MapPost("/api/v1/reserve", async (
    [FromBody] ReserveRequest req,
    NpgsqlDataSource dataSource,
    IConnectionMultiplexer redis) =>
{
    var db = redis.GetDatabase();
    int mask = TicketingEngine.GetMask(req.from_station_seq, req.to_station_seq);
    string reservationId = Guid.NewGuid().ToString();
    string resKey = $"r:{req.schedule_id}:reservation:{reservationId}";

    // 1. 获取目标席级物理席位列表
    var seatIds = new List<int>();
    using (var conn = await dataSource.OpenConnectionAsync())
    using (var cmd = new NpgsqlCommand("SELECT id FROM seat WHERE schedule_id = @sid AND seat_class = @sc", conn))
    {
        cmd.Parameters.AddWithValue("sid", req.schedule_id);
        cmd.Parameters.AddWithValue("sc", req.seat_class);
        using (var reader = await cmd.ExecuteReaderAsync())
        {
            while (await reader.ReadAsync())
            {
                seatIds.Add(reader.GetInt32(0));
            }
        }
    }

    int reservedSeatId = 0;

    // 2. 高并发 Redis Lua 原子位图预占过滤
    foreach (var seatId in seatIds)
    {
        string seatKey = $"r:{req.schedule_id}:seat:{seatId}";
        
        // 如果 Redis 中还未加载位图，则回源数据库查询已被占用的区间
        if (!await db.KeyExistsAsync(seatKey))
        {
            int dbMask = 0;
            using (var conn = await dataSource.OpenConnectionAsync())
            using (var cmd = new NpgsqlCommand("SELECT segment_no FROM seat_segment WHERE seat_id = @sid AND state != 'AVAILABLE'", conn))
            {
                cmd.Parameters.AddWithValue("sid", seatId);
                using (var reader = await cmd.ExecuteReaderAsync())
                {
                    while (await reader.ReadAsync())
                    {
                        dbMask |= (1 << (reader.GetInt32(0) - 1));
                    }
                }
            }
            await db.StringSetAsync(seatKey, dbMask);
        }

        var luaRes = await db.ScriptEvaluateAsync(reserveSha, 
            new RedisKey[] { seatKey, resKey }, 
            new RedisValue[] { mask, reservationId, 900 }); // 15分钟生存期

        if ((int)luaRes == 1)
        {
            reservedSeatId = seatId;
            break;
        }
    }

    if (reservedSeatId == 0)
    {
        return Results.Conflict(new { error = "No seats available (Redis filtered)" });
    }

    // 3. PostgreSQL 升序 FOR UPDATE 互斥锁级原子占位
    using (var conn = await dataSource.OpenConnectionAsync())
    using (var tx = await conn.BeginTransactionAsync(IsolationLevel.ReadCommitted))
    {
        try
        {
            // 3.1 强制升序 FOR UPDATE，绝无死锁
            int segmentCount = 0;
            using (var lockCmd = new NpgsqlCommand(@"
                SELECT COUNT(*) 
                FROM seat_segment 
                WHERE schedule_id = @sid AND seat_id = @seat_id AND segment_no >= @from AND segment_no < @to AND state = 'AVAILABLE' 
                FOR UPDATE", conn, tx))
            {
                lockCmd.Parameters.AddWithValue("sid", req.schedule_id);
                lockCmd.Parameters.AddWithValue("seat_id", reservedSeatId);
                lockCmd.Parameters.AddWithValue("from", req.from_station_seq);
                lockCmd.Parameters.AddWithValue("to", req.to_station_seq);
                segmentCount = Convert.ToInt32(await lockCmd.ExecuteScalarAsync());
            }

            int expectedSegments = req.to_station_seq - req.from_station_seq;
            if (segmentCount != expectedSegments)
            {
                await TicketingEngine.RevertRedisAsync(db, req.schedule_id, reservedSeatId, reservationId, mask);
                return Results.Conflict(new { error = "Seat state conflict inside DB" });
            }

            // 3.2 更新物理状态
            using (var updateCmd = new NpgsqlCommand(@"
                UPDATE seat_segment 
                SET state = 'HELD', reservation_id = @res_id 
                WHERE schedule_id = @sid AND seat_id = @seat_id AND segment_no >= @from AND segment_no < @to", conn, tx))
            {
                updateCmd.Parameters.AddWithValue("res_id", reservationId);
                updateCmd.Parameters.AddWithValue("sid", req.schedule_id);
                updateCmd.Parameters.AddWithValue("seat_id", reservedSeatId);
                updateCmd.Parameters.AddWithValue("from", req.from_station_seq);
                updateCmd.Parameters.AddWithValue("to", req.to_station_seq);
                await updateCmd.ExecuteNonQueryAsync();
            }

            // 3.3 插入预留单
            DateTime expiresAt = DateTime.UtcNow.AddMinutes(15);
            using (var resCmd = new NpgsqlCommand(@"
                INSERT INTO reservation (id, request_id, schedule_id, seat_id, from_segment, to_segment, state, expires_at) 
                VALUES (@id, @req_id, @sid, @seat_id, @from, @to, 'HELD', @expires)", conn, tx))
            {
                resCmd.Parameters.AddWithValue("id", reservationId);
                resCmd.Parameters.AddWithValue("req_id", req.request_id);
                resCmd.Parameters.AddWithValue("sid", req.schedule_id);
                resCmd.Parameters.AddWithValue("seat_id", reservedSeatId);
                resCmd.Parameters.AddWithValue("from", req.from_station_seq);
                resCmd.Parameters.AddWithValue("to", req.to_station_seq);
                resCmd.Parameters.AddWithValue("expires", expiresAt);
                await resCmd.ExecuteNonQueryAsync();
            }

            // 3.4 写入事件发件箱（Transactional Outbox）
            var payload = new { reservation_id = reservationId, schedule_id = req.schedule_id, seat_id = reservedSeatId, from_station_seq = req.from_station_seq, to_station_seq = req.to_station_seq };
            string payloadStr = JsonSerializer.Serialize(payload);
            string eventId = Guid.NewGuid().ToString();

            using (var outboxCmd = new NpgsqlCommand(@"
                INSERT INTO outbox_event (event_id, aggregate_type, aggregate_id, event_type, payload, status) 
                VALUES (@id, 'Reservation', @agg_id, 'RESERVATION_HELD', @payload::jsonb, 'NEW')", conn, tx))
            {
                outboxCmd.Parameters.AddWithValue("id", eventId);
                outboxCmd.Parameters.AddWithValue("agg_id", reservationId);
                outboxCmd.Parameters.AddWithValue("payload", payloadStr);
                await outboxCmd.ExecuteNonQueryAsync();
            }

            await tx.CommitAsync();
            return Results.Ok(new { reservation_id = reservationId, seat_id = reservedSeatId });
        }
        catch (Exception)
        {
            await tx.RollbackAsync();
            await TicketingEngine.RevertRedisAsync(db, req.schedule_id, reservedSeatId, reservationId, mask);
            return Results.StatusCode(500);
        }
    }
});

// C. 创建交易支付订单 (Order Engine - POST)
app.MapPost("/api/v1/order", async (
    [FromBody] OrderRequest req,
    NpgsqlDataSource dataSource) =>
{
    string orderId = Guid.NewGuid().ToString();
    DateTime expiresAt = DateTime.UtcNow.AddMinutes(15);

    using (var conn = await dataSource.OpenConnectionAsync())
    using (var cmd = new NpgsqlCommand(@"
        INSERT INTO orders (id, request_id, reservation_id, state, total_amount, expires_at) 
        VALUES (@id, @req_id, @res_id, 'WAITING_PAYMENT', @amount, @expires)", conn))
    {
        cmd.Parameters.AddWithValue("id", orderId);
        cmd.Parameters.AddWithValue("req_id", req.request_id);
        cmd.Parameters.AddWithValue("res_id", req.reservation_id);
        cmd.Parameters.AddWithValue("amount", req.amount);
        cmd.Parameters.AddWithValue("expires", expiresAt);
        await cmd.ExecuteNonQueryAsync();
    }

    return Results.Ok(new { order_id = orderId, state = "WAITING_PAYMENT" });
});

// D. 模拟支付核销最终一致锁扣减 (Pay Process - POST)
app.MapPost("/api/v1/pay", async (
    [FromBody] PayRequest req,
    NpgsqlDataSource dataSource) =>
{
    using (var conn = await dataSource.OpenConnectionAsync())
    using (var tx = await conn.BeginTransactionAsync(IsolationLevel.ReadCommitted))
    {
        try
        {
            // 1. 锁订单
            string reservationId = "";
            string orderState = "";
            using (var orderCmd = new NpgsqlCommand("SELECT reservation_id, state FROM orders WHERE id = @id FOR UPDATE", conn, tx))
            {
                orderCmd.Parameters.AddWithValue("id", req.order_id);
                using (var reader = await orderCmd.ExecuteReaderAsync())
                {
                    if (await reader.ReadAsync())
                    {
                        reservationId = reader.GetString(0);
                        orderState = reader.GetString(1);
                    }
                }
            }

            if (orderState != "WAITING_PAYMENT")
            {
                return Results.Conflict(new { error = "Order cannot be paid" });
            }

            // 2. 查询预留座位信息
            int scheduleId = 0, seatId = 0, fromSeq = 0, toSeq = 0;
            using (var resCmd = new NpgsqlCommand("SELECT schedule_id, seat_id, from_segment, to_segment FROM reservation WHERE id = @id", conn, tx))
            {
                resCmd.Parameters.AddWithValue("id", reservationId);
                using (var reader = await resCmd.ExecuteReaderAsync())
                {
                    if (await reader.ReadAsync())
                    {
                        scheduleId = reader.GetInt32(0);
                        seatId = reader.GetInt32(1);
                        fromSeq = reader.GetInt32(2);
                        toSeq = reader.GetInt32(3);
                    }
                }
            }

            // 3. 推进状态机
            using (var upOrder = new NpgsqlCommand("UPDATE orders SET state = 'CONFIRMED' WHERE id = @id", conn, tx))
            {
                upOrder.Parameters.AddWithValue("id", req.order_id);
                await upOrder.ExecuteNonQueryAsync();
            }

            using (var upRes = new NpgsqlCommand("UPDATE reservation SET state = 'CONFIRMED' WHERE id = @id", conn, tx))
            {
                upRes.Parameters.AddWithValue("id", reservationId);
                await upRes.ExecuteNonQueryAsync();
            }

            using (var upSeg = new NpgsqlCommand(@"
                UPDATE seat_segment 
                SET state = 'CONFIRMED' 
                WHERE schedule_id = @sid AND seat_id = @seat_id AND segment_no >= @from AND segment_no < @to", conn, tx))
            {
                upSeg.Parameters.AddWithValue("sid", scheduleId);
                upSeg.Parameters.AddWithValue("seat_id", seatId);
                upSeg.Parameters.AddWithValue("from", fromSeq);
                upSeg.Parameters.AddWithValue("to", toSeq);
                await upSeg.ExecuteNonQueryAsync();
            }

            // 4. 发送已支付事件
            var payload = new { order_id = req.order_id, schedule_id = scheduleId, reservation_id = reservationId };
            string payloadStr = JsonSerializer.Serialize(payload);
            string eventId = Guid.NewGuid().ToString();

            using (var outboxCmd = new NpgsqlCommand(@"
                INSERT INTO outbox_event (event_id, aggregate_type, aggregate_id, event_type, payload, status) 
                VALUES (@id, 'Order', @agg_id, 'ORDER_PAID', @payload::jsonb, 'NEW')", conn, tx))
            {
                outboxCmd.Parameters.AddWithValue("id", eventId);
                outboxCmd.Parameters.AddWithValue("agg_id", req.order_id);
                outboxCmd.Parameters.AddWithValue("payload", payloadStr);
                await outboxCmd.ExecuteNonQueryAsync();
            }

            await tx.CommitAsync();
            return Results.Ok(new { success = true });
        }
        catch (Exception ex)
        {
            await tx.RollbackAsync();
            return Results.BadRequest(new { error = ex.Message });
        }
    }
});

// E. SRE 级别系统健康探针检测 (Health Check - GET)
app.MapGet("/api/v1/ops/health", async (NpgsqlDataSource dataSource, IConnectionMultiplexer redis) =>
{
    try
    {
        using (var conn = await dataSource.OpenConnectionAsync())
        using (var cmd = new NpgsqlCommand("SELECT 1", conn))
        {
            await cmd.ExecuteScalarAsync();
        }

        var db = redis.GetDatabase();
        await db.PingAsync();

        return Results.Ok(new { status = "healthy", postgresql = "up", redis = "up" });
    }
    catch (Exception)
    {
        return Results.StatusCode(500);
    }
});

// F. TRS 物理排班列车一键导入 (Import - POST)
app.MapPost("/api/v1/ops/trs/import-schedule", async (
    [FromBody] ImportScheduleRequest req,
    NpgsqlDataSource dataSource) =>
{
    using (var conn = await dataSource.OpenConnectionAsync())
    using (var tx = await conn.BeginTransactionAsync(IsolationLevel.ReadCommitted))
    {
        try
        {
            // 1. Train insert: Single-Query CTE
            int trainId = 0;
            using (var cmd = new NpgsqlCommand(@"
                WITH s AS (SELECT id FROM train WHERE code = @code),
                     i AS (INSERT INTO train (code) VALUES (@code) ON CONFLICT (code) DO NOTHING RETURNING id)
                SELECT id FROM i UNION ALL SELECT id FROM s LIMIT 1", conn, tx))
            {
                cmd.Parameters.AddWithValue("code", req.train_code);
                trainId = Convert.ToInt32(await cmd.ExecuteScalarAsync());
            }

            // 2. Stations insert
            foreach (var st in req.stations)
            {
                using (var cmd = new NpgsqlCommand(@"
                    INSERT INTO station (train_id, name, sequence) 
                    VALUES (@tid, @name, @seq) 
                    ON CONFLICT (train_id, name) DO NOTHING", conn, tx))
                {
                    cmd.Parameters.AddWithValue("tid", trainId);
                    cmd.Parameters.AddWithValue("name", st.name);
                    cmd.Parameters.AddWithValue("seq", st.sequence);
                    await cmd.ExecuteNonQueryAsync();
                }
            }

            // 3. TrainSchedule insert
            int scheduleId = 0;
            using (var cmd = new NpgsqlCommand(@"
                WITH s AS (SELECT id FROM train_schedule WHERE train_id = @tid AND service_date = @sdate),
                     i AS (INSERT INTO train_schedule (train_id, service_date, status) VALUES (@tid, @sdate, 'ACTIVE') ON CONFLICT (train_id, service_date) DO NOTHING RETURNING id)
                SELECT id FROM i UNION ALL SELECT id FROM s LIMIT 1", conn, tx))
            {
                cmd.Parameters.AddWithValue("tid", trainId);
                cmd.Parameters.AddWithValue("sdate", DateTime.Parse(req.service_date));
                scheduleId = Convert.ToInt32(await cmd.ExecuteScalarAsync());
            }

            // 4. Seats & Segments insert
            int maxSeq = req.stations.Count;
            foreach (var seat in req.seats)
            {
                int seatId = 0;
                using (var cmd = new NpgsqlCommand(@"
                    WITH s AS (SELECT id FROM seat WHERE schedule_id = @sid AND carriage_no = @c_no AND seat_no = @s_no),
                         i AS (INSERT INTO seat (schedule_id, carriage_no, seat_no, seat_class, is_long_distance_pool, quota_released) VALUES (@sid, @c_no, @s_no, @s_cl, 0, 0) ON CONFLICT (schedule_id, carriage_no, seat_no) DO NOTHING RETURNING id)
                    SELECT id FROM i UNION ALL SELECT id FROM s LIMIT 1", conn, tx))
                {
                    cmd.Parameters.AddWithValue("sid", scheduleId);
                    cmd.Parameters.AddWithValue("c_no", seat.carriage_no);
                    cmd.Parameters.AddWithValue("s_no", seat.seat_no);
                    cmd.Parameters.AddWithValue("s_cl", seat.seat_class);
                    seatId = Convert.ToInt32(await cmd.ExecuteScalarAsync());
                }

                // 创建段 (Segments)
                for (int i = 1; i < maxSeq; i++)
                {
                    using (var cmd = new NpgsqlCommand(@"
                        INSERT INTO seat_segment (schedule_id, seat_id, segment_no, state, version) 
                        VALUES (@sid, @seat_id, @seg_no, 'AVAILABLE', 0) 
                        ON CONFLICT (schedule_id, seat_id, segment_no) DO NOTHING", conn, tx))
                    {
                        cmd.Parameters.AddWithValue("sid", scheduleId);
                        cmd.Parameters.AddWithValue("seat_id", seatId);
                        cmd.Parameters.AddWithValue("seg_no", i);
                        await cmd.ExecuteNonQueryAsync();
                    }
                }
            }

            // 5. Submit event to Outbox
            var payload = new { schedule_id = scheduleId };
            string payloadStr = JsonSerializer.Serialize(payload);
            string eventId = Guid.NewGuid().ToString();

            using (var cmd = new NpgsqlCommand(@"
                INSERT INTO outbox_event (event_id, aggregate_type, aggregate_id, event_type, payload, status) 
                VALUES (@id, 'TrainSchedule', @agg_id, 'SCHEDULE_IMPORTED', @payload::jsonb, 'NEW')", conn, tx))
            {
                cmd.Parameters.AddWithValue("id", eventId);
                cmd.Parameters.AddWithValue("agg_id", scheduleId.ToString());
                cmd.Parameters.AddWithValue("payload", payloadStr);
                await cmd.ExecuteNonQueryAsync();
            }

            await tx.CommitAsync();
            return Results.Ok(new { success = true, schedule_id = scheduleId });
        }
        catch (Exception ex)
        {
            await tx.RollbackAsync();
            return Results.BadRequest(new { error = ex.Message });
        }
    }
});

var port = Environment.GetEnvironmentVariable("PORT") ?? "8002";
app.Run($"http://0.0.0.0:{port}");

// ============================================================================
// 4. 重算与投影核心算法静态容器 (Encapsulated Engine Helper Methods)
// ============================================================================

public static class TicketingEngine
{
    public static async Task RecalculateAndProjectAsync(NpgsqlDataSource dataSource, IDatabase db, int scheduleId, string seatClass)
    {
        int maxSeq = 4;
        using (var conn = await dataSource.OpenConnectionAsync())
        {
            using (var cmd = new NpgsqlCommand(@"
                SELECT COALESCE(MAX(sequence), 4) 
                FROM station s 
                JOIN train_schedule ts ON s.train_id = ts.train_id 
                WHERE ts.id = @sid", conn))
            {
                cmd.Parameters.AddWithValue("sid", scheduleId);
                var res = await cmd.ExecuteScalarAsync();
                if (res != null && res != DBNull.Value)
                {
                    maxSeq = Convert.ToInt32(res);
                }
            }
        }

        var seats = new List<(int Id, string Class)>();
        using (var conn = await dataSource.OpenConnectionAsync())
        using (var cmd = new NpgsqlCommand("SELECT id, seat_class FROM seat WHERE schedule_id = @sid", conn))
        {
            cmd.Parameters.AddWithValue("sid", scheduleId);
            using (var reader = await cmd.ExecuteReaderAsync())
            {
                while (await reader.ReadAsync())
                {
                    seats.Add((reader.GetInt32(0), reader.GetString(1)));
                }
            }
        }

        var segments = new List<(int SeatId, int SegNo, string State)>();
        using (var conn = await dataSource.OpenConnectionAsync())
        using (var cmd = new NpgsqlCommand("SELECT seat_id, segment_no, state FROM seat_segment WHERE schedule_id = @sid", conn))
        {
            cmd.Parameters.AddWithValue("sid", scheduleId);
            using (var reader = await cmd.ExecuteReaderAsync())
            {
                while (await reader.ReadAsync())
                {
                    segments.Add((reader.GetInt32(0), reader.GetInt32(1), reader.GetString(2)));
                }
            }
        }

        // 将 Segments 依据 seat_id + seg_no 归档，便于快速多区间滑动检索
        var segmentMap = new Dictionary<int, Dictionary<int, string>>();
        foreach (var seg in segments)
        {
            if (!segmentMap.ContainsKey(seg.SeatId))
            {
                segmentMap[seg.SeatId] = new Dictionary<int, string>();
            }
            segmentMap[seg.SeatId][seg.SegNo] = seg.State;
        }

        // 计算各站区间余票
        var counts = new Dictionary<string, string>();
        for (int from = 1; from < maxSeq; from++)
        {
            for (int to = from + 1; to <= maxSeq; to++)
            {
                int availableCount = 0;
                foreach (var seat in seats)
                {
                    if (seat.Class != seatClass) continue;

                    bool isAvailable = true;
                    for (int s = from; s < to; s++)
                    {
                        if (segmentMap.TryGetValue(seat.Id, out var segs) && segs.TryGetValue(s, out var state))
                        {
                            if (state != "AVAILABLE")
                            {
                                isAvailable = false;
                                break;
                            }
                        }
                        else
                        {
                            isAvailable = false;
                            break;
                        }
                    }
                    if (isAvailable) availableCount++;
                }
                counts[$"{from}-{to}"] = availableCount.ToString();
            }
        }

        string cacheKey = $"q:availability:{scheduleId}:{seatClass}";
        await db.KeyDeleteAsync(cacheKey);
        if (counts.Count > 0)
        {
            var hashEntries = counts.Select(kv => new HashEntry(kv.Key, kv.Value)).ToArray();
            await db.HashSetAsync(cacheKey, hashEntries);
            await db.KeyExpireAsync(cacheKey, TimeSpan.FromMinutes(15));
        }
    }

    public static int GetMask(int fromSeq, int toSeq)
    {
        int mask = 0;
        for (int i = fromSeq; i < toSeq; i++)
        {
            mask |= (1 << (i - 1));
        }
        return mask;
    }

    public static async Task RevertRedisAsync(IDatabase db, int scheduleId, int seatId, string reservationId, int mask)
    {
        string seatKey = $"r:{scheduleId}:seat:{seatId}";
        string resKey = $"r:{scheduleId}:reservation:{reservationId}";
        const string LUA_RELEASE_INLINE = @"
            local seat_key = KEYS[1]
            local res_key = KEYS[2]
            local mask = tonumber(ARGV[1])
            local res_id = ARGV[2]

            local locked_id = redis.call('GET', res_key)
            if locked_id == res_id then
                local current_mask = tonumber(redis.call('GET', seat_key) or '0')
                local new_mask = bit.bxor(current_mask, mask)
                redis.call('SET', seat_key, new_mask)
                redis.call('DEL', res_key)
                return 1
            else
                return 0
            end";
        await db.ScriptEvaluateAsync(LUA_RELEASE_INLINE, new RedisKey[] { seatKey, resKey }, new RedisValue[] { mask, reservationId });
    }
}

// ============================================================================
// 5. C# 高性能后台协程托管服务 (Standard ASP.NET Core Hosted Services)
// ============================================================================

// A. 轮询本地发件箱 (Outbox Publisher Service)
public class OutboxPublisherBackgroundWorker : BackgroundService
{
    private readonly NpgsqlDataSource _dataSource;
    private readonly Channel<OutboxEvent> _channel;
    private readonly ILogger<OutboxPublisherBackgroundWorker> _logger;

    public OutboxPublisherBackgroundWorker(
        NpgsqlDataSource dataSource, 
        Channel<OutboxEvent> channel,
        ILogger<OutboxPublisherBackgroundWorker> logger)
    {
        _dataSource = dataSource;
        _channel = channel;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("🌌 C# Outbox Publisher Background Service started.");
        while (!stoppingToken.IsCancellationRequested)
        {
            try
            {
                var events = new List<OutboxEvent>();

                using (var conn = await _dataSource.OpenConnectionAsync(stoppingToken))
                using (var tx = await conn.BeginTransactionAsync(IsolationLevel.ReadCommitted, stoppingToken))
                {
                    // 极速无锁高并发捞取：SELECT FOR UPDATE SKIP LOCKED
                    using (var cmd = new NpgsqlCommand(@"
                        SELECT id, event_id, aggregate_type, aggregate_id, event_type, payload::text, status 
                        FROM outbox_event 
                        WHERE status = 'NEW' 
                        ORDER BY id ASC 
                        LIMIT 100 
                        FOR UPDATE SKIP LOCKED", conn, tx))
                    {
                        using (var reader = await cmd.ExecuteReaderAsync(stoppingToken))
                        {
                            while (await reader.ReadAsync(stoppingToken))
                            {
                                events.Add(new OutboxEvent(
                                    reader.GetInt64(0),
                                    reader.GetString(1),
                                    reader.GetString(2),
                                    reader.GetString(3),
                                    reader.GetString(4),
                                    reader.GetString(5),
                                    reader.GetString(6)
                                ));
                            }
                        }
                    }

                    if (events.Count > 0)
                    {
                        foreach (var ev in events)
                        {
                            using (var updateCmd = new NpgsqlCommand(@"
                                UPDATE outbox_event 
                                SET status = 'PROCESSED', published_at = NOW() 
                                WHERE id = @id", conn, tx))
                            {
                                updateCmd.Parameters.AddWithValue("id", ev.ID);
                                await updateCmd.ExecuteNonQueryAsync(stoppingToken);
                            }
                        }
                        await tx.CommitAsync(stoppingToken);
                    }
                }

                // 将事件推送给无锁管道
                foreach (var ev in events)
                {
                    await _channel.Writer.WriteAsync(ev, stoppingToken);
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error occurred in C# Outbox Publisher Background Service");
            }

            await Task.Delay(100, stoppingToken); // 轮询周期
        }
    }
}

// B. 消费无锁队列，更新 Redis 余票投影 (CQRS Projector Service)
public class CQRSProjectorBackgroundWorker : BackgroundService
{
    private readonly Channel<OutboxEvent> _channel;
    private readonly NpgsqlDataSource _dataSource;
    private readonly IConnectionMultiplexer _redis;
    private readonly ILogger<CQRSProjectorBackgroundWorker> _logger;

    public CQRSProjectorBackgroundWorker(
        Channel<OutboxEvent> channel, 
        NpgsqlDataSource dataSource, 
        IConnectionMultiplexer redis,
        ILogger<CQRSProjectorBackgroundWorker> logger)
    {
        _channel = channel;
        _dataSource = dataSource;
        _redis = redis;
        _logger = logger;
    }

    protected override async Task ExecuteAsync(CancellationToken stoppingToken)
    {
        _logger.LogInformation("🌌 C# CQRS Projector Background Service started.");
        var db = _redis.GetDatabase();

        await foreach (var ev in _channel.Reader.ReadAllAsync(stoppingToken))
        {
            try
            {
                int scheduleId = 0;
                if (ev.EventType == "SCHEDULE_IMPORTED" || ev.EventType == "RESERVATION_HELD")
                {
                    using (var doc = JsonDocument.Parse(ev.Payload))
                    {
                        scheduleId = doc.RootElement.GetProperty("schedule_id").GetInt32();
                    }
                }
                else if (ev.EventType == "ORDER_PAID")
                {
                    using (var doc = JsonDocument.Parse(ev.Payload))
                    {
                        scheduleId = doc.RootElement.GetProperty("schedule_id").GetInt32();
                    }
                }

                if (scheduleId > 0)
                {
                    // 异步重算缓存并投影
                    await TicketingEngine.RecalculateAndProjectAsync(_dataSource, db, scheduleId, "BUSINESS");
                    _logger.LogInformation($"[Projector] Projecting availability completed for schedule {scheduleId}");
                }
            }
            catch (Exception ex)
            {
                _logger.LogError(ex, "Error occurred in C# CQRS Projector Background Service processing event {ID}", ev.EventID);
            }
        }
    }
}

// ============================================================================
// 6. C# 领域数据实体对象定义 (Domain Models)
// ============================================================================

public record OutboxEvent(
    long ID,
    string EventID,
    string AggregateType,
    string AggregateID,
    string EventType,
    string Payload,
    string Status
);

public record ReserveRequest(
    string request_id,
    int schedule_id,
    int from_station_seq,
    int to_station_seq,
    string seat_class,
    List<string> passenger_ids
);

public record OrderRequest(
    string request_id,
    string reservation_id,
    decimal amount
);

public record PayRequest(
    string order_id
);

public record StationImportModel(
    string name,
    int sequence
);

public record SeatImportModel(
    string carriage_no,
    string seat_no,
    string seat_class
);

public record ImportScheduleRequest(
    string train_code,
    string service_date,
    List<StationImportModel> stations,
    List<SeatImportModel> seats
);
