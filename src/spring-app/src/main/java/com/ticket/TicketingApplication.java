package com.ticket;

import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ObjectNode;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.context.annotation.Bean;
import org.springframework.core.io.ClassPathResource;
import org.springframework.data.redis.core.StringRedisTemplate;
import org.springframework.data.redis.core.script.DefaultRedisScript;
import org.springframework.http.HttpStatus;
import org.springframework.http.ResponseEntity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.annotation.EnableScheduling;
import org.springframework.scheduling.annotation.Scheduled;
import org.springframework.transaction.annotation.Isolation;
import org.springframework.transaction.annotation.Transactional;
import org.springframework.web.bind.annotation.*;
import org.springframework.stereotype.Service;

import java.util.*;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;

@SpringBootApplication
@EnableScheduling
public class TicketingApplication {

    public static void main(String[] args) {
        SpringApplication.run(TicketingApplication.class, args);
    }

    @Bean
    public BlockingQueue<String> outboxChannel() {
        return new LinkedBlockingQueue<>(1000);
    }
}

// ============================================================================
// Redis Lua Scripts Config
// ============================================================================
@org.springframework.context.annotation.Configuration
class RedisConfig {
    @Bean
    public DefaultRedisScript<Long> reserveScript() {
        DefaultRedisScript<Long> script = new DefaultRedisScript<>();
        script.setScriptText(
            "local seat_key = KEYS[1]\n" +
            "local res_key = KEYS[2]\n" +
            "local mask = tonumber(ARGV[1])\n" +
            "local res_id = ARGV[2]\n" +
            "local ttl = tonumber(ARGV[3])\n" +
            "local current_mask = tonumber(redis.call('GET', seat_key) or '0')\n" +
            "if bit.band(current_mask, mask) == 0 then\n" +
            "    local new_mask = bit.bor(current_mask, mask)\n" +
            "    redis.call('SET', seat_key, new_mask)\n" +
            "    redis.call('SET', res_key, res_id, 'EX', ttl)\n" +
            "    return 1\n" +
            "else\n" +
            "    return 0\n" +
            "end"
        );
        script.setResultType(Long.class);
        return script;
    }

    @Bean
    public DefaultRedisScript<Long> releaseScript() {
        DefaultRedisScript<Long> script = new DefaultRedisScript<>();
        script.setScriptText(
            "local seat_key = KEYS[1]\n" +
            "local res_key = KEYS[2]\n" +
            "local mask = tonumber(ARGV[1])\n" +
            "local res_id = ARGV[2]\n" +
            "local locked_id = redis.call('GET', res_key)\n" +
            "if locked_id == res_id then\n" +
            "    local current_mask = tonumber(redis.call('GET', seat_key) or '0')\n" +
            "    local new_mask = bit.bxor(current_mask, mask)\n" +
            "    redis.call('SET', seat_key, new_mask)\n" +
            "    redis.call('DEL', res_key)\n" +
            "    return 1\n" +
            "else\n" +
            "    return 0\n" +
            "end"
        );
        script.setResultType(Long.class);
        return script;
    }
}

// ============================================================================
// Controllers
// ============================================================================
@RestController
@RequestMapping("/api/v1")
@CrossOrigin(origins = "*")
class TicketController {

    private final TicketingService ticketingService;
    private final JdbcTemplate jdbcTemplate;
    private final StringRedisTemplate redisTemplate;

    public TicketController(TicketingService ticketingService, JdbcTemplate jdbcTemplate, StringRedisTemplate redisTemplate) {
        this.ticketingService = ticketingService;
        this.jdbcTemplate = jdbcTemplate;
        this.redisTemplate = redisTemplate;
    }

    @GetMapping("/query")
    public Map<String, Object> query(
            @RequestParam("schedule_id") int scheduleId,
            @RequestParam("from_station_seq") int fromSeq,
            @RequestParam("to_station_seq") int toSeq,
            @RequestParam("seat_class") String seatClass) {
        
        String cacheKey = "q:availability:" + scheduleId + ":" + seatClass;
        String field = fromSeq + "-" + toSeq;
        
        Object val = redisTemplate.opsForHash().get(cacheKey, field);
        if (val == null) {
            ticketingService.recalculateAndProject(scheduleId, seatClass);
            val = redisTemplate.opsForHash().get(cacheKey, field);
        }
        
        int availableCount = val != null ? Integer.parseInt(val.toString()) : 0;
        return Map.of("available_seats", availableCount);
    }

    @PostMapping("/reserve")
    public ResponseEntity<?> reserve(@RequestBody JsonNode req) {
        try {
            Map<String, Object> res = ticketingService.reserveTicket(req);
            return ResponseEntity.ok(res);
        } catch (IllegalStateException e) {
            return ResponseEntity.status(HttpStatus.CONFLICT).body(Map.of("error", e.getMessage()));
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(Map.of("error", e.getMessage()));
        }
    }

    @PostMapping("/order")
    public ResponseEntity<?> createOrder(@RequestBody JsonNode req) {
        try {
            Map<String, Object> res = ticketingService.createOrder(req);
            return ResponseEntity.ok(res);
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(Map.of("error", e.getMessage()));
        }
    }

    @PostMapping("/pay")
    public ResponseEntity<?> pay(@RequestBody JsonNode req) {
        try {
            ticketingService.payOrder(req);
            return ResponseEntity.ok(Map.of("success", true));
        } catch (IllegalStateException e) {
            return ResponseEntity.status(HttpStatus.CONFLICT).body(Map.of("error", e.getMessage()));
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(Map.of("error", e.getMessage()));
        }
    }

    @GetMapping("/ops/health")
    public Map<String, String> health() {
        jdbcTemplate.execute("SELECT 1");
        redisTemplate.getConnectionFactory().getConnection().ping();
        return Map.of("status", "healthy", "postgresql", "up", "redis", "up");
    }

    @PostMapping("/ops/trs/import-schedule")
    public ResponseEntity<?> importSchedule(@RequestBody JsonNode req) {
        try {
            int scheduleId = ticketingService.importSchedule(req);
            return ResponseEntity.ok(Map.of("success", true, "schedule_id", scheduleId));
        } catch (Exception e) {
            return ResponseEntity.status(HttpStatus.INTERNAL_SERVER_ERROR).body(Map.of("error", e.getMessage()));
        }
    }
}

// ============================================================================
// Service Layer
// ============================================================================
@Service
class TicketingService {

    private final JdbcTemplate jdbc;
    private final StringRedisTemplate redis;
    private final DefaultRedisScript<Long> reserveScript;
    private final DefaultRedisScript<Long> releaseScript;
    private final ObjectMapper mapper = new ObjectMapper();

    public TicketingService(JdbcTemplate jdbc, StringRedisTemplate redis, 
                            DefaultRedisScript<Long> reserveScript, DefaultRedisScript<Long> releaseScript) {
        this.jdbc = jdbc;
        this.redis = redis;
        this.reserveScript = reserveScript;
        this.releaseScript = releaseScript;
    }

    public int getMask(int fromSeq, int toSeq) {
        int mask = 0;
        for (int i = fromSeq; i < toSeq; i++) {
            mask |= (1 << (i - 1));
        }
        return mask;
    }

    public void recalculateAndProject(int scheduleId, String seatClass) {
        Integer maxSeq = jdbc.queryForObject(
            "SELECT COALESCE(MAX(sequence), 4) FROM station s JOIN train_schedule ts ON s.train_id = ts.train_id WHERE ts.id = ?", 
            Integer.class, scheduleId);
        if (maxSeq == null) maxSeq = 4;

        List<Map<String, Object>> seats = jdbc.queryForList("SELECT id, seat_class FROM seat WHERE schedule_id = ?", scheduleId);
        List<Map<String, Object>> segments = jdbc.queryForList("SELECT seat_id, segment_no, state FROM seat_segment WHERE schedule_id = ?", scheduleId);

        Map<Integer, Map<Integer, String>> segmentMap = new HashMap<>();
        for (Map<String, Object> seg : segments) {
            int seatId = (Integer) seg.get("seat_id");
            int segNo = (Integer) seg.get("segment_no");
            String state = (String) seg.get("state");
            segmentMap.computeIfAbsent(seatId, k -> new HashMap<>()).put(segNo, state);
        }

        Map<String, String> counts = new HashMap<>();
        for (int from = 1; from < maxSeq; from++) {
            for (int to = from + 1; to <= maxSeq; to++) {
                int availableCount = 0;
                for (Map<String, Object> seat : seats) {
                    if (!seatClass.equals(seat.get("seat_class"))) continue;
                    int seatId = (Integer) seat.get("id");
                    
                    boolean isAvailable = true;
                    for (int s = from; s < to; s++) {
                        Map<Integer, String> segs = segmentMap.get(seatId);
                        if (segs == null || !"AVAILABLE".equals(segs.get(s))) {
                            isAvailable = false;
                            break;
                        }
                    }
                    if (isAvailable) availableCount++;
                }
                counts.put(from + "-" + to, String.valueOf(availableCount));
            }
        }

        String cacheKey = "q:availability:" + scheduleId + ":" + seatClass;
        redis.delete(cacheKey);
        if (!counts.isEmpty()) {
            redis.opsForHash().putAll(cacheKey, counts);
            redis.expire(cacheKey, java.time.Duration.ofMinutes(15));
        }
    }

    @Transactional(isolation = Isolation.READ_COMMITTED)
    public Map<String, Object> reserveTicket(JsonNode req) {
        int scheduleId = req.get("schedule_id").asInt();
        int fromSeq = req.get("from_station_seq").asInt();
        int toSeq = req.get("to_station_seq").asInt();
        String seatClass = req.get("seat_class").asText();
        String requestId = req.get("request_id").asText();

        int mask = getMask(fromSeq, toSeq);
        String reservationId = UUID.randomUUID().toString();
        String resKey = "r:" + scheduleId + ":reservation:" + reservationId;

        List<Integer> seatIds = jdbc.queryForList(
            "SELECT id FROM seat WHERE schedule_id = ? AND seat_class = ?", Integer.class, scheduleId, seatClass);

        int reservedSeatId = 0;

        for (int seatId : seatIds) {
            String seatKey = "r:" + scheduleId + ":seat:" + seatId;
            
            if (Boolean.FALSE.equals(redis.hasKey(seatKey))) {
                List<Integer> segs = jdbc.queryForList(
                    "SELECT segment_no FROM seat_segment WHERE seat_id = ? AND state != 'AVAILABLE'", Integer.class, seatId);
                int dbMask = 0;
                for (int s : segs) {
                    dbMask |= (1 << (s - 1));
                }
                redis.opsForValue().set(seatKey, String.valueOf(dbMask));
            }

            Long luaRes = redis.execute(reserveScript, Arrays.asList(seatKey, resKey), String.valueOf(mask), reservationId, "900");
            if (luaRes != null && luaRes == 1L) {
                reservedSeatId = seatId;
                break;
            }
        }

        if (reservedSeatId == 0) {
            throw new IllegalStateException("No seats available (Redis filtered)");
        }

        try {
            Integer count = jdbc.queryForObject(
                "SELECT COUNT(*) FROM seat_segment WHERE schedule_id = ? AND seat_id = ? AND segment_no >= ? AND segment_no < ? AND state = 'AVAILABLE' FOR UPDATE",
                Integer.class, scheduleId, reservedSeatId, fromSeq, toSeq);
            
            if (count == null || count != (toSeq - fromSeq)) {
                throw new IllegalStateException("Seat state conflict inside DB");
            }

            jdbc.update("UPDATE seat_segment SET state = 'HELD', reservation_id = ? WHERE schedule_id = ? AND seat_id = ? AND segment_no >= ? AND segment_no < ?",
                reservationId, scheduleId, reservedSeatId, fromSeq, toSeq);

            jdbc.update("INSERT INTO reservation (id, request_id, schedule_id, seat_id, from_segment, to_segment, state, expires_at) VALUES (?, ?, ?, ?, ?, ?, 'HELD', ?)",
                reservationId, requestId, scheduleId, reservedSeatId, fromSeq, toSeq, new Date(System.currentTimeMillis() + 900000));

            ObjectNode payload = mapper.createObjectNode();
            payload.put("reservation_id", reservationId);
            payload.put("schedule_id", scheduleId);
            payload.put("seat_id", reservedSeatId);
            payload.put("from_station_seq", fromSeq);
            payload.put("to_station_seq", toSeq);

            jdbc.update("INSERT INTO outbox_event (event_id, aggregate_type, aggregate_id, event_type, payload, status) VALUES (?, 'Reservation', ?, 'RESERVATION_HELD', ?::jsonb, 'NEW')",
                UUID.randomUUID().toString(), reservationId, payload.toString());

            return Map.of("reservation_id", reservationId, "seat_id", reservedSeatId);
        } catch (Exception e) {
            String seatKey = "r:" + scheduleId + ":seat:" + reservedSeatId;
            redis.execute(releaseScript, Arrays.asList(seatKey, resKey), String.valueOf(mask), reservationId);
            throw e;
        }
    }

    public Map<String, Object> createOrder(JsonNode req) {
        String orderId = UUID.randomUUID().toString();
        jdbc.update("INSERT INTO orders (id, request_id, reservation_id, state, total_amount, expires_at) VALUES (?, ?, ?, 'WAITING_PAYMENT', ?, ?)",
            orderId, req.get("request_id").asText(), req.get("reservation_id").asText(), req.get("amount").asDouble(), new Date(System.currentTimeMillis() + 900000));
        return Map.of("order_id", orderId, "state", "WAITING_PAYMENT");
    }

    @Transactional(isolation = Isolation.READ_COMMITTED)
    public void payOrder(JsonNode req) {
        String orderId = req.get("order_id").asText();
        
        List<Map<String, Object>> orders = jdbc.queryForList("SELECT reservation_id, state FROM orders WHERE id = ? FOR UPDATE", orderId);
        if (orders.isEmpty() || !"WAITING_PAYMENT".equals(orders.get(0).get("state"))) {
            throw new IllegalStateException("Order cannot be paid");
        }
        String reservationId = (String) orders.get(0).get("reservation_id");

        Map<String, Object> res = jdbc.queryForMap("SELECT schedule_id, seat_id, from_segment, to_segment FROM reservation WHERE id = ?", reservationId);
        int scheduleId = (Integer) res.get("schedule_id");
        int seatId = (Integer) res.get("seat_id");
        int fromSeq = (Integer) res.get("from_segment");
        int toSeq = (Integer) res.get("to_segment");

        jdbc.update("UPDATE orders SET state = 'CONFIRMED' WHERE id = ?", orderId);
        jdbc.update("UPDATE reservation SET state = 'CONFIRMED' WHERE id = ?", reservationId);
        jdbc.update("UPDATE seat_segment SET state = 'CONFIRMED' WHERE schedule_id = ? AND seat_id = ? AND segment_no >= ? AND segment_no < ?", scheduleId, seatId, fromSeq, toSeq);

        ObjectNode payload = mapper.createObjectNode();
        payload.put("order_id", orderId);
        payload.put("schedule_id", scheduleId);
        payload.put("reservation_id", reservationId);

        jdbc.update("INSERT INTO outbox_event (event_id, aggregate_type, aggregate_id, event_type, payload, status) VALUES (?, 'Order', ?, 'ORDER_PAID', ?::jsonb, 'NEW')",
            UUID.randomUUID().toString(), orderId, payload.toString());
    }

    @Transactional(isolation = Isolation.READ_COMMITTED)
    public int importSchedule(JsonNode req) {
        String trainCode = req.get("train_code").asText();
        String serviceDate = req.get("service_date").asText();

        // 1. Train
        Integer trainId = jdbc.queryForObject("WITH s AS (SELECT id FROM train WHERE code = ?), i AS (INSERT INTO train (code) VALUES (?) ON CONFLICT (code) DO NOTHING RETURNING id) SELECT id FROM i UNION ALL SELECT id FROM s LIMIT 1", Integer.class, trainCode, trainCode);
        
        // 2. Stations
        for (JsonNode st : req.get("stations")) {
            jdbc.update("INSERT INTO station (train_id, name, sequence) VALUES (?, ?, ?) ON CONFLICT (train_id, name) DO NOTHING", trainId, st.get("name").asText(), st.get("sequence").asInt());
        }

        // 3. Schedule
        Integer scheduleId = jdbc.queryForObject("WITH s AS (SELECT id FROM train_schedule WHERE train_id = ? AND service_date = ?::date), i AS (INSERT INTO train_schedule (train_id, service_date, status) VALUES (?, ?::date, 'ACTIVE') ON CONFLICT (train_id, service_date) DO NOTHING RETURNING id) SELECT id FROM i UNION ALL SELECT id FROM s LIMIT 1", Integer.class, trainId, serviceDate, trainId, serviceDate);

        // 4. Seats & Segments
        int maxSeq = req.get("stations").size();
        for (JsonNode seat : req.get("seats")) {
            Integer seatId = jdbc.queryForObject("WITH s AS (SELECT id FROM seat WHERE schedule_id = ? AND carriage_no = ? AND seat_no = ?), i AS (INSERT INTO seat (schedule_id, carriage_no, seat_no, seat_class, is_long_distance_pool, quota_released) VALUES (?, ?, ?, ?, 0, 0) ON CONFLICT (schedule_id, carriage_no, seat_no) DO NOTHING RETURNING id) SELECT id FROM i UNION ALL SELECT id FROM s LIMIT 1", Integer.class, scheduleId, seat.get("carriage_no").asText(), seat.get("seat_no").asText(), scheduleId, seat.get("carriage_no").asText(), seat.get("seat_no").asText(), seat.get("seat_class").asText());
            for (int i = 1; i < maxSeq; i++) {
                jdbc.update("INSERT INTO seat_segment (schedule_id, seat_id, segment_no, state, version) VALUES (?, ?, ?, 'AVAILABLE', 0) ON CONFLICT (schedule_id, seat_id, segment_no) DO NOTHING", scheduleId, seatId, i);
            }
        }

        // 5. Outbox Event
        ObjectNode payload = mapper.createObjectNode();
        payload.put("schedule_id", scheduleId);
        jdbc.update("INSERT INTO outbox_event (event_id, aggregate_type, aggregate_id, event_type, payload, status) VALUES (?, 'TrainSchedule', ?, 'SCHEDULE_IMPORTED', ?::jsonb, 'NEW')",
            UUID.randomUUID().toString(), String.valueOf(scheduleId), payload.toString());

        return scheduleId;
    }
}

// ============================================================================
// Background Tasks (Outbox Publisher & Projector)
// ============================================================================
@Service
class BackgroundWorkers {

    private final JdbcTemplate jdbc;
    private final BlockingQueue<String> outboxChannel;
    private final TicketingService ticketingService;
    private final ObjectMapper mapper = new ObjectMapper();

    public BackgroundWorkers(JdbcTemplate jdbc, BlockingQueue<String> outboxChannel, TicketingService ticketingService) {
        this.jdbc = jdbc;
        this.outboxChannel = outboxChannel;
        this.ticketingService = ticketingService;
    }

    @Scheduled(fixedDelay = 100)
    @Transactional
    public void publishOutboxEvents() {
        List<Map<String, Object>> events = jdbc.queryForList("SELECT id, payload::text, event_type FROM outbox_event WHERE status = 'NEW' ORDER BY id ASC LIMIT 100 FOR UPDATE SKIP LOCKED");
        for (Map<String, Object> ev : events) {
            jdbc.update("UPDATE outbox_event SET status = 'PROCESSED', published_at = NOW() WHERE id = ?", ev.get("id"));
            outboxChannel.offer((String) ev.get("payload"));
        }
    }

    @Scheduled(fixedDelay = 50)
    public void projectEvents() {
        String payloadStr;
        while ((payloadStr = outboxChannel.poll()) != null) {
            try {
                JsonNode payload = mapper.readTree(payloadStr);
                if (payload.has("schedule_id")) {
                    ticketingService.recalculateAndProject(payload.get("schedule_id").asInt(), "BUSINESS");
                }
            } catch (Exception e) {
                // log error
            }
        }
    }
}
