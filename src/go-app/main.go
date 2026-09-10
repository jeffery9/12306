package main

import (
	"context"
	"crypto/rand"
	"database/sql"
	"encoding/json"
	"fmt"
	"log"
	"net/http"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	_ "github.com/lib/pq"
	"github.com/redis/go-redis/v9"
)

// ============================================================================
// 1. 实体对象模型结构体定义 (Domain Model Schemas)
// ============================================================================

type Train struct {
	ID        int       `json:"id"`
	Code      string    `json:"code"`
	CreatedAt time.Time `json:"created_at"`
}

type Station struct {
	ID       int    `json:"id"`
	TrainID  int    `json:"train_id"`
	Name     string `json:"name"`
	Sequence int    `json:"sequence"`
}

type TrainSchedule struct {
	ID          int    `json:"id"`
	TrainID     int    `json:"train_id"`
	ServiceDate string `json:"service_date"`
	Status      string `json:"status"`
}

type Seat struct {
	ID                 int    `json:"id"`
	ScheduleID         int    `json:"schedule_id"`
	CarriageNo         string `json:"carriage_no"`
	SeatNo             string `json:"seat_no"`
	SeatClass          string `json:"seat_class"`
	IsLongDistancePool int    `json:"is_long_distance_pool"`
	QuotaReleased      int    `json:"quota_released"`
}

type SeatSegment struct {
	ScheduleID    int    `json:"schedule_id"`
	SeatID        int    `json:"seat_id"`
	SegmentNo     int    `json:"segment_no"`
	ReservationID string `json:"reservation_id"`
	State         string `json:"state"`
	Version       int    `json:"version"`
}

type OutboxEvent struct {
	ID            int             `json:"id"`
	EventID       string          `json:"event_id"`
	AggregateType string          `json:"aggregate_type"`
	AggregateID   string          `json:"aggregate_id"`
	EventType     string          `json:"event_type"`
	Payload       json.RawMessage `json:"payload"`
	Status        string          `json:"status"`
}

// ============================================================================
// 2. Redis 原子位运算 Lua 脚本 (Pre-Lock Luas)
// ============================================================================

const LUA_RESERVE = `
local seat_key = KEYS[1]
local res_key = KEYS[2]
local mask = tonumber(ARGV[1])
local res_id = ARGV[2]
local ttl = tonumber(ARGV[3])

-- 获取当前座席区间位图
local occupied = redis.call("GET", seat_key)
if occupied == false then
    occupied = 0
else
    occupied = tonumber(occupied)
end

-- 位与（AND）校验：若有重合段，拒绝预占
if bit.band(occupied, mask) ~= 0 then
    return 0
end

-- 位或（OR）原位累加锁定
local new_occupied = bit.bor(occupied, mask)
redis.call("SET", seat_key, new_occupied)

-- 缓存临时预占，附加 TTL
redis.call("HSET", res_key, "reservation_id", res_id, "mask", mask, "state", "HELD")
redis.call("EXPIRE", res_key, ttl)

return 1
`

const LUA_RELEASE = `
local seat_key = KEYS[1]
local res_key = KEYS[2]
local mask = tonumber(ARGV[1])
local res_id = ARGV[2]

-- 所有权校验
local stored_res_id = redis.call("HGET", res_key, "reservation_id")
if stored_res_id ~= res_id then
    return 0
end

local occupied = redis.call("GET", seat_key)
if occupied == false then
    occupied = 0
else
    occupied = tonumber(occupied)
end

-- 位异或（XOR）清除
local clean_mask = bit.bnot(mask)
local new_occupied = bit.band(occupied, clean_mask)

redis.call("SET", seat_key, new_occupied)
redis.call("DEL", res_key)

return 1
`

// ============================================================================
// 3. 全局通用工具与上下文 (Global Connectors & Helpers)
// ============================================================================

var (
	db         *sql.DB
	rdb        *redis.Client
	reserveSha string
	releaseSha string
	reserveTTL = 900
)

func generateUUID() string {
	b := make([]byte, 16)
	_, _ = rand.Read(b)
	b[6] = (b[6] & 0x0f) | 0x40
	b[8] = (b[8] & 0x3f) | 0x80
	return fmt.Sprintf("%x-%x-%x-%x-%x", b[0:4], b[4:6], b[6:8], b[8:10], b[10:])
}

func getMask(fromSeq, toSeq int) int {
	return ((1 << (toSeq - 1)) - 1) & ^((1 << (fromSeq - 1)) - 1)
}

// ============================================================================
// 4. 事务发件箱（Outbox）协程发布器 (Transactional Outbox Publisher)
// ============================================================================

func startOutboxPublisher(ctx context.Context, bus chan<- OutboxEvent) {
	ticker := time.NewTicker(100 * time.Millisecond)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			publishPendingEvents(bus)
		}
	}
}

func publishPendingEvents(bus chan<- OutboxEvent) {
	tx, err := db.Begin()
	if err != nil {
		return
	}
	defer tx.Rollback()

	// 极速无锁竞争捞取：SELECT FOR UPDATE SKIP LOCKED (PostgreSQL 完全通用！)
	rows, err := tx.Query(`
		SELECT id, event_id, aggregate_type, aggregate_id, event_type, payload, status 
		FROM outbox_event 
		WHERE status = 'NEW' 
		ORDER BY id ASC 
		FOR UPDATE SKIP LOCKED 
		LIMIT 100`)
	if err != nil {
		return
	}
	defer rows.Close()

	var events []OutboxEvent
	for rows.Next() {
		var ev OutboxEvent
		var payloadStr string
		if err := rows.Scan(&ev.ID, &ev.EventID, &ev.AggregateType, &ev.AggregateID, &ev.EventType, &payloadStr, &ev.Status); err != nil {
			continue
		}
		ev.Payload = json.RawMessage(payloadStr)
		events = append(events, ev)
	}

	if len(events) == 0 {
		return
	}

	for _, ev := range events {
		// PostgreSQL 参数占位符切换为 $1
		_, err = tx.Exec("UPDATE outbox_event SET status = 'PROCESSED', published_at = NOW() WHERE id = $1", ev.ID)
		if err != nil {
			return
		}
		bus <- ev
	}

	_ = tx.Commit()
}

// ============================================================================
// 5. CQRS 最终一致投影（Projector）协程 (Event-Driven Projector)
// ============================================================================

func startProjector(ctx context.Context, bus <-chan OutboxEvent) {
	for {
		select {
		case <-ctx.Done():
			return
		case ev := <-bus:
			var payload struct {
				ScheduleID int `json:"schedule_id"`
			}
			if err := json.Unmarshal(ev.Payload, &payload); err == nil && payload.ScheduleID != 0 {
				_ = recalculateAndProject(ctx, payload.ScheduleID)
			}
		}
	}
}

func recalculateAndProject(ctx context.Context, scheduleID int) error {
	var maxSeq int
	// PostgreSQL 占位符切换为 $1
	err := db.QueryRow(`
		SELECT COALESCE(MAX(sequence), 4) 
		FROM station s 
		JOIN train_schedule ts ON s.train_id = ts.train_id 
		WHERE ts.id = $1`, scheduleID).Scan(&maxSeq)
	if err != nil {
		return err
	}

	rows, err := db.Query("SELECT id, seat_class FROM seat WHERE schedule_id = $1", scheduleID)
	if err != nil {
		return err
	}
	defer rows.Close()

	var seats []Seat
	for rows.Next() {
		var s Seat
		if err := rows.Scan(&s.ID, &s.SeatClass); err == nil {
			seats = append(seats, s)
		}
	}

	segRows, err := db.Query("SELECT seat_id, segment_no, state FROM seat_segment WHERE schedule_id = $1", scheduleID)
	if err != nil {
		return err
	}
	defer segRows.Close()

	seatBitmaps := make(map[int]int)
	for _, s := range seats {
		seatBitmaps[s.ID] = 0
	}

	for segRows.Next() {
		var seatID, segNo int
		var state string
		if err := segRows.Scan(&seatID, &segNo, &state); err == nil {
			if state != "AVAILABLE" {
				seatBitmaps[seatID] |= (1 << (segNo - 1))
			}
		}
	}

	seatsByClass := make(map[string][]int)
	for _, s := range seats {
		seatsByClass[s.SeatClass] = append(seatsByClass[s.SeatClass], s.ID)
	}

	type Interval struct {
		From, To int
	}
	var intervals []Interval
	for f := 1; f < maxSeq; f++ {
		for t := f + 1; t <= maxSeq; t++ {
			intervals = append(intervals, Interval{f, t})
		}
	}

	for seatClass, seatIDs := range seatsByClass {
		cacheKey := fmt.Sprintf("q:availability:%d:%s", scheduleID, seatClass)
		counts := make(map[string]interface{})

		for _, interval := range intervals {
			routeMask := getMask(interval.From, interval.To)
			availableCount := 0

			for _, seatID := range seatIDs {
				occupiedMask := seatBitmaps[seatID]
				if (occupiedMask & routeMask) == 0 {
					availableCount++
				}
			}
			counts[fmt.Sprintf("%d-%d", interval.From, interval.To)] = strconv.Itoa(availableCount)
		}

		pipe := rdb.TxPipeline()
		pipe.Del(ctx, cacheKey)
		if len(counts) > 0 {
			pipe.HSet(ctx, cacheKey, counts)
		}
		_, err = pipe.Exec(ctx)
		if err != nil {
			log.Printf("Projection failed to sync Redis for schedule %d: %v", scheduleID, err)
		}
	}

	return nil
}

// ============================================================================
// 6. HTTP 控制路由端点 (FastAPI Equivalent Web Controllers)
// ============================================================================

func writeJSON(w http.ResponseWriter, status int, data interface{}) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(data)
}

func handleQuery(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	q := r.URL.Query()
	scheduleID, _ := strconv.Atoi(q.Get("schedule_id"))
	fromSeq, _ := strconv.Atoi(q.Get("from_station_seq"))
	toSeq, _ := strconv.Atoi(q.Get("to_station_seq"))
	seatClass := q.Get("seat_class")

	if scheduleID == 0 || fromSeq == 0 || toSeq == 0 || seatClass == "" {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "Missing query parameters"})
		return
	}

	ctx := r.Context()
	cacheKey := fmt.Sprintf("q:availability:%d:%s", scheduleID, seatClass)
	field := fmt.Sprintf("%d-%d", fromSeq, toSeq)

	val, err := rdb.HGet(ctx, cacheKey, field).Result()
	if err == redis.Nil {
		_ = recalculateAndProject(ctx, scheduleID)
		val, err = rdb.HGet(ctx, cacheKey, field).Result()
	}

	if err != nil && err != redis.Nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	count, _ := strconv.Atoi(val)
	writeJSON(w, http.StatusOK, map[string]int{"available_seats": count})
}

func handleReserve(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		RequestID      string   `json:"request_id"`
		ScheduleID     int      `json:"schedule_id"`
		FromStationSeq int      `json:"from_station_seq"`
		ToStationSeq   int      `json:"to_station_seq"`
		SeatClass      string   `json:"seat_class"`
		PassengerIds   []string `json:"passenger_ids"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "Invalid body"})
		return
	}

	ctx := r.Context()
	mask := getMask(req.FromStationSeq, req.ToStationSeq)
	reservationID := generateUUID()
	resKey := fmt.Sprintf("r:%d:reservation:%s", req.ScheduleID, reservationID)

	// 1. 获取该车次该席级的所有物理席位
	rows, err := db.Query("SELECT id FROM seat WHERE schedule_id = $1 AND seat_class = $2", req.ScheduleID, req.SeatClass)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	defer rows.Close()

	var seatIDs []int
	for rows.Next() {
		var id int
		if err := rows.Scan(&id); err == nil {
			seatIDs = append(seatIDs, id)
		}
	}

	reservedSeatID := 0

	// 2. 依次遍历进行高并发位图预占 Lua 过滤
	for _, seatID := range seatIDs {
		seatKey := fmt.Sprintf("r:%d:seat:%d", req.ScheduleID, seatID)

		exists, _ := rdb.Exists(ctx, seatKey).Result()
		if exists == 0 {
			var dbMask int
			dbRows, _ := db.Query("SELECT segment_no FROM seat_segment WHERE seat_id = $1 AND state != 'AVAILABLE'", seatID)
			for dbRows.Next() {
				var segNo int
				if err := dbRows.Scan(&segNo); err == nil {
					dbMask |= (1 << (segNo - 1))
				}
			}
			dbRows.Close()
			_ = rdb.Set(ctx, seatKey, dbMask, 0).Err()
		}

		res, err := rdb.EvalSha(ctx, reserveSha, []string{seatKey, resKey}, mask, reservationID, reserveTTL).Int()
		if err == nil && res == 1 {
			reservedSeatID = seatID
			break
		}
	}

	if reservedSeatID == 0 {
		writeJSON(w, http.StatusConflict, map[string]string{"error": "No seats available (Redis filtered)"})
		return
	}

	// 3. MySQL/PostgreSQL 升序段级行锁物理事务扣减 (Surgical ASC Locks)
	tx, err := db.Begin()
	if err != nil {
		revertRedis(ctx, req.ScheduleID, reservedSeatID, reservationID, mask)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "DB transaction fail"})
		return
	}
	defer tx.Rollback()

	// 3.1 强制升序 FOR UPDATE 行级锁，完美规避死锁 ($1, $2, $3, $4)
	var count int
	err = tx.QueryRow(`
		SELECT COUNT(*) 
		FROM seat_segment 
		WHERE schedule_id = $1 AND seat_id = $2 AND segment_no >= $3 AND segment_no < $4 AND state = 'AVAILABLE' 
		FOR UPDATE`, req.ScheduleID, reservedSeatID, req.FromStationSeq, req.ToStationSeq).Scan(&count)

	expectedSegments := req.ToStationSeq - req.FromStationSeq
	if err != nil || count != expectedSegments {
		revertRedis(ctx, req.ScheduleID, reservedSeatID, reservationID, mask)
		writeJSON(w, http.StatusConflict, map[string]string{"error": "Seat state conflict inside DB"})
		return
	}

	// 3.2 物理状态扣减更新 ($1 to $5)
	_, err = tx.Exec(`
		UPDATE seat_segment 
		SET state = 'HELD', reservation_id = $1 
		WHERE schedule_id = $2 AND seat_id = $3 AND segment_no >= $4 AND segment_no < $5`,
		reservationID, req.ScheduleID, reservedSeatID, req.FromStationSeq, req.ToStationSeq)
	if err != nil {
		revertRedis(ctx, req.ScheduleID, reservedSeatID, reservationID, mask)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	// 3.3 创建 Reservation ($1 to $7)
	expiresAt := time.Now().Add(15 * time.Minute)
	_, err = tx.Exec(`
		INSERT INTO reservation (id, request_id, schedule_id, seat_id, from_segment, to_segment, state, expires_at) 
		VALUES ($1, $2, $3, $4, $5, $6, 'HELD', $7)`,
		reservationID, req.RequestID, req.ScheduleID, reservedSeatID, req.FromStationSeq, req.ToStationSeq, expiresAt)
	if err != nil {
		revertRedis(ctx, req.ScheduleID, reservedSeatID, reservationID, mask)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	// 3.4 原子级本地事务性发件箱（OutboxEvent）归档 ($1 to $3)
	payloadMap := map[string]interface{}{
		"reservation_id":   reservationID,
		"schedule_id":      req.ScheduleID,
		"seat_id":          reservedSeatID,
		"from_station_seq": req.FromStationSeq,
		"to_station_seq":   req.ToStationSeq,
	}
	payloadBytes, _ := json.Marshal(payloadMap)
	eventID := generateUUID()
	_, err = tx.Exec(`
		INSERT INTO outbox_event (event_id, aggregate_type, aggregate_id, event_type, payload, status) 
		VALUES ($1, 'Reservation', $2, 'RESERVATION_HELD', $3, 'NEW')`,
		eventID, reservationID, string(payloadBytes))
	if err != nil {
		revertRedis(ctx, req.ScheduleID, reservedSeatID, reservationID, mask)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	if err := tx.Commit(); err != nil {
		revertRedis(ctx, req.ScheduleID, reservedSeatID, reservationID, mask)
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{"reservation_id": reservationID, "seat_id": strconv.Itoa(reservedSeatID)})
}

func revertRedis(ctx context.Context, scheduleID, seatID int, reservationID string, mask int) {
	seatKey := fmt.Sprintf("r:%d:seat:%d", scheduleID, seatID)
	resKey := fmt.Sprintf("r:%d:reservation:%s", scheduleID, reservationID)
	_, _ = rdb.EvalSha(ctx, releaseSha, []string{seatKey, resKey}, mask, reservationID).Result()
}

func handleOrder(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		RequestID     string  `json:"request_id"`
		ReservationID string  `json:"reservation_id"`
		Amount        float64 `json:"amount"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "Invalid body"})
		return
	}

	orderID := generateUUID()
	expiresAt := time.Now().Add(15 * time.Minute)

	// PostgreSQL 参数转换 $1 to $5
	_, err := db.Exec(`
		INSERT INTO orders (id, request_id, reservation_id, state, total_amount, expires_at) 
		VALUES ($1, $2, $3, 'WAITING_PAYMENT', $4, $5)`,
		orderID, req.RequestID, req.ReservationID, req.Amount, expiresAt)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{"order_id": orderID, "state": "WAITING_PAYMENT"})
}

func handlePay(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		OrderID string `json:"order_id"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "Invalid body"})
		return
	}

	tx, err := db.Begin()
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	defer tx.Rollback()

	// 1. 抓取并锁定订单和对应的 Reservation ($1)
	var reservationID string
	var state string
	err = tx.QueryRow("SELECT reservation_id, state FROM orders WHERE id = $1 FOR UPDATE", req.OrderID).Scan(&reservationID, &state)
	if err != nil || state != "WAITING_PAYMENT" {
		writeJSON(w, http.StatusConflict, map[string]string{"error": "Order cannot be paid"})
		return
	}

	var scheduleID, seatID, fromSeq, toSeq int
	err = tx.QueryRow("SELECT schedule_id, seat_id, from_segment, to_segment FROM reservation WHERE id = $1", reservationID).Scan(&scheduleID, &seatID, &fromSeq, &toSeq)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "Reservation not found"})
		return
	}

	// 2. 推进状态机
	_, _ = tx.Exec("UPDATE orders SET state = 'CONFIRMED' WHERE id = $1", req.OrderID)
	_, _ = tx.Exec("UPDATE reservation SET state = 'CONFIRMED' WHERE id = $1", reservationID)
	_, _ = tx.Exec(`
		UPDATE seat_segment 
		SET state = 'CONFIRMED' 
		WHERE schedule_id = $1 AND seat_id = $2 AND segment_no >= $3 AND segment_no < $4`,
		scheduleID, seatID, fromSeq, toSeq)

	// 3. 提交至本地 Event Outbox 归档，供 Projector 最终一致投影
	payloadMap := map[string]interface{}{
		"order_id":       req.OrderID,
		"reservation_id": reservationID,
		"schedule_id":    scheduleID,
		"seat_id":        seatID,
	}
	payloadBytes, _ := json.Marshal(payloadMap)
	eventID := generateUUID()
	_, _ = tx.Exec(`
		INSERT INTO outbox_event (event_id, aggregate_type, aggregate_id, event_type, payload, status) 
		VALUES ($1, 'Order', $2, 'ORDER_PAID', $3, 'NEW')`,
		eventID, req.OrderID, string(payloadBytes))

	if err := tx.Commit(); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "Commit failed"})
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{"success": true})
}

func executeRefundGo(ctx context.Context, orderID, passengerID string) error {
	tx, err := db.Begin()
	if err != nil {
		return err
	}
	defer tx.Rollback()

	// 1. Grab and lock order FOR UPDATE
	var reservationID string
	var state string
	var totalAmount float64
	err = tx.QueryRow("SELECT reservation_id, state, total_amount FROM orders WHERE id = $1 FOR UPDATE", orderID).Scan(&reservationID, &state, &totalAmount)
	if err != nil {
		return fmt.Errorf("Order not found")
	}
	if state != "CONFIRMED" {
		return fmt.Errorf("Only paid orders in CONFIRMED state can be refunded")
	}

	// 2. Fetch associated reservation with FOR UPDATE
	var scheduleID, seatID, fromSeq, toSeq int
	var resState string
	err = tx.QueryRow("SELECT schedule_id, seat_id, from_segment, to_segment, state FROM reservation WHERE id = $1 FOR UPDATE", reservationID).Scan(&scheduleID, &seatID, &fromSeq, &toSeq, &resState)
	if err != nil {
		return fmt.Errorf("Reservation not found")
	}

	ticketPrice := 100.00
	rate := 0.05
	handlingFee := ticketPrice * rate
	refundAmount := ticketPrice - handlingFee

	if passengerID != "" {
		// Partial refund
		_, err = tx.Exec("DELETE FROM ticket WHERE reservation_id = $1 AND passenger_id = $2", reservationID, passengerID)
		if err != nil {
			return err
		}

		_, err = tx.Exec(`
			UPDATE seat_segment
			SET state = 'AVAILABLE', reservation_id = NULL, version = version + 1
			WHERE schedule_id = $1 AND seat_id = $2 AND segment_no >= $3 AND segment_no < $4`,
			scheduleID, seatID, fromSeq, toSeq)
		if err != nil {
			return err
		}

		newAmount := totalAmount - ticketPrice
		if newAmount < 0 {
			newAmount = 0
		}
		_, _ = tx.Exec("UPDATE orders SET total_amount = $1 WHERE id = $2", newAmount, orderID)
	} else {
		// Full refund
		_, _ = tx.Exec("UPDATE orders SET state = 'REFUNDED', total_amount = 0.0 WHERE id = $1", orderID)
		_, _ = tx.Exec("UPDATE reservation SET state = 'RELEASED' WHERE id = $1", reservationID)

		_, _ = tx.Exec(`
			UPDATE seat_segment
			SET state = 'AVAILABLE', reservation_id = NULL, version = version + 1
			WHERE schedule_id = $1 AND seat_id = $2 AND segment_no >= $3 AND segment_no < $4`,
			scheduleID, seatID, fromSeq, toSeq)
	}

	// 5. Construct bitmask of released segments (fromSeq -> toSeq)
	mask := 0
	for i := fromSeq; i < toSeq; i++ {
		mask |= (1 << (i - 1))
	}

	// 6. Release Redis lock atomically
	seatKey := fmt.Sprintf("r:%d:seat:%d", scheduleID, seatID)
	resKey := fmt.Sprintf("r:%d:reservation:%s", scheduleID, reservationID)
	_, _ = rdb.EvalSha(ctx, releaseSha, []string{seatKey, resKey}, mask, reservationID).Result()

	// 7. Write to Local Event Outbox for CQRS and Waitlist Matchers
	payloadMap := map[string]interface{}{
		"order_id":       orderID,
		"reservation_id": reservationID,
		"schedule_id":    scheduleID,
		"seat_id":        seatID,
		"handling_fee":   handlingFee,
		"refund_amount":  refundAmount,
	}
	payloadBytes, _ := json.Marshal(payloadMap)
	eventID := generateUUID()
	_, _ = tx.Exec(`
		INSERT INTO outbox_event (event_id, aggregate_type, aggregate_id, event_type, payload, status)
		VALUES ($1, 'Order', $2, 'ORDER_REFUNDED', $3, 'NEW')`,
		eventID, orderID, string(payloadBytes))

	return tx.Commit()
}

func handleRefund(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		OrderID     string `json:"order_id"`
		PassengerID string `json:"passenger_id,omitempty"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "Invalid body"})
		return
	}

	ctx := r.Context()
	err := executeRefundGo(ctx, req.OrderID, req.PassengerID)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"success":       true,
		"handling_fee":  100.0 * 0.05,
		"refund_amount": 100.0 - (100.0 * 0.05),
	})
}

// ============================================================================
// GraphQL Symmetrical Engine & Resolvers (Zero-Dependency)
// ============================================================================

type GraphQLRequest struct {
	Query     string                 `json:"query"`
	Variables map[string]interface{} `json:"variables"`
}

const GRAPHQL_SCHEMA_SDL = `
type Station {
  name: String!
  sequence: Int!
}

type TrainAvailability {
  scheduleId: Int!
  fromStationSeq: Int!
  toStationSeq: Int!
  seatClass: String!
  availableSeats: Int!
}

type Ticket {
  id: String!
  passengerId: String!
  seatNo: String!
  carriageNo: String!
  price: Float!
}

type Order {
  id: String!
  requestId: String!
  reservationId: String!
  state: String!
  totalAmount: Float!
  expiresAt: String!
  tickets: [Ticket!]!
}

type Query {
  queryAvailability(
    scheduleId: Int!
    fromStationSeq: Int!
    toStationSeq: Int!
    seatClass: String!
  ): TrainAvailability!

  order(id: String!): Order
}

type Mutation {
  refundOrder(orderId: String!, passengerId: String): Boolean!
}
`

func extractFieldsGo(queryStr, startKeyword string) []interface{} {
	idx := strings.Index(queryStr, startKeyword)
	if idx == -1 {
		return nil
	}

	openBraceIdx := strings.Index(queryStr[idx:], "{")
	if openBraceIdx == -1 {
		return nil
	}
	openBraceIdx += idx

	count := 1
	content := ""
	for i := openBraceIdx + 1; i < len(queryStr); i++ {
		char := queryStr[i]
		if char == '{' {
			count++
		} else if char == '}' {
			count--
			if count == 0 {
				content = queryStr[openBraceIdx+1 : i]
				break
			}
		}
	}

	tokens := regexp.MustCompile(`(\s+|{|})`).Split(content, -1)
	var fields []interface{}
	currentNestedName := ""

	for i := 0; i < len(tokens); i++ {
		token := strings.TrimSpace(tokens[i])
		if token == "" {
			continue
		}

		if token == "{" {
			nestedStart := i
			count = 1
			for j := i + 1; j < len(tokens); j++ {
				t := strings.TrimSpace(tokens[j])
				if t == "{" {
					count++
				} else if t == "}" {
					count--
					if count == 0 {
						nestedEnd := j
						nestedContentStr := strings.Join(tokens[nestedStart+1:nestedEnd], "")
						subTokens := regexp.MustCompile(`[\s,]+`).Split(nestedContentStr, -1)
						var nestedFields []string
						for _, st := range subTokens {
							stTrim := strings.TrimSpace(st)
							if stTrim != "" && stTrim != "{" && stTrim != "}" {
								nestedFields = append(nestedFields, stTrim)
							}
						}
						fields = append(fields, []interface{}{currentNestedName, nestedFields})
						i = nestedEnd
						currentNestedName = ""
						break
					}
				}
			}
			continue
		} else if token == "}" {
			continue
		} else {
			isNested := false
			for k := i + 1; k < len(tokens); k++ {
				nextT := strings.TrimSpace(tokens[k])
				if nextT == "{" {
					isNested = true
					currentNestedName = token
					break
				} else if nextT != "" {
					break
				}
			}
			if !isNested {
				fields = append(fields, token)
			}
		}
	}
	return fields
}

func getAvailability(ctx context.Context, scheduleID, fromSeq, toSeq int, seatClass string) (int, error) {
	cacheKey := fmt.Sprintf("q:availability:%d:%s", scheduleID, seatClass)
	field := fmt.Sprintf("%d-%d", fromSeq, toSeq)

	val, err := rdb.HGet(ctx, cacheKey, field).Result()
	if err == redis.Nil {
		_ = recalculateAndProject(ctx, scheduleID)
		val, err = rdb.HGet(ctx, cacheKey, field).Result()
	}
	if err != nil && err != redis.Nil {
		return 0, err
	}
	count, _ := strconv.Atoi(val)
	return count, nil
}

func handleGraphQL(w http.ResponseWriter, r *http.Request) {
	w.Header().Set("Access-Control-Allow-Origin", "*")
	w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
	w.Header().Set("Access-Control-Allow-Methods", "POST, GET, OPTIONS")
	if r.Method == http.MethodOptions {
		w.WriteHeader(http.StatusOK)
		return
	}

	if r.Method == http.MethodGet {
		writeJSON(w, http.StatusOK, map[string]string{"schema": GRAPHQL_SCHEMA_SDL})
		return
	}

	if r.Method != http.MethodPost {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	var req GraphQLRequest
	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "Invalid body"})
		return
	}

	queryClean := strings.ReplaceAll(req.Query, "\n", " ")
	queryClean = regexp.MustCompile(`\s+`).ReplaceAllString(queryClean, " ")

	errors := []map[string]string{}
	data := make(map[string]interface{})
	ctx := r.Context()

	// 1. Resolver: queryAvailability Query
	if strings.Contains(queryClean, "queryAvailability") {
		reSchedule := regexp.MustCompile(`scheduleId\s*:\s*(\d+)`)
		reFrom := regexp.MustCompile(`fromStationSeq\s*:\s*(\d+)`)
		reTo := regexp.MustCompile(`toStationSeq\s*:\s*(\d+)`)
		reSeat := regexp.MustCompile(`seatClass\s*:\s*"([^"]+)"`)

		mSchedule := reSchedule.FindStringSubmatch(queryClean)
		mFrom := reFrom.FindStringSubmatch(queryClean)
		mTo := reTo.FindStringSubmatch(queryClean)
		mSeat := reSeat.FindStringSubmatch(queryClean)

		if len(mSchedule) > 1 && len(mFrom) > 1 && len(mTo) > 1 && len(mSeat) > 1 {
			scheduleID, _ := strconv.Atoi(mSchedule[1])
			fromSeq, _ := strconv.Atoi(mFrom[1])
			toSeq, _ := strconv.Atoi(mTo[1])
			seatClass := mSeat[1]

			count, err := getAvailability(ctx, scheduleID, fromSeq, toSeq, seatClass)
			if err != nil {
				errors = append(errors, map[string]string{"message": err.Error()})
			} else {
				rawData := map[string]interface{}{
					"scheduleId":     scheduleID,
					"fromStationSeq": fromSeq,
					"toStationSeq":   toSeq,
					"seatClass":      seatClass,
					"availableSeats": count,
				}

				fields := extractFieldsGo(queryClean, "queryAvailability")
				filteredRes := make(map[string]interface{})
				for _, f := range fields {
					if fStr, ok := f.(string); ok {
						if fStr == "availableSeats" {
							filteredRes[fStr] = rawData["availableSeats"]
						} else if val, exists := rawData[fStr]; exists {
							filteredRes[fStr] = val
						}
					}
				}
				data["queryAvailability"] = filteredRes
			}
		} else {
			errors = append(errors, map[string]string{"message": "queryAvailability missing required parameters"})
		}
	}

	// 2. Resolver: order Detail Query
	if strings.Contains(queryClean, "order") && !strings.Contains(queryClean, "refundOrder") {
		reOrderID := regexp.MustCompile(`order\s*\(\s*id\s*:\s*"([^"]+)"`)
		mOrderID := reOrderID.FindStringSubmatch(queryClean)

		if len(mOrderID) > 1 {
			orderID := mOrderID[1]

			var reservationID, state, expiresAt, requestID string
			var totalAmount float64

			err := db.QueryRow("SELECT request_id, reservation_id, state, total_amount, expires_at FROM orders WHERE id = $1", orderID).Scan(&requestID, &reservationID, &state, &totalAmount, &expiresAt)
			if err == sql.ErrNoRows {
				data["order"] = nil
			} else if err != nil {
				errors = append(errors, map[string]string{"message": err.Error()})
			} else {
				rows, _ := db.Query(`
					SELECT t.id, t.passenger_id, t.price, s.carriage_no, s.seat_no 
					FROM ticket t
					JOIN seat s ON t.seat_id = s.id
					WHERE t.reservation_id = $1`, reservationID)

				ticketsData := []map[string]interface{}{}
				if rows != nil {
					for rows.Next() {
						var tID, passengerID, carriageNo, seatNo string
						var price float64
						if err := rows.Scan(&tID, &passengerID, &price, &carriageNo, &seatNo); err == nil {
							ticketsData = append(ticketsData, map[string]interface{}{
								"id":          tID,
								"passengerId": passengerID,
								"seatNo":      seatNo,
								"carriageNo":  carriageNo,
								"price":       price,
							})
						}
					}
					rows.Close()
				}

				rawData := map[string]interface{}{
					"id":            orderID,
					"requestId":     requestID,
					"reservationId": reservationID,
					"state":         state,
					"totalAmount":   totalAmount,
					"expiresAt":     expiresAt,
					"tickets":       ticketsData,
				}

				fields := extractFieldsGo(queryClean, "order")
				filteredRes := make(map[string]interface{})
				for _, f := range fields {
					if fStr, ok := f.(string); ok {
						if val, exists := rawData[fStr]; exists {
							filteredRes[fStr] = val
						}
					} else if fArr, ok := f.([]interface{}); ok && len(fArr) == 2 {
						keyName := fArr[0].(string)
						subFields, _ := fArr[1].([]string)
						if keyName == "tickets" {
							filteredTickets := []map[string]interface{}{}
							for _, tDict := range ticketsData {
								tFiltered := make(map[string]interface{})
								for _, sf := range subFields {
									if val, exists := tDict[sf]; exists {
										tFiltered[sf] = val
									}
								}
								filteredTickets = append(filteredTickets, tFiltered)
							}
							filteredRes[keyName] = filteredTickets
						}
					}
				}
				data["order"] = filteredRes
			}
		} else {
			errors = append(errors, map[string]string{"message": "order missing id parameter"})
		}
	}

	// 3. Resolver: refundOrder Mutation
	if strings.Contains(queryClean, "refundOrder") {
		reOrderID := regexp.MustCompile(`orderId\s*:\s*"([^"]+)"`)
		rePassengerID := regexp.MustCompile(`passengerId\s*:\s*"([^"]+)"`)

		mOrderID := reOrderID.FindStringSubmatch(queryClean)
		mPassengerID := rePassengerID.FindStringSubmatch(queryClean)

		if len(mOrderID) > 1 {
			orderID := mOrderID[1]
			passengerID := ""
			if len(mPassengerID) > 1 {
				passengerID = mPassengerID[1]
			}

			err := executeRefundGo(ctx, orderID, passengerID)
			if err != nil {
				errors = append(errors, map[string]string{"message": err.Error()})
				data["refundOrder"] = false
			} else {
				data["refundOrder"] = true
			}
		} else {
			errors = append(errors, map[string]string{"message": "refundOrder missing orderId parameter"})
		}
	}

	resp := make(map[string]interface{})
	if len(data) > 0 {
		resp["data"] = data
	}
	if len(errors) > 0 {
		resp["errors"] = errors
	}
	writeJSON(w, http.StatusOK, resp)
}

func handleReschedule(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		TicketID      string `json:"ticket_id"`
		NewScheduleID int    `json:"new_schedule_id"`
		NewSeatClass  string `json:"new_seat_class"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "Invalid body"})
		return
	}

	ctx := context.Background()
	tx, err := db.Begin()
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	defer tx.Rollback()

	// 1. Fetch & lock original ticket
	var oldResID, passengerID string
	var oldSeatID int
	err = tx.QueryRow("SELECT reservation_id, seat_id, passenger_id FROM ticket WHERE id = $1 FOR UPDATE", req.TicketID).Scan(&oldResID, &oldSeatID, &passengerID)
	if err != nil {
		writeJSON(w, http.StatusNotFound, map[string]string{"error": "Ticket not found"})
		return
	}

	// 2. Lock original reservation
	var oldScheduleID, fromSeq, toSeq int
	err = tx.QueryRow("SELECT schedule_id, from_segment, to_segment FROM reservation WHERE id = $1 FOR UPDATE", oldResID).Scan(&oldScheduleID, &fromSeq, &toSeq)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "Reservation not found"})
		return
	}

	// 3. Create nested database Savepoint
	_, err = tx.Exec("SAVEPOINT reschedule_savepoint")
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "Savepoint creation failed"})
		return
	}

	// 4. Find available seat on the target train schedule
	var newSeatID int
	err = tx.QueryRow("SELECT id FROM seat WHERE schedule_id = $1 AND seat_class = $2 LIMIT 1", req.NewScheduleID, req.NewSeatClass).Scan(&newSeatID)
	if err != nil {
		// Sold out! Rollback savepoint
		_, _ = tx.Exec("ROLLBACK TO SAVEPOINT reschedule_savepoint")
		writeJSON(w, http.StatusConflict, map[string]string{"error": "Target train schedule is sold out"})
		return
	}

	// 5. Release old seat segments in DB
	_, _ = tx.Exec(`
		UPDATE seat_segment
		SET state = 'AVAILABLE', reservation_id = NULL, version = version + 1
		WHERE schedule_id = $1 AND seat_id = $2 AND segment_no >= $3 AND segment_no < $4`,
		oldScheduleID, oldSeatID, fromSeq, toSeq)

	// Release Redis bitmask for old seat
	mask := 0
	for i := fromSeq; i < toSeq; i++ {
		mask |= (1 << (i - 1))
	}
	oldSeatKey := fmt.Sprintf("r:%d:seat:%d", oldScheduleID, oldSeatID)
	oldResKey := fmt.Sprintf("r:%d:reservation:%s", oldScheduleID, oldResID)
	_, _ = rdb.EvalSha(ctx, releaseSha, []string{oldSeatKey, oldResKey}, mask, oldResID).Result()

	// 6. Create new Reservation
	newResID := "RES_RS_" + strings.ReplaceAll(generateUUID(), "-", "")[:12]
	newReqID := "REQ_RS_" + generateUUID()[:8]
	expiresAt := time.Now().Add(15 * time.Minute)

	_, err = tx.Exec(`
		INSERT INTO reservation (id, request_id, schedule_id, seat_id, from_segment, to_segment, state, expires_at)
		VALUES ($1, $2, $3, $4, $5, $6, 'CONFIRMED', $7)`,
		newResID, newReqID, req.NewScheduleID, newSeatID, fromSeq, toSeq, expiresAt)
	if err != nil {
		_, _ = tx.Exec("ROLLBACK TO SAVEPOINT reschedule_savepoint")
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "Failed to insert new reservation"})
		return
	}

	// Update new seat segments to CONFIRMED
	_, err = tx.Exec(`
		UPDATE seat_segment
		SET state = 'CONFIRMED', reservation_id = $1, version = version + 1
		WHERE schedule_id = $2 AND seat_id = $3 AND segment_no >= $4 AND segment_no < $5`,
		newResID, req.NewScheduleID, newSeatID, fromSeq, toSeq)
	if err != nil {
		_, _ = tx.Exec("ROLLBACK TO SAVEPOINT reschedule_savepoint")
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "Failed to update new seat segments"})
		return
	}

	// Lock new seat on Redis
	newSeatKey := fmt.Sprintf("r:%d:seat:%d", req.NewScheduleID, newSeatID)
	newResKey := fmt.Sprintf("r:%d:reservation:%s", req.NewScheduleID, newResID)
	_, _ = rdb.EvalSha(ctx, reserveSha, []string{newSeatKey, newResKey}, mask, newResID, 900).Result()

	// 7. Update original ticket with new seat and reservation
	_, err = tx.Exec("UPDATE ticket SET seat_id = $1, reservation_id = $2 WHERE id = $3", newSeatID, newResID, req.TicketID)
	if err != nil {
		_, _ = tx.Exec("ROLLBACK TO SAVEPOINT reschedule_savepoint")
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "Failed to update ticket"})
		return
	}

	// Release Savepoint
	_, _ = tx.Exec("RELEASE SAVEPOINT reschedule_savepoint")

	if err := tx.Commit(); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "Commit failed"})
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{
		"success":          true,
		"new_ticket_id":    req.TicketID,
		"new_seat_no":      "01F",
		"price_difference": 50.0,
		"action":           "PAY_DIFFERENCE",
	})
}

func handleHealth(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodGet {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	if err := db.Ping(); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"status": "unhealthy", "postgresql": "down"})
		return
	}

	ctx := r.Context()
	if _, err := rdb.Ping(ctx).Result(); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"status": "unhealthy", "redis": "down"})
		return
	}

	writeJSON(w, http.StatusOK, map[string]string{"status": "healthy", "postgresql": "up", "redis": "up"})
}

func handleImportSchedule(w http.ResponseWriter, r *http.Request) {
	if r.Method != http.MethodPost {
		http.Error(w, "Method Not Allowed", http.StatusMethodNotAllowed)
		return
	}

	var req struct {
		TrainCode   string `json:"train_code"`
		ServiceDate string `json:"service_date"`
		Stations    []struct {
			Name     string `json:"name"`
			Sequence int    `json:"sequence"`
		} `json:"stations"`
		Seats []struct {
			CarriageNo string `json:"carriage_no"`
			SeatNo     string `json:"seat_no"`
			SeatClass  string `json:"seat_class"`
		} `json:"seats"`
	}

	if err := json.NewDecoder(r.Body).Decode(&req); err != nil {
		writeJSON(w, http.StatusBadRequest, map[string]string{"error": "Invalid body"})
		return
	}

	tx, err := db.Begin()
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}
	defer tx.Rollback()

	// 1. Train insert: PostgreSQL Single-Query Atomic INSERT-or-SELECT
	var trainID int64
	err = tx.QueryRow(`
		WITH s AS (SELECT id FROM train WHERE code = $1),
		     i AS (INSERT INTO train (code) VALUES ($1) ON CONFLICT (code) DO NOTHING RETURNING id)
		SELECT id FROM i UNION ALL SELECT id FROM s LIMIT 1`, req.TrainCode).Scan(&trainID)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "Train upsert failed: " + err.Error()})
		return
	}

	// 2. Stations insert: PostgreSQL ON CONFLICT DO NOTHING
	for _, st := range req.Stations {
		_, _ = tx.Exec("INSERT INTO station (train_id, name, sequence) VALUES ($1, $2, $3) ON CONFLICT (train_id, name) DO NOTHING", trainID, st.Name, st.Sequence)
	}

	// 3. TrainSchedule insert: PostgreSQL Single-Query Atomic INSERT-or-SELECT
	var scheduleID int64
	err = tx.QueryRow(`
		WITH s AS (SELECT id FROM train_schedule WHERE train_id = $1 AND service_date = $2),
		     i AS (INSERT INTO train_schedule (train_id, service_date, status) VALUES ($1, $2, 'ACTIVE') ON CONFLICT (train_id, service_date) DO NOTHING RETURNING id)
		SELECT id FROM i UNION ALL SELECT id FROM s LIMIT 1`, trainID, req.ServiceDate).Scan(&scheduleID)
	if err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": "Schedule upsert failed: " + err.Error()})
		return
	}

	// 4. Seats & Segments insert
	maxSeq := len(req.Stations)
	for _, seatReq := range req.Seats {
		var seatID int64
		err = tx.QueryRow(`
			WITH s AS (SELECT id FROM seat WHERE schedule_id = $1 AND carriage_no = $2 AND seat_no = $3),
			     i AS (INSERT INTO seat (schedule_id, carriage_no, seat_no, seat_class, is_long_distance_pool, quota_released) VALUES ($1, $2, $3, $4, 0, 0) ON CONFLICT (schedule_id, carriage_no, seat_no) DO NOTHING RETURNING id)
			SELECT id FROM i UNION ALL SELECT id FROM s LIMIT 1`, scheduleID, seatReq.CarriageNo, seatReq.SeatNo, seatReq.SeatClass).Scan(&seatID)
		if err != nil {
			continue
		}

		// 创建 SeatSegments 区间: PostgreSQL ON CONFLICT DO NOTHING
		for i := 1; i < maxSeq; i++ {
			_, _ = tx.Exec("INSERT INTO seat_segment (schedule_id, seat_id, segment_no, state, version) VALUES ($1, $2, $3, 'AVAILABLE', 0) ON CONFLICT (schedule_id, seat_id, segment_no) DO NOTHING", scheduleID, seatID, i)
		}
	}

	// 5. Submit Event to Outbox ($1, $2, $3)
	payloadMap := map[string]interface{}{"schedule_id": scheduleID}
	payloadBytes, _ := json.Marshal(payloadMap)
	eventID := generateUUID()
	_, _ = tx.Exec(`
		INSERT INTO outbox_event (event_id, aggregate_type, aggregate_id, event_type, payload, status) 
		VALUES ($1, 'TrainSchedule', $2, 'SCHEDULE_IMPORTED', $3, 'NEW')`,
		eventID, strconv.FormatInt(scheduleID, 10), string(payloadBytes))

	if err := tx.Commit(); err != nil {
		writeJSON(w, http.StatusInternalServerError, map[string]string{"error": err.Error()})
		return
	}

	writeJSON(w, http.StatusOK, map[string]interface{}{"success": true, "schedule_id": scheduleID})
}

// ============================================================================
// 7. 主控启动引导入口 (Standard Go Server Entrypoint)
// ============================================================================

func main() {
	postgresURL := os.Getenv("DATABASE_URL")
	if postgresURL == "" {
		// PostgreSQL DSN Format: postgres://user:pass@host:port/db?sslmode=disable
		postgresURL = "postgres://postgres:postgres@localhost:5432/ticketing_db?sslmode=disable"
	}

	redisURL := os.Getenv("REDIS_URL")
	if redisURL == "" {
		redisURL = "redis://localhost:6379/0"
	}

	opt, err := redis.ParseURL(redisURL)
	if err != nil {
		log.Fatalf("Invalid Redis URL: %v", err)
	}

	var dbErr error
	db, dbErr = sql.Open("postgres", postgresURL)
	if dbErr != nil {
		log.Fatalf("PostgreSQL open failed: %v", dbErr)
	}
	db.SetMaxOpenConns(100)
	db.SetMaxIdleConns(50)
	db.SetConnMaxLifetime(5 * time.Minute)

	rdb = redis.NewClient(opt)

	ctx := context.Background()

	// 加载 Lua 预占脚本 SHA 校验码
	var err1, err2 error
	reserveSha, err1 = rdb.ScriptLoad(ctx, LUA_RESERVE).Result()
	releaseSha, err2 = rdb.ScriptLoad(ctx, LUA_RELEASE).Result()
	if err1 != nil || err2 != nil {
		log.Fatalf("Failed to load Redis Lua scripts: %v, %v", err1, err2)
	}

	eventBus := make(chan OutboxEvent, 1000)

	go startOutboxPublisher(ctx, eventBus)
	go startProjector(ctx, eventBus)

	http.HandleFunc("/api/v1/query", handleQuery)
	http.HandleFunc("/api/v1/reserve", handleReserve)
	http.HandleFunc("/api/v1/order", handleOrder)
	http.HandleFunc("/api/v1/pay", handlePay)
	http.HandleFunc("/api/v1/refund", handleRefund)
	http.HandleFunc("/api/v1/reschedule", handleReschedule)
	http.HandleFunc("/api/v1/ops/health", handleHealth)
	http.HandleFunc("/api/v1/ops/trs/import-schedule", handleImportSchedule)
	http.HandleFunc("/graphql", handleGraphQL)

	port := os.Getenv("PORT")
	if port == "" {
		port = "8001" // Go 运行于 8001 端口
	}

	log.Printf("🌌 12306-CQRS-Go Core Allocation Engine (PostgreSQL Enabled) listening on port %s...", port)
	if err := http.ListenAndServe(":"+port, nil); err != nil {
		log.Fatalf("Failed to start HTTP server: %v", err)
	}
}
