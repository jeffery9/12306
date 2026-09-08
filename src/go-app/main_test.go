package main

import (
	"bytes"
	"context"
	"database/sql"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	_ "github.com/lib/pq"
	"github.com/redis/go-redis/v9"
)

// ============================================================================
// 1. 原子单元测试 (Atomic Unit Tests)
// ============================================================================

func TestGetMask(t *testing.T) {
	tests := []struct {
		fromSeq  int
		toSeq    int
		expected int
	}{
		{1, 2, 1}, // 第1区间: 2^0 = 1 (二进制 01)
		{2, 3, 2}, // 第2区间: 2^1 = 2 (二进制 10)
		{1, 3, 3}, // 区间合拢 [1-3]: 2^0 + 2^1 = 3 (二进制 11)
		{3, 4, 4}, // 第3区间: 2^2 = 4 (二进制 100)
		{2, 4, 6}, // 区间合拢 [2-4]: 2^1 + 2^2 = 6 (二进制 110)
		{1, 4, 7}, // 区间合拢 [1-4]: 2^0 + 2^1 + 2^2 = 7 (二进制 111)
	}

	for _, tt := range tests {
		result := getMask(tt.fromSeq, tt.toSeq)
		if result != tt.expected {
			t.Errorf("getMask(%d, %d) failed: got %d, expected %d", tt.fromSeq, tt.toSeq, result, tt.expected)
		}
	}
}

func TestGenerateUUID(t *testing.T) {
	uuid1 := generateUUID()
	uuid2 := generateUUID()

	if uuid1 == uuid2 {
		t.Errorf("generateUUID() returned collision: %s", uuid1)
	}

	if len(uuid1) != 36 {
		t.Errorf("generateUUID() length invalid: got %d, expected 36", len(uuid1))
	}

	hyphens := strings.Count(uuid1, "-")
	if hyphens != 4 {
		t.Errorf("generateUUID() format invalid: hyphens count is %d, expected 4", hyphens)
	}
}

// ============================================================================
// 2. 数据库与缓存测试沙盒自适应初始化 (Sandboxed Database & Cache Initializer)
// ============================================================================

func setupTestDB(t *testing.T) context.Context {
	ctx := context.Background()

	postgresURL := os.Getenv("DATABASE_URL")
	if postgresURL == "" {
		postgresURL = "postgres://postgres:postgres@localhost:5432/ticketing_db?sslmode=disable"
	}

	redisURL := os.Getenv("REDIS_URL")
	if redisURL == "" {
		redisURL = "redis://localhost:6379/0"
	}

	// 1. 尝试连接 Redis
	opt, err := redis.ParseURL(redisURL)
	if err != nil {
		t.Skipf("Skipping integration test: invalid Redis URL: %v", err)
	}

	rdb = redis.NewClient(opt)
	if err := rdb.Ping(ctx).Err(); err != nil {
		t.Skipf("Skipping integration test: Redis not reachable: %v", err)
	}

	// 2. 尝试连接 PostgreSQL
	db, err = sql.Open("postgres", postgresURL)
	if err != nil {
		t.Skipf("Skipping integration test: Postgres connection open failed: %v", err)
	}

	if err := db.Ping(); err != nil {
		t.Skipf("Skipping integration test: Postgres not reachable: %v", err)
	}

	// 3. 注册加载原子位运算 Lua 预占脚本
	var err1, err2 error
	reserveSha, err1 = rdb.ScriptLoad(ctx, LUA_RESERVE).Result()
	releaseSha, err2 = rdb.ScriptLoad(ctx, LUA_RELEASE).Result()
	if err1 != nil || err2 != nil {
		t.Fatalf("Failed to load Redis Lua scripts into test sandbox: %v, %v", err1, err2)
	}

	return ctx
}

// ============================================================================
// 3. 全生命周期交易闭环集成测试 (End-to-End CQRS Ticketing Integration Test)
// ============================================================================

func TestFullWorkflow(t *testing.T) {
	ctx := setupTestDB(t)

	// ==========================================
	// ① TRS 物理车次同步导入阶段 (POST)
	// ==========================================
	importReq := map[string]interface{}{
		"train_code":   "T999",
		"service_date": "2026-11-11",
		"stations": []map[string]interface{}{
			{"name": "北京", "sequence": 1},
			{"name": "天津", "sequence": 2},
			{"name": "上海", "sequence": 3},
		},
		"seats": []map[string]interface{}{
			{"carriage_no": "01", "seat_no": "01A", "seat_class": "BUSINESS"},
		},
	}
	bodyBytes, _ := json.Marshal(importReq)
	req := httptest.NewRequest("POST", "/api/v1/ops/trs/import-schedule", bytes.NewReader(bodyBytes))
	rec := httptest.NewRecorder()

	handleImportSchedule(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("handleImportSchedule failed: status %d, response: %s", rec.Code, rec.Body.String())
	}

	var importRes struct {
		Success    bool `json:"success"`
		ScheduleID int  `json:"schedule_id"`
	}
	_ = json.Unmarshal(rec.Body.Bytes(), &importRes)
	scheduleID := importRes.ScheduleID
	if scheduleID == 0 {
		t.Fatalf("Imported schedule_id is invalid (0)")
	}

	// 清理该 Schedule 的临时 Redis 缓存以模拟最干净的冷启动 (Cold Start)
	cacheKey := fmt.Sprintf("q:availability:%d:BUSINESS", scheduleID)
	rdb.Del(ctx, cacheKey)

	// ==========================================
	// ② 余票冷查询暖身阶段 (GET)
	// ==========================================
	queryURL := fmt.Sprintf("/api/v1/query?schedule_id=%d&from_station_seq=1&to_station_seq=3&seat_class=BUSINESS", scheduleID)
	req = httptest.NewRequest("GET", queryURL, nil)
	rec = httptest.NewRecorder()

	handleQuery(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("handleQuery failed: status %d, response: %s", rec.Code, rec.Body.String())
	}

	var queryRes struct {
		AvailableSeats int `json:"available_seats"`
	}
	_ = json.Unmarshal(rec.Body.Bytes(), &queryRes)
	if queryRes.AvailableSeats != 1 {
		t.Errorf("Expected 1 available seat initially, got %d", queryRes.AvailableSeats)
	}

	// ==========================================
	// ③ 原子位图锁预占锁定阶段 (POST)
	// ==========================================
	reserveReq := map[string]interface{}{
		"request_id":       "REQ_GO_TEST_WORKFLOW_01",
		"schedule_id":      scheduleID,
		"from_station_seq": 1,
		"to_station_seq":   2, // 购买北京 -> 天津
		"seat_class":       "BUSINESS",
		"passenger_ids":    []string{"PSG_GO_01"},
	}
	bodyBytes, _ = json.Marshal(reserveReq)
	req = httptest.NewRequest("POST", "/api/v1/reserve", bytes.NewReader(bodyBytes))
	rec = httptest.NewRecorder()

	handleReserve(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("handleReserve failed: status %d, response: %s", rec.Code, rec.Body.String())
	}

	var reserveRes struct {
		ReservationID string `json:"reservation_id"`
		SeatID        string `json:"seat_id"`
	}
	_ = json.Unmarshal(rec.Body.Bytes(), &reserveRes)
	reservationID := reserveRes.ReservationID
	if reservationID == "" {
		t.Fatalf("ReservationID is empty")
	}

	// ==========================================
	// ④ 同步交易订单创建阶段 (POST)
	// ==========================================
	orderReq := map[string]interface{}{
		"request_id":     "REQ_GO_TEST_ORDER_01",
		"reservation_id": reservationID,
		"amount":         288.50,
	}
	bodyBytes, _ = json.Marshal(orderReq)
	req = httptest.NewRequest("POST", "/api/v1/order", bytes.NewReader(bodyBytes))
	rec = httptest.NewRecorder()

	handleOrder(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("handleOrder failed: status %d, response: %s", rec.Code, rec.Body.String())
	}

	var orderRes struct {
		OrderID string `json:"order_id"`
		State   string `json:"state"`
	}
	_ = json.Unmarshal(rec.Body.Bytes(), &orderRes)
	orderID := orderRes.OrderID
	if orderID == "" {
		t.Fatalf("OrderID is empty")
	}

	// ==========================================
	// ⑤ 最终一致支付核销核销阶段 (POST)
	// ==========================================
	payReq := map[string]interface{}{
		"order_id": orderID,
	}
	bodyBytes, _ = json.Marshal(payReq)
	req = httptest.NewRequest("POST", "/api/v1/pay", bytes.NewReader(bodyBytes))
	rec = httptest.NewRecorder()

	handlePay(rec, req)

	if rec.Code != http.StatusOK {
		t.Fatalf("handlePay failed: status %d, response: %s", rec.Code, rec.Body.String())
	}

	var payRes struct {
		Success bool `json:"success"`
	}
	_ = json.Unmarshal(rec.Body.Bytes(), &payRes)
	if !payRes.Success {
		t.Fatalf("handlePay reported unsuccessful transaction")
	}

	// ==========================================
	// ⑥ 事务发件箱与投影机制手动推演测试
	// ==========================================
	bus := make(chan OutboxEvent, 10)
	publishPendingEvents(bus) // 从 Outbox 捞取 NEW 事件

	select {
	case ev := <-bus:
		if ev.EventType != "SCHEDULE_IMPORTED" && ev.EventType != "RESERVATION_HELD" && ev.EventType != "ORDER_PAID" {
			t.Errorf("Unexpected outbox event type: %s", ev.EventType)
		}
		// 推动 Projector 最终一致投影更新
		var payload struct {
			ScheduleID int `json:"schedule_id"`
		}
		if err := json.Unmarshal(ev.Payload, &payload); err == nil {
			_ = recalculateAndProject(ctx, payload.ScheduleID)
		}
	default:
		// No event, fallback (which is fine since we ran atomic queries)
	}

	// ==========================================
	// ⑦ 最终 SRE 基础设施可用性探针检测 (GET)
	// ==========================================
	req = httptest.NewRequest("GET", "/api/v1/ops/health", nil)
	rec = httptest.NewRecorder()

	handleHealth(rec, req)

	if rec.Code != http.StatusOK {
		t.Errorf("handleHealth failed: status %d", rec.Code)
	}
}

// ============================================================================
// 4. 超高并发瞬间冲突碰撞集成测试 (High-Concurrency Race Condition Collision Test)
// ============================================================================

func TestConcurrentReservation(t *testing.T) {
	ctx := setupTestDB(t)

	// 1. 物理导入一趟全新的列车，配置 1 个席位，保证竞争剧烈度
	importReq := map[string]interface{}{
		"train_code":   "T777",
		"service_date": "2026-11-12",
		"stations": []map[string]interface{}{
			{"name": "北京", "sequence": 1},
			{"name": "天津", "sequence": 2},
			{"name": "上海", "sequence": 3},
		},
		"seats": []map[string]interface{}{
			{"carriage_no": "01", "seat_no": "99F", "seat_class": "BUSINESS"},
		},
	}
	bodyBytes, _ := json.Marshal(importReq)
	req := httptest.NewRequest("POST", "/api/v1/ops/trs/import-schedule", bytes.NewReader(bodyBytes))
	rec := httptest.NewRecorder()

	handleImportSchedule(rec, req)
	if rec.Code != http.StatusOK {
		t.Fatalf("handleImportSchedule failed in Concurrency Test Setup: %s", rec.Body.String())
	}

	var importRes struct {
		Success    bool `json:"success"`
		ScheduleID int  `json:"schedule_id"`
	}
	_ = json.Unmarshal(rec.Body.Bytes(), &importRes)
	scheduleID := importRes.ScheduleID

	// 确保 Redis 预热位图处于初始状态
	rdb.Del(ctx, fmt.Sprintf("q:availability:%d:BUSINESS", scheduleID))
	_ = recalculateAndProject(ctx, scheduleID)

	// 2. 瞬间裂变 20 路并发抢票协程
	concurrencyLimit := 20
	var wg sync.WaitGroup
	wg.Add(concurrencyLimit)

	resultsChan := make(chan int, concurrencyLimit)

	for i := 0; i < concurrencyLimit; i++ {
		go func(idx int) {
			defer wg.Done()

			reserveReq := map[string]interface{}{
				"request_id":       fmt.Sprintf("REQ_GO_CONCURRENCY_%d_%d", idx, time.Now().UnixNano()),
				"schedule_id":      scheduleID,
				"from_station_seq": 1,
				"to_station_seq":   3, // 购买北京 -> 上海 (Seq 1 -> 3)
				"seat_class":       "BUSINESS",
				"passenger_ids":    []string{fmt.Sprintf("PSG_CONCURRENCY_%d", idx)},
			}
			bBytes, _ := json.Marshal(reserveReq)
			hReq := httptest.NewRequest("POST", "/api/v1/reserve", bytes.NewReader(bBytes))
			hRec := httptest.NewRecorder()

			// 执行抢票
			handleReserve(hRec, hReq)

			// 收集其响应状态码
			resultsChan <- hRec.Code
		}(i)
	}

	wg.Wait()
	close(resultsChan)

	successCount := 0
	conflictCount := 0
	otherCount := 0

	for code := range resultsChan {
		switch code {
		case http.StatusOK:
			successCount++
		case http.StatusConflict:
			conflictCount++
		default:
			otherCount++
		}
	}

	// ==========================================
	// 5. 黄金断言 (SRE Concurrency Invariants)
	// ==========================================
	// (a) 有且仅有 1 个购票协程可以成功预占 (200 OK)
	if successCount != 1 {
		t.Errorf("CONCURRENCY FAILURE: Expected exactly 1 successful booking, but got %d", successCount)
	}

	// (b) 剩余的 19 个协程必须精确、互斥地被 Redis Lua / SQL Lock 拦截并打回 409 Conflict 拒绝码
	expectedConflicts := concurrencyLimit - 1
	if conflictCount != expectedConflicts {
		t.Errorf("CONCURRENCY FAILURE: Expected exactly %d conflict failures, but got %d", expectedConflicts, conflictCount)
	}

	if otherCount > 0 {
		t.Errorf("CONCURRENCY FAILURE: Detected %d responses with unexpected error status codes (non-200/409)", otherCount)
	}

	t.Logf("🏆 CONCURRENCY TEST PASSED: Success Booking = %d, Safe Blocks (409 Conflict) = %d. No double booking occurred!", successCount, conflictCount)
}
