Feature: 12306 High-Concurrency Ticketing MVP BDD Acceptance
  As a railway passenger
  I want to query and book sub-route train tickets safely
  So that seat inventory remains strictly consistent without over-selling or deadlocks

  # ------------------------------------------------------------
  # B2C PASSENGER SELLING CORE FLOWS (EPIC-01 ~ EPIC-04)
  # ------------------------------------------------------------

  # 对应 US-2.1 与 US-2.2：区间预占与升序行锁原子保障
  Scenario: Successful sub-route seat reservation
    Given a clean ticketing system with train "G666" and Stations "北京", "天津", "上海"
    And a seat with class "BUSINESS" is fully available
    When passenger requests to reserve a ticket from sequence 1 to 2
    Then the system should grant a reservation ID
    And the MySQL seat segment 1 should be marked as "HELD"
    And the Redis seat mask should reflect the reservation

  # 对应 US-2.1 的区间冲突验证：重叠区段买断，非重叠区段并行占用
  Scenario: Reject overlapping sub-route booking
    Given a passenger has already reserved a ticket from sequence 1 to 2
    When another passenger attempts to reserve a ticket from sequence 2 to 3
    And another passenger attempts to reserve an overlapping ticket from sequence 1 to 3
    Then the non-overlapping booking should succeed
    And the overlapping booking should be rejected as "No seats available"

  # 对应 US-3.2 与 US-4.2：模拟支付状态机与 EDA 最终一致性投影自愈
  Scenario: Payment confirmation triggers eventual consistency
    Given a passenger has successfully reserved a ticket from sequence 1 to 2
    And they have established an order for that reservation
    When they complete payment for the order
    And the background event processor consumes the "ORDER_PAID" event
    Then the MySQL seat segment 1 should be "CONFIRMED"
    And the Redis query model for route 1 to 3 should return 0 available seats


  # ------------------------------------------------------------
  # B2B RAILWAY OPERATOR PORTAL & SMART SEATING (EPIC-05 ~ EPIC-08)
  # ------------------------------------------------------------

  # 对应 US-5.3：官方应急锁段，拒绝普通客流预占 (EPIC-05)
  Scenario: Railway bureau coordinator blocks a seat segment for emergency use, rejecting passenger booking
    Given a clean ticketing system with train "G666" and Stations "北京", "天津", "上海"
    And a seat with class "BUSINESS" is fully available
    When the railway bureau coordinator issues an emergency block on segment 1 for official crew reservation
    Then the MySQL seat segment 1 should be marked as "BLOCKED"
    And the Redis seat mask should reflect the official requisition block
    When a regular passenger attempts to reserve a ticket from sequence 1 to 2
    Then their booking request should be rejected as "Seat segment blocked for official use"

  # 对应 US-5.1：收益定价调整，后续结算金额更新 (EPIC-05)
  Scenario: Revenue manager adjusts dynamic pricing rate and updates passenger billing amount
    Given the dynamic base tariff rate for "BUSINESS" is set to 1.2 yuan per km
    When the revenue manager increases the dynamic base tariff rate to 1.5 yuan per km
    And a passenger creates an order for route sequence 1 to 2 (120 km)
    Then the order payment amount should reflect the updated pricing tariff of 180.00 yuan

  # 对应 US-6.1：多人出行邻座自动分配 (EPIC-06)
  Scenario: Traveling group of two passengers requests booking, receiving adjacent physical seats automatically
    Given a clean ticketing system with train "G666" and Stations "北京", "天津", "上海"
    And adjacent seats "01A" (Window) and "01C" (Aisle) in Carriage 1 are fully available
    When a traveling group of 2 passengers requests to reserve seats from sequence 1 to 3
    Then the system adjacent seat locator should lock both seats "01A" and "01C" in Carriage 1
    And both passengers should receive unified booking details on the same order

  # 对应 US-6.2：同车断配拼座换座自愈推荐 (EPIC-06)
  Scenario: No direct single seat available from start to end, system recomposes a split-seat route for the passenger
    Given the direct ticket availability for train "G666" from sequence 1 to 4 is fully sold out
    And Seat "01A" is available only for segment 1 to 2 (北京-天津)
    And Seat "02C" is available only for segment 2 to 4 (天津-上海)
    When a passenger queries tickets from sequence 1 to 4
    Then the smart recomposition engine should propose a split-seat itinerary "Seat 01A (Seg 1-2) + Seat 02C (Seg 2-4)"
    When the passenger confirms the split-seat itinerary
    Then the system should atomic-reserve segment 1-2 on Seat 01A and segment 2-4 on Seat 02C in a single transaction

  # 对应 US-7.1：长途专售隔离，限制短途购票 (EPIC-07)
  Scenario: Long-distance safeguard pool restricts short-distance booking to preserve full-journey ticket assets
    Given a clean ticketing system with train "G666" and Stations "北京", "天津", "上海"
    And Seat "01A" is allocated in the long-distance safeguard pool (Sequence 1 to 3 exclusive)
    When a passenger attempts to reserve Seat "01A" for short-distance from sequence 1 to 2
    Then the reservation engine should reject the booking as "Quota restricted"
    When another passenger attempts to reserve Seat "01A" for full-journey from sequence 1 to 3
    Then the reservation should succeed with a valid reservation ID

  # 对应 US-7.2：临离发车配额自动释放共享 (EPIC-07)
  Scenario: Unsold long-distance quotas are auto-released near departure time, enabling short-distance bookings
    Given Seat "01A" was locked in the long-distance safeguard pool for full-journey sequence 1 to 3
    And the time to departure is within 24 hours threshold
    When the automatic quota releaser triggers allocation merger
    Then the long-distance isolation lock on Seat "01A" should be dynamic-released
    And the short-distance queries for sequence 1 to 2 should now return 1 available seat

  # 对应 US-8.1、US-8.2 与 US-8.3：TRS 权威发布并秒级激活 12306 Redis 内存预热 (EPIC-08)
  Scenario: TRS authority publishes train schedule with seat allocations, automatically pre-heating 12306 Redis cache
    Given the authoritative Railway Bureau TRS system dispatches a new train schedule for "G999"
    When TRS calls the integration endpoint to publish the G999 plan to 12306
    Then 12306 should atomically commit the train, Stations, and 3 BUSINESS seats with Segment Locks
    And the high-concurrency query cache on Redis for G999 should automatically pre-heat
    And the subsequent passenger query for route sequence 1 to 2 should instantly return 3 available seats

  # ------------------------------------------------------------
  # REAL-NAME PASSENGER TICKETING & COLLISION GUARD (EPIC-09)
  # ------------------------------------------------------------

  # 对应 US-9.1：同乘车人同车次占位重合时空碰撞拦截
  Scenario: Prevent same passenger from duplicate bookings on the same train schedule (Collision Guard)
    Given a clean ticketing system with train "G666" and Stations "北京", "天津", "上海"
    And a seat with class "BUSINESS" is fully available
    And passenger "PSG_CO_01" has already reserved a ticket from sequence 1 to 3
    When passenger "PSG_CO_01" attempts to reserve another ticket on the same train schedule from sequence 1 to 2
    Then the second booking request should be rejected as "conflicting booking"

  # 对应 US-9.2：学生证乘车人自动折抵 75% 优惠结算
  Scenario: Automatically apply student discount for registered student passengers
    Given a clean ticketing system with train "G666" and Stations "北京", "天津", "上海"
    And a seat with class "BUSINESS" is fully available
    And the dynamic base tariff rate for "BUSINESS" is set to 1.2 yuan per km
    And passenger "PSG_ST_01" is registered as a "STUDENT" passenger
    When passenger "PSG_ST_01" requests to reserve a ticket from sequence 1 to 2
    And a passenger creates an order for route sequence 1 to 2 (120 km) for passenger "PSG_ST_01"
    Then the order payment amount should reflect the student discount tariff of 108.00 yuan

  # 对应 US-9.3：候补队列实名穿透与自动安全兑现
  Scenario: Waitlist real-name collision guarding and late-binding auto-fulfillment
    Given a passenger has successfully reserved a ticket from sequence 1 to 2
    And no seats are available for route sequence 1 to 2
    And passenger "PSG_WL_01" attempts to join the waitlist for route sequence 1 to 2
    When the first reservation expires and is released back to the pool
    Then the waitlist queue should trigger real-name collision check
    And passenger "PSG_WL_01" should be atomically fulfilled and granted a seat reservation

  # ------------------------------------------------------------
  # HIGH-FIDELITY REFUND & ATOMIC RESCHEDULE (EPIC-10)
  # ------------------------------------------------------------

  # 对应 US-10.1：阶梯退票手续费与部分退票
  Scenario: Process active refund with dynamic tier-based handling fees
    Given a passenger "PSG_001" has a paid confirmed ticket on "G666" from sequence 1 to 3
    And the departure date is set to "2026-10-02"
    When passenger "PSG_001" requests a refund 48 hours before departure
    Then the system should approve the refund with a 5% handling fee applied
    And the physical seat segments from sequence 1 to 3 should be marked as "AVAILABLE"
    And the Redis seat mask should reflect the released seat segment
    And the waitlist auto-fulfillment queue should be triggered immediately

  # 对应 US-10.2：退旧买新原子改签与多退少补
  Scenario: Atomic rescheduling of ticket to a new train schedule with price adjustment
    Given passenger "PSG_001" holds a paid confirmed ticket on train "G666" (Seq 1 to 3, Business)
    And there is another train "G888" on the same day with available seats
    And the ticket price for "G888" is more expensive than "G666" by 50.00 yuan
    When passenger "PSG_001" requests to reschedule their ticket to "G888"
    Then the system should atomically reserve the new seat on "G888"
    And the old seat on "G666" should be released to the pool
    And the passenger should pay a price difference of 50.00 yuan
    And the old ticket should be updated with the new seat details
