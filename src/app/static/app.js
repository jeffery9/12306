import { createApp, ref, computed, watch, onMounted, nextTick } from 'https://unpkg.com/vue@3/dist/vue.esm-browser.js';

// 1. Component: RouteSelector
const RouteSelector = {
  template: "#route-selector-template",
  props: {
    stations: { type: Array, required: true },
    fromStation: { type: Number, required: true },
    toStation: { type: Number, required: true },
    activeTrackStyle: { type: Object, required: true }
  },
  emits: ["update:fromStation", "update:toStation", "select-station", "query-availability"]
};

// 2. Component: TicketBooking
const TicketBooking = {
  template: "#ticket-booking-template",
  props: {
    availableSeats: { type: [Number, Object], default: null },
    passengerCount: { type: Number, required: true },
    isCacheMiss: { type: Boolean, required: true },
    isQueryingSplit: { type: Boolean, required: true },
    splitItinerary: { type: Object, default: null },
    isReserveDisabled: { type: Boolean, required: true }
  },
  emits: ["update:passengerCount", "query-split", "reserve-ticket", "reserve-split"]
};

// 3. Component: OrderPayment
const OrderPayment = {
  template: "#order-payment-template",
  props: {
    reservationId: { type: String, default: null },
    lockedPassengerCount: { type: Number, required: true },
    secondsLeft: { type: Number, required: true },
    progressBarStyle: { type: Object, required: true },
    computedTotalAmount: { type: Number, required: true },
    isPaid: { type: Boolean, required: true },
    splitItinerary: { type: Object, default: null },
    isSplitReservation: { type: Boolean, required: true },
    fromStation: { type: Number, required: true },
    toStation: { type: Number, required: true }
  },
  emits: ["pay-order", "reset"]
};

// 4. Component: SystemLogs
const SystemLogs = {
  template: "#system-logs-template",
  props: {
    logs: { type: Array, required: true }
  }
};

// 5. Component: DevopsConsole
const DevopsConsole = {
  template: "#devops-console-template",
  emits: ["cron-release", "quota-release"]
};

// Main App Orchestrator Instance
const app = createApp({
  components: {
    RouteSelector,
    TicketBooking,
    OrderPayment,
    SystemLogs,
    DevopsConsole
  },
  setup() {
    // Configuration Constants
    const BACKEND_URL = "http://localhost:8000";
    const SCHEDULE_ID = 1;
    const SEAT_CLASS = "BUSINESS";

    // Stations Static Data
    const stations = [
      { seq: 1, name: "北京" },
      { seq: 2, name: "天津" },
      { seq: 3, name: "济南" },
      { seq: 4, name: "上海" },
    ];

    // Reactive State Variables (Centralized Single Source of Truth)
    const fromStation = ref(1);
    const toStation = ref(4);
    const availableSeats = ref(null);
    const isCacheMiss = ref(false);
    const hasQueried = ref(false);
    const queryMissCount = ref(0);

    const passengerCount = ref(1);
    const lockedPassengerCount = ref(1);
    const splitItinerary = ref(null);
    const isQueryingSplit = ref(false);
    const isSplitReservation = ref(false);

    const reservationId = ref(null);
    const orderId = ref(null);
    const secondsLeft = ref(60);
    const isPaid = ref(false);
    const logs = ref([]);

    let countdownInterval = null;

    // Log Console Writer Helper
    const pushLog = (text, color = null) => {
      const timeStr = new Date().toTimeString().split(" ")[0];
      logs.value.push({ time: timeStr, text, color });

      // Auto scroll logger on next DOM tick
      nextTick(() => {
        const consoleLogs = document.getElementById("console-logs");
        if (consoleLogs) {
          consoleLogs.scrollTop = consoleLogs.scrollHeight;
        }
      });
    };

    // COMPUTED: Train Track Highlight Style
    const activeTrackStyle = computed(() => {
      const leftPercent = (fromStation.value - 1) * 33.33;
      const widthPercent = (toStation.value - fromStation.value) * 33.33;
      return {
        left: `calc(10% + ${leftPercent * 0.8}%)`,
        width: `${widthPercent * 0.8}%`,
      };
    });

    // COMPUTED: Dynamic distance and price calculation
    const computedTotalAmount = computed(() => {
      const segments = toStation.value - fromStation.value;
      const distance = segments * 120; // 120km per segment
      const rate = 1.2; // base rate
      return rate * distance * lockedPassengerCount.value;
    });

    // COMPUTED: Live Countdown Progress Bar style
    const progressBarStyle = computed(() => {
      if (secondsLeft.value <= 0) return { width: "0%" };
      return {
        width: `${(secondsLeft.value / 60) * 100}%`,
      };
    });

    // COMPUTED: Is Reservation Button Disabled
    const isReserveDisabled = computed(() => {
      return (
        availableSeats.value === null ||
        availableSeats.value <= 0 ||
        isPaid.value
      );
    });

    // WATCHER: Reset query state when station selection changes
    watch([fromStation, toStation], ([newFrom, newTo]) => {
      // Keep "To Station" greater than "From Station"
      if (newTo <= newFrom) {
        toStation.value = newFrom + 1;
        return;
      }
      availableSeats.value = null; // Reset results to force fresh query
      splitItinerary.value = null;
      isSplitReservation.value = false;
      pushLog(`路由区间更新为: Seq ${newFrom} ──► Seq ${newTo}`);
    });

    // Actions: User selection via station nodes click
    const selectStation = (seq) => {
      if (seq === 4) {
        toStation.value = 4;
      } else {
        fromStation.value = seq;
      }
    };

    // QUERY AVAILABILITY (GET /api/v1/query)
    const queryAvailability = async () => {
      pushLog(
        `[Query] 向后端发起余票查询 (Route: ${fromStation.value} -> ${toStation.value})...`,
      );
      splitItinerary.value = null;

      try {
        const url = `${BACKEND_URL}/api/v1/query?schedule_id=${SCHEDULE_ID}&from_station_seq=${fromStation.value}&to_station_seq=${toStation.value}&seat_class=${SEAT_CLASS}`;
        const response = await fetch(url);
        if (!response.ok)
          throw new Error(`HTTP Error Status: ${response.status}`);

        const data = await response.json();
        availableSeats.value = data.available_seats;
        hasQueried.value = true;

        // Mock self-healing display indicator for cold projections on first query
        if (
          queryMissCount.value === 0 ||
          (fromStation.value === 1 &&
            toStation.value === 3 &&
            queryMissCount.value === 1)
        ) {
          isCacheMiss.value = true;
          pushLog(
            `[CQRS-Heal] 读缓存未命中 (Cache Miss)，写侧投影器已异步触发 cold projection 重算并重新建立 Redis 哈希视图！`,
            "var(--color-amber)",
          );
          queryMissCount.value++;
        } else {
          isCacheMiss.value = false;
          pushLog(
            `[CQRS-Hit] 读缓存秒命中 (Redis Hit)，当前可用商务票数: ${availableSeats.value} 张`,
            "var(--color-emerald)",
          );
        }

        if (availableSeats.value <= 0) {
          pushLog(
            `[SOLD_OUT] 很抱歉，当前直达席位已被买断售罄！已自动激活智能拼座推荐引擎。`,
            "var(--color-rose)",
          );
        }
      } catch (error) {
        pushLog(
          `[Error] 余票查询失败: ${error.message}. 请确保 Uvicorn 后端已拉起并在 http://localhost:8000 运行。`,
          "var(--color-rose)",
        );
        alert("API 网络连通性失败，请查看日志控制台");
      }
    };

    // QUERY SPLIT ITINERARY (GET /api/v1/query/recompose)
    const querySplitItinerary = async () => {
      isQueryingSplit.value = true;
      pushLog(`[Smart-Recompose] 智能换乘接续引擎开始推演中转拼位...`);
      
      try {
        const url = `${BACKEND_URL}/api/v1/query/recompose?schedule_id=${SCHEDULE_ID}&from_station_seq=${fromStation.value}&to_station_seq=${toStation.value}&seat_class=${SEAT_CLASS}`;
        const response = await fetch(url);
        if (!response.ok) throw new Error("接续引擎调用失败");

        const data = await response.json();
        if (data.split_found) {
          splitItinerary.value = data;
          pushLog(
            `[Split-Found] 发现完美同车拼位方案: ${data.description}`,
            "var(--color-amber)",
          );
        } else {
          splitItinerary.value = { split_found: false };
          pushLog(
            `[Split-None] 很抱歉，该直达售罄区间目前无法拼凑任何同车中转接续点。`,
            "var(--color-rose)",
          );
        }
      } catch (error) {
        pushLog(`[Error] 拼座查询失败: ${error.message}`, "var(--color-rose)");
      } finally {
        isQueryingSplit.value = false;
      }
    };

    // RESERVE TICKET (POST /api/v1/reserve)
    const reserveTicket = async () => {
      const reqId =
        "REQ_WEB_" +
        Math.random().toString(36).substr(2, 9).toUpperCase();
      
      if (passengerCount.value === 2) {
        pushLog(`[Adjacent-Lock] 正在向分配中心申请 2 张相邻席位锁票...`);
      } else {
        pushLog(`[Reserve] 正在向分配中心预留座位，Request ID: ${reqId}...`);
      }

      try {
        const url = `${BACKEND_URL}/api/v1/reserve`;
        const response = await fetch(url, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            request_id: reqId,
            schedule_id: SCHEDULE_ID,
            from_station_seq: fromStation.value,
            to_station_seq: toStation.value,
            seat_class: SEAT_CLASS,
            passenger_count: passengerCount.value,
          }),
        });

        if (!response.ok) {
          const errorData = await response.json();
          throw new Error(
            errorData.detail || `HTTP Error ${response.status}`,
          );
        }

        const data = await response.json();
        reservationId.value = data.reservation_id;
        lockedPassengerCount.value = passengerCount.value;
        isSplitReservation.value = false;

        if (lockedPassengerCount.value === 2) {
          pushLog(
            `[Adjacent-Lock] 邻座自动分配成功！锁座 ID: ${reservationId.value} | 占用了同一排 01A(窗) + 01C(道) 物理座位！`,
            "var(--color-emerald)",
          );
        } else {
          pushLog(
            `[Success] 预占锁座成功！Reservation ID: ${reservationId.value}`,
            "var(--color-emerald)",
          );
        }

        pushLog(
          `[Lock] MySQL 区间行锁、Redis 位掩码已原子加锁，预留状态为: HELD`,
          "var(--color-amber)",
        );

        startCountdown();
      } catch (error) {
        pushLog(
          `[Error] 占座锁定失败: ${error.message}`,
          "var(--color-rose)",
        );
        alert(`抢票失败: ${error.message}`);
      }
    };

    // RESERVE SPLIT TICKET (POST /api/v1/reserve/split)
    const reserveSplitTicket = async () => {
      const reqId =
        "REQ_SPLIT_" +
        Math.random().toString(36).substr(2, 9).toUpperCase();
      
      pushLog(`[Split-Reserve] 正在一键原子锁定同车拼位接续座位...`);

      try {
        const url = `${BACKEND_URL}/api/v1/reserve/split`;
        const response = await fetch(url, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            request_id: reqId,
            schedule_id: SCHEDULE_ID,
            seat1_id: splitItinerary.value.seat1_id,
            from1: fromStation.value,
            to1: splitItinerary.value.mid_seq,
            seat2_id: splitItinerary.value.seat2_id,
            from2: splitItinerary.value.mid_seq,
            to2: toStation.value
          }),
        });

        if (!response.ok) {
          const errorData = await response.json();
          throw new Error(
            errorData.detail || `HTTP Error ${response.status}`,
          );
        }

        const data = await response.json();
        reservationId.value = data.reservation_id;
        lockedPassengerCount.value = 1;
        isSplitReservation.value = true;

        pushLog(
          `[Success] 拼位接续锁定成功！锁座 ID: ${reservationId.value}。在单个事务中分别原子占用了 ${splitItinerary.value.seat1_no} (Leg 1) 和 ${splitItinerary.value.seat2_no} (Leg 2) 的物理分段！`,
          "var(--color-emerald)",
          "var(--color-emerald)",
        );

        startCountdown();
      } catch (error) {
        pushLog(
          `[Error] 拼座锁定失败: ${error.message}`,
          "var(--color-rose)",
        );
        alert(`抢票失败: ${error.message}`);
      }
    };

    // Holding Countdown Handler
    const startCountdown = () => {
      if (countdownInterval) clearInterval(countdownInterval);
      secondsLeft.value = 60;

      countdownInterval = setInterval(() => {
        secondsLeft.value--;
        if (secondsLeft.value <= 0) {
          clearInterval(countdownInterval);
          pushLog(
            "[Timeout] 预占已超过 60s 支付限期，该订单在底层已被废弃。请点击下方 DevOps 控制台强制触发超时回收任务！",
            "var(--color-rose)",
          );
        }
      }, 1000);
    };

    // CREATE ORDER & PAY ORDER (POST /api/v1/order -> POST /api/v1/pay)
    const payOrder = async () => {
      if (secondsLeft.value <= 0) {
        alert("订单已超时失效，请重新预占！");
        return;
      }

      const reqId =
        "REQ_PAY_" +
        Math.random().toString(36).substr(2, 9).toUpperCase();
      pushLog(
        `[Pay] 正在为预留单 ${reservationId.value} 创建正式待付订单并请求动态计费网关...`,
      );

      try {
        // Step A: Create Order
        const orderUrl = `${BACKEND_URL}/api/v1/order`;
        const orderResponse = await fetch(orderUrl, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({
            request_id: reqId,
            reservation_id: reservationId.value,
            amount: 0.0, // Pass 0.0 to trigger write model Distance Pricing calculator!
          }),
        });

        if (!orderResponse.ok) {
          const errorData = await orderResponse.json();
          throw new Error(`创建订单失败: ${errorData.detail}`);
        }

        const orderData = await orderResponse.json();
        orderId.value = orderData.order_id;
        pushLog(
          `[Order] 订单生成并计价成功！ID: ${orderId.value}，结算额: ¥${computedTotalAmount.value.toFixed(2)}，进入状态: WAITING_PAYMENT`,
        );

        // Step B: Pay Order (Confirm and settle seat segments)
        pushLog(`[Gateway] 正在调起银行扣款网关并核销订单...`);
        const payUrl = `${BACKEND_URL}/api/v1/pay`;
        const payResponse = await fetch(payUrl, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ order_id: orderId.value }),
        });

        if (!payResponse.ok) {
          const errorData = await payResponse.json();
          throw new Error(`订单支付失败: ${errorData.detail}`);
        }

        const payResult = await payResponse.json();
        if (payResult.success) {
          if (countdownInterval) clearInterval(countdownInterval);
          isPaid.value = true;
          pushLog(
            `[Success] 支付成功！状态机转移为: CONFIRMED。物理席位已被原子占领！`,
            "var(--color-emerald)",
          );
          pushLog(
            `[Outbox] 事务性发件箱已写入 "ORDER_PAID" 物理事件，准备通知高并发 Kafka 进行一致性读视图投影。`,
            "var(--color-rail-blue)",
          );
        } else {
          throw new Error("扣款网关返回核销失败");
        }
      } catch (error) {
        pushLog(
          `[Error] 支付失败: ${error.message}`,
          "var(--color-rose)",
        );
        alert(error.message);
      }
    };

    // ADMIN TRIGGER CRON TIMEOUT RELEASE (POST /api/v1/cron/release)
    const triggerCronRelease = async () => {
      pushLog(`[Cron] 手动调度后台超时清理 Worker ...`);

      try {
        const url = `${BACKEND_URL}/api/v1/cron/release`;
        const response = await fetch(url, { method: "POST" });
        if (!response.ok)
          throw new Error(`Cron 触发失败 Status: ${response.status}`);

        const data = await response.json();
        const released = data.released_count;

        if (released > 0) {
          pushLog(
            `[Cron-Cleanup] 清理成功！物理发还了 ${released} 个超时未付的过期座位段。`,
            "var(--color-emerald)",
          );
          pushLog(
            `[Cron-Sync] 倒带 Redis 内存占用，同时向 Outbox 插入了释放事件以确保最终一致性！`,
            "var(--color-rail-blue)",
          );

          if (secondsLeft.value <= 0 && reservationId.value) {
            resetWorkflow();
          }
          await queryAvailability(); // Auto re-query to pull fresh state
        } else {
          pushLog(`[Cron-Skip] 无任何超期未支付席位，跳过回收逻辑。`);
        }
      } catch (error) {
        pushLog(
          `[Error] Cron 清理执行异常: ${error.message}`,
          "var(--color-rose)",
        );
      }
    };

    // ADMIN TRIGGER LONG DISTANCE QUOTA RELEASE (POST /api/v1/ops/quota/release)
    const releaseLongDistanceQuota = async () => {
      pushLog(`[Ops] 正在手动调用客运调度 API 一键合并长途限售配额...`);

      try {
        const url = `${BACKEND_URL}/api/v1/ops/quota/release`;
        const response = await fetch(url, {
          method: "POST",
          headers: { "Content-Type": "application/json" },
          body: JSON.stringify({ schedule_id: SCHEDULE_ID }),
        });
        if (!response.ok)
          throw new Error(`解封 API 返回失败 Status: ${response.status}`);

        const data = await response.json();
        const released = data.released_count;

        if (released > 0) {
          pushLog(
            `[Ops-Release] 合并解封成功！成功释放了 ${released} 个长途隔离专属席位（Seat 01A），并将其合并回共享席位票池中。`,
            "var(--color-emerald)",
          );
          pushLog(
            `[Ops-Projector] 已自动触发写侧数据库冷源重算并向 Redis 高并发哈希读视图推送最新余票。`,
            "var(--color-rail-blue)",
          );
          await queryAvailability(); // Pull the fresh seats instantly!
        } else {
          pushLog(`[Ops-Skip] 无任何未售出的长途隔离配额席位，跳过解锁动作。`);
        }
      } catch (error) {
        pushLog(
          `[Error] 解封执行异常: ${error.message}`,
          "var(--color-rose)",
        );
      }
    };

    // Reset workflows for next ticket purchase
    const resetWorkflow = () => {
      reservationId.value = null;
      orderId.value = null;
      isPaid.value = false;
      splitItinerary.value = null;
      isSplitReservation.value = false;
      if (countdownInterval) clearInterval(countdownInterval);
      pushLog("重置前端购票面板，旅客可以进行下一次区间抢购。");
    };

    // Lifecycle Hooks
    onMounted(() => {
      pushLog("Vue 3 原生 ESM 响应式组件化大屏系统初始化成功！");
    });

    return {
      stations,
      fromStation,
      toStation,
      availableSeats,
      isCacheMiss,
      hasQueried,
      passengerCount,
      lockedPassengerCount,
      splitItinerary,
      isQueryingSplit,
      isSplitReservation,
      reservationId,
      orderId,
      secondsLeft,
      isPaid,
      logs,
      activeTrackStyle,
      computedTotalAmount,
      progressBarStyle,
      isReserveDisabled,
      selectStation,
      queryAvailability,
      querySplitItinerary,
      reserveTicket,
      reserveSplitTicket,
      payOrder,
      triggerCronRelease,
      releaseLongDistanceQuota,
      resetWorkflow,
    };
  },
});

app.mount("#app");
