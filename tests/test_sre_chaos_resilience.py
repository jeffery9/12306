import pytest
import datetime
import httpx
from unittest.mock import patch, AsyncMock
from src.app.models import Train, Station, TrainSchedule, Seat, SeatSegment
from src.app.main import app

def test_w3c_trace_context_propagation(event_loop):
    """Verify HTTP tracing context propagation middleware and W3C header alignment."""
    async def _impl():
        # Create standard W3C traceparent header
        # Format: 00-trace_id-span_id-trace_flags
        input_trace_id = "4bf92f3577b34da6a3ce929d0e0e4736"
        input_span_id = "00f067aa0ba902b7"
        input_traceparent = f"00-{input_trace_id}-{input_span_id}-01"
        
        headers = {
            "traceparent": input_traceparent
        }
        
        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
            # Query any lightweight endpoint (e.g., status/root/health)
            response = await client.get("/api/v1/ops/health", headers=headers)
            
            # Check trace headers in the response
            assert response.status_code == 200
            assert "traceparent" in response.headers
            assert "X-Trace-ID" in response.headers
            assert "X-12306-Engine" in response.headers
            
            # Verify tracing context preservation
            assert response.headers["X-Trace-ID"] == input_trace_id
            assert response.headers["X-12306-Engine"] == "python-fastapi"
            assert response.headers["traceparent"].startswith(f"00-{input_trace_id}-")
            
    event_loop.run_until_complete(_impl())

def test_redis_spof_down_resilience_and_graceful_degradation(db_session, event_loop):
    """Verify the query API's self-healing fallback when Redis encounters a physical SPOF breakdown."""
    async def _impl():
        # 1. Seed PostgreSQL database with a train schedule, station and seats
        train = Train(code="G999")
        db_session.add(train)
        await db_session.flush()

        s1 = Station(train_id=train.id, name="北京", sequence=1)
        s2 = Station(train_id=train.id, name="天津", sequence=2)
        s3 = Station(train_id=train.id, name="南京", sequence=3)
        db_session.add_all([s1, s2, s3])

        schedule = TrainSchedule(train_id=train.id, service_date=datetime.date(2026, 11, 1), status="ACTIVE")
        db_session.add(schedule)
        await db_session.flush()

        # Seed 3 business class seats for testing
        seat1 = Seat(schedule_id=schedule.id, carriage_no="01", seat_no="01A", seat_class="BUSINESS")
        seat2 = Seat(schedule_id=schedule.id, carriage_no="01", seat_no="01B", seat_class="BUSINESS")
        seat3 = Seat(schedule_id=schedule.id, carriage_no="01", seat_no="01C", seat_class="BUSINESS")
        db_session.add_all([seat1, seat2, seat3])
        await db_session.flush()

        # Segment 1 (Beijing -> Tianjin): seat1 is reserved, seat2 & seat3 are free
        # Segment 2 (Tianjin -> Nanjing): all three are free
        seg1_1 = SeatSegment(schedule_id=schedule.id, seat_id=seat1.id, segment_no=1, state="HELD", version=0)
        seg1_2 = SeatSegment(schedule_id=schedule.id, seat_id=seat1.id, segment_no=2, state="AVAILABLE", version=0)
        
        seg2_1 = SeatSegment(schedule_id=schedule.id, seat_id=seat2.id, segment_no=1, state="AVAILABLE", version=0)
        seg2_2 = SeatSegment(schedule_id=schedule.id, seat_id=seat2.id, segment_no=2, state="AVAILABLE", version=0)
        
        seg3_1 = SeatSegment(schedule_id=schedule.id, seat_id=seat3.id, segment_no=1, state="AVAILABLE", version=0)
        seg3_2 = SeatSegment(schedule_id=schedule.id, seat_id=seat3.id, segment_no=2, state="AVAILABLE", version=0)
        
        db_session.add_all([seg1_1, seg1_2, seg2_1, seg2_2, seg3_1, seg3_2])
        await db_session.commit()

        # 2. Simulate Redis Connection Error (SPOF down) by patching get_redis to throw ConnectionError
        # We raise an exception on redis hget to simulate cache breakdown
        mock_redis = AsyncMock()
        mock_redis.hget.side_effect = Exception("Redis physical cluster connection timeout.")
        
        with patch("src.app.main.get_redis", return_value=mock_redis):
            async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
                # Query seat availability for Beijing (seq 1) -> Tianjin (seq 2)
                # Expected seats: 2 (Seat2, Seat3 are free; Seat1 is HELD)
                response1 = await client.get(
                    f"/api/v1/query?schedule_id={schedule.id}&from_station_seq=1&to_station_seq=2&seat_class=BUSINESS"
                )
                
                # Query availability for Tianjin (seq 2) -> Nanjing (seq 3)
                # Expected seats: 3 (Seat1, Seat2, Seat3 are all free for segment 2)
                response2 = await client.get(
                    f"/api/v1/query?schedule_id={schedule.id}&from_station_seq=2&to_station_seq=3&seat_class=BUSINESS"
                )
                
                # 3. Assertions to verify successful degradation & accurate bitmask calculation on-the-fly without Redis
                assert response1.status_code == 200
                assert response1.json() == {"available_seats": 2}
                
                assert response2.status_code == 200
                assert response2.json() == {"available_seats": 3}
                
                # Confirm that redis hget was indeed called and failed, triggering fallback
                assert mock_redis.hget.called

    event_loop.run_until_complete(_impl())

def test_outbox_and_projector_trace_propagation(db_session, event_loop):
    """Verify asynchronous W3C tracing context propagation across transactional outbox and CQRS Projector boundaries."""
    async def _impl():
        from src.app.telemetry import trace_id_var, traceparent_var
        from src.app.models import OutboxEvent
        from src.app.projector import Projector
        
        # 1. Set context variables to simulate active HTTP request thread
        test_trace_id = "aabbccddeeff00112233445566778899"
        test_traceparent = f"00-{test_trace_id}-1122334455667788-01"
        
        token_id = trace_id_var.set(test_trace_id)
        token_parent = traceparent_var.set(test_traceparent)
        
        try:
            # 2. Instantiate OutboxEvent; context should automatically inject W3C traceparent into the JSON payload
            event = OutboxEvent(
                event_id="evt_tracing_test_01",
                aggregate_type="ORDER",
                aggregate_id="ord_test_01",
                event_type="ORDER_CREATED",
                payload={"order_id": "ord_test_01", "schedule_id": 999}
            )
            
            # Assert automatic context injection
            assert "traceparent" in event.payload
            assert event.payload["traceparent"] == test_traceparent
        finally:
            trace_id_var.reset(token_id)
            traceparent_var.reset(token_parent)
            
        # Clear variables to simulate a fresh Kafka worker environment
        assert trace_id_var.get() == ""
        assert traceparent_var.get() == ""
        
        # 3. Simulate Projector consuming the outbox event in background
        # We patch Projector.recalculate_and_project to capture and verify the trace_id in context during execution!
        trace_captured_in_projector = ""
        
        async def mock_recalculate(db_session, schedule_id):
            nonlocal trace_captured_in_projector
            trace_captured_in_projector = trace_id_var.get()
            
        with patch.object(Projector, "recalculate_and_project", side_effect=mock_recalculate):
            # Formulate the raw event dictionary received from Kafka
            event_dict = {
                "event_id": "evt_tracing_test_01",
                "payload": event.payload
            }
            
            # Execute background projection
            await Projector.process_event(db_session=db_session, event=event_dict)
            
            # Verify context propagation successfully resumed inside the background task context!
            assert trace_captured_in_projector == test_trace_id
            
        # Verify that context variables are safely cleaned up after processing to prevent memory leakage
        assert trace_id_var.get() == ""
        assert traceparent_var.get() == ""
        
    event_loop.run_until_complete(_impl())

def test_graphql_api_queries_and_mutations(db_session, event_loop):
    """Verify standard GraphQL schema query filtering, nested ticket resolving, and metadata SDL endpoints."""
    async def _impl():
        # 1. Seed Train and Seats for GraphQL query verification
        train = Train(code="G777")
        db_session.add(train)
        await db_session.flush()

        s1 = Station(train_id=train.id, name="北京", sequence=1)
        s2 = Station(train_id=train.id, name="上海", sequence=2)
        db_session.add_all([s1, s2])

        schedule = TrainSchedule(train_id=train.id, service_date=datetime.date(2026, 12, 1), status="ACTIVE")
        db_session.add(schedule)
        await db_session.flush()

        seat = Seat(schedule_id=schedule.id, carriage_no="02", seat_no="02D", seat_class="BUSINESS")
        db_session.add(seat)
        await db_session.flush()

        seg = SeatSegment(schedule_id=schedule.id, seat_id=seat.id, segment_no=1, state="AVAILABLE", version=0)
        db_session.add(seg)
        await db_session.flush()

        # Seed Passenger, Reservation and Orders record for detail querying
        from src.app.models import Passenger, Reservation, Orders, Ticket
        psg = Passenger(
            id="PSG_GQL_01",
            name="GraphQL Tester",
            id_no="110101199001017777",
            passenger_type="ADULT"
        )
        db_session.add(psg)
        await db_session.flush()

        res_record = Reservation(
            id="RES_GQL_01",
            request_id="REQ_GQL_RES_01",
            schedule_id=schedule.id,
            seat_id=seat.id,
            from_segment=1,
            to_segment=2,
            state="HELD",
            expires_at=datetime.datetime(2026, 12, 1, 12, 0, 0)
        )
        db_session.add(res_record)
        await db_session.flush()

        order = Orders(
            id="ORD_GQL_TEST_01",
            request_id="REQ_GQL_01",
            reservation_id="RES_GQL_01",
            state="CONFIRMED",
            total_amount=100.0,
            expires_at=datetime.datetime(2026, 12, 1, 12, 0, 0)
        )
        db_session.add(order)
        await db_session.flush()

        ticket = Ticket(
            id="TK_GQL_TEST_01",
            reservation_id="RES_GQL_01",
            passenger_id="PSG_GQL_01",
            seat_id=seat.id,
            price=100.0
        )
        db_session.add(ticket)
        await db_session.commit()

        # 2. Invoke HTTP GraphQL Query: queryAvailability
        # We request ONLY availableSeats and scheduleId to test over-fetching avoidance
        query_availability_gql = """
        query {
          queryAvailability(scheduleId: SCHEDULE_ID, fromStationSeq: 1, toStationSeq: 2, seatClass: "BUSINESS") {
            availableSeats
            scheduleId
          }
        }
        """.replace("SCHEDULE_ID", str(schedule.id))

        async with httpx.AsyncClient(transport=httpx.ASGITransport(app=app), base_url="http://test") as client:
            # POST endpoint query
            resp1 = await client.post("/graphql", json={"query": query_availability_gql})
            assert resp1.status_code == 200
            resp1_json = resp1.json()
            assert "errors" not in resp1_json
            assert "data" in resp1_json
            
            qa_data = resp1_json["data"]["queryAvailability"]
            assert qa_data["availableSeats"] == 1  # Field name was resolved & normalized
            assert qa_data["scheduleId"] == schedule.id
            # Confirm other fields (fromStationSeq, seatClass) are NOT returned, enforcing strict over-fetching defense!
            assert "seatClass" not in qa_data
            assert "fromStationSeq" not in qa_data

            # 3. Invoke HTTP GraphQL Query: order details with nested tickets
            query_order_gql = """
            query {
              order(id: "ORD_GQL_TEST_01") {
                id
                state
                totalAmount
                tickets {
                  id
                  seatNo
                  price
                }
              }
            }
            """
            resp2 = await client.post("/graphql", json={"query": query_order_gql})
            assert resp2.status_code == 200
            resp2_json = resp2.json()
            assert "errors" not in resp2_json
            assert "data" in resp2_json
            
            order_data = resp2_json["data"]["order"]
            assert order_data["id"] == "ORD_GQL_TEST_01"
            assert order_data["state"] == "CONFIRMED"
            assert order_data["totalAmount"] == 100.0
            
            tickets_list = order_data["tickets"]
            assert len(tickets_list) == 1
            assert tickets_list[0]["id"] == "TK_GQL_TEST_01"
            assert tickets_list[0]["seatNo"] == "02D"
            assert tickets_list[0]["price"] == 100.0
            # Ensure "passengerId" is not returned, verifying nested over-fetching protection
            assert "passengerId" not in tickets_list[0]

            # 4. Invoke HTTP GraphQL GET: Schema SDL representation
            resp3 = await client.get("/graphql")
            assert resp3.status_code == 200
            resp3_json = resp3.json()
            assert "schema" in resp3_json
            assert "type TrainAvailability" in resp3_json["schema"]
            assert "type Order" in resp3_json["schema"]

    event_loop.run_until_complete(_impl())
