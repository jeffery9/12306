import random
import uuid
from locust import HttpUser, task, between

class RailwayPassengerUser(HttpUser):
    # Simulate realistic passenger thinking time (0.1 to 1.5 seconds between clicks)
    wait_time = between(0.1, 1.5)

    def on_start(self):
        """
        Runs when a simulated passenger spawns.
        We initialize a pool of virtual passenger IDs and select one to avoid immediate real-name collisions,
        while occasionally allowing collisions to test the system's defensive blocking capabilities.
        """
        self.passenger_pool = ["PSG_001", "PSG_002", "PSG_003"]
        # Also generate some virtual/random passengers to test scalability
        for i in range(100):
            self.passenger_pool.append(f"PSG_LOCUST_{i}")

    @task(70)
    def query_tickets(self):
        """
        Task 1 (High Volume Query): Simulates high frequency ticket queries across random stop sequences.
        This tests the Redis Bitmap pre-filter caching layer and FastAPI read-path throughput.
        """
        # G888 train has 4 stations (Seq 1->2->3->4)
        from_seq = random.choice([1, 2, 3])
        to_seq = random.choice([from_seq + 1, from_seq + 2, 4])
        # Ensure to_seq <= 4
        if to_seq > 4:
            to_seq = 4

        schedule_id = 1  # Standard G888 schedule seeded by ./ops.sh seed
        self.client.get(
            f"/api/v1/query?schedule_id={schedule_id}&from_seq={from_seq}&to_seq={to_seq}&seat_class=BUSINESS",
            name="/api/v1/query (Ticket Availability Query)"
        )

    @task(10)
    def reserve_ticket(self):
        """
        Task 2 (High Conflict Reservation): Simulates parallel pre-booking requests.
        Tests Redis Lua + MySQL transactional segment-locking and spatiotemporal collision guarding.
        """
        from_seq = random.choice([1, 2, 3])
        to_seq = random.choice([from_seq + 1, from_seq + 2, 4])
        if to_seq > 4:
            to_seq = 4

        req_id = f"REQ_LOCUST_{uuid.uuid4().hex[:12]}"
        # Randomly select a passenger from our pool
        passenger_id = random.choice(self.passenger_pool)

        payload = {
            "request_id": req_id,
            "schedule_id": 1,
            "from_seq": from_seq,
            "to_seq": to_seq,
            "seat_class": "BUSINESS",
            "passenger_ids": [passenger_id]
        }

        # Catch expected business failures (like sold out, or collision guard blocks) gracefully as success
        with self.client.post("/api/v1/reserve", json=payload, catch_response=True, name="/api/v1/reserve (Book Ticket)") as response:
            if response.status_code == 200:
                response.success()
            elif response.status_code == 400:
                # Business rule rejection (e.g. Sold out or Collision guard block) is an expected outcome
                response.success()
            else:
                response.failure(f"Unexpected HTTP {response.status_code}: {response.text}")

    @task(10)
    def join_waitlist(self):
        """
        Task 3 (Waitlist Standby Queue): Simulates queuing up for sold-out routes.
        """
        from_seq = random.choice([1, 2, 3])
        to_seq = random.choice([from_seq + 1, from_seq + 2, 4])
        if to_seq > 4:
            to_seq = 4

        req_id = f"REQ_LOCUST_WL_{uuid.uuid4().hex[:12]}"
        passenger_id = random.choice(self.passenger_pool)

        payload = {
            "request_id": req_id,
            "schedule_id": 1,
            "from_seq": from_seq,
            "to_seq": to_seq,
            "seat_class": "BUSINESS",
            "passenger_ids": [passenger_id]
        }

        with self.client.post("/api/v1/waitlist", json=payload, catch_response=True, name="/api/v1/waitlist (Submit Waitlist)") as response:
            if response.status_code in [200, 400]:
                response.success()
            else:
                response.failure(f"Unexpected HTTP {response.status_code}: {response.text}")

    @task(10)
    def check_health(self):
        """
        Task 4 (SRE Health Probe): Simulates automatic monitoring systems probing the cluster state.
        """
        self.client.get("/api/v1/ops/health", name="/api/v1/ops/health (SRE Health Check)")
