import datetime
from sqlalchemy import Column, Integer, String, Date, DateTime, Numeric, JSON, ForeignKey, UniqueConstraint, TIMESTAMP, text
from sqlalchemy.orm import relationship
from src.app.database import Base

class Train(Base):
    __tablename__ = "train"

    id = Column(Integer, primary_key=True, autoincrement=True)
    code = Column(String(16), nullable=False, unique=True)
    created_at = Column(TIMESTAMP, server_default=text("CURRENT_TIMESTAMP"))

    stations = relationship("Station", back_populates="train", cascade="all, delete-orphan")
    schedules = relationship("TrainSchedule", back_populates="train", cascade="all, delete-orphan")

class Station(Base):
    __tablename__ = "station"

    id = Column(Integer, primary_key=True, autoincrement=True)
    train_id = Column(Integer, ForeignKey("train.id"), nullable=False)
    name = Column(String(64), nullable=False)
    sequence = Column(Integer, nullable=False)

    train = relationship("Train", back_populates="stations")

    __table_args__ = (
        UniqueConstraint("train_id", "name", name="uk_train_station"),
        UniqueConstraint("train_id", "sequence", name="uk_train_sequence"),
    )

class TrainSchedule(Base):
    __tablename__ = "train_schedule"

    id = Column(Integer, primary_key=True, autoincrement=True)
    train_id = Column(Integer, ForeignKey("train.id"), nullable=False)
    service_date = Column(Date, nullable=False)
    status = Column(String(32), nullable=False, default="ACTIVE")

    train = relationship("Train", back_populates="schedules")
    seats = relationship("Seat", back_populates="schedule", cascade="all, delete-orphan")

    __table_args__ = (
        UniqueConstraint("train_id", "service_date", name="uk_train_date"),
    )

class Seat(Base):
    __tablename__ = "seat"

    id = Column(Integer, primary_key=True, autoincrement=True)
    schedule_id = Column(Integer, ForeignKey("train_schedule.id"), nullable=False)
    carriage_no = Column(String(16), nullable=False)
    seat_no = Column(String(16), nullable=False)
    seat_class = Column(String(32), nullable=False)

    schedule = relationship("TrainSchedule", back_populates="seats")
    segments = relationship("SeatSegment", back_populates="seat", cascade="all, delete-orphan")

    __table_args__ = (
        UniqueConstraint("schedule_id", "carriage_no", "seat_no", name="uk_schedule_seat"),
    )

class SeatSegment(Base):
    __tablename__ = "seat_segment"

    schedule_id = Column(Integer, nullable=False, primary_key=True)
    seat_id = Column(Integer, ForeignKey("seat.id"), nullable=False, primary_key=True)
    segment_no = Column(Integer, nullable=False, primary_key=True)
    reservation_id = Column(String(64), nullable=True)
    state = Column(String(16), nullable=False, default="AVAILABLE")
    version = Column(Integer, nullable=False, default=0)

    seat = relationship("Seat", back_populates="segments")

class Reservation(Base):
    __tablename__ = "reservation"

    id = Column(String(64), primary_key=True)
    request_id = Column(String(64), nullable=False, unique=True)
    schedule_id = Column(Integer, ForeignKey("train_schedule.id"), nullable=False)
    seat_id = Column(Integer, ForeignKey("seat.id"), nullable=False)
    from_segment = Column(Integer, nullable=False)
    to_segment = Column(Integer, nullable=False)
    state = Column(String(32), nullable=False)  # HELD, CONFIRMED, RELEASED
    expires_at = Column(TIMESTAMP, nullable=False)
    created_at = Column(TIMESTAMP, server_default=text("CURRENT_TIMESTAMP"))
    updated_at = Column(TIMESTAMP, server_default=text("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"))

class Orders(Base):
    __tablename__ = "orders"

    id = Column(String(64), primary_key=True)
    request_id = Column(String(64), nullable=False, unique=True)
    reservation_id = Column(String(64), ForeignKey("reservation.id"), nullable=False, unique=True)
    state = Column(String(32), nullable=False)  # WAITING_PAYMENT, CONFIRMED, EXPIRED, CANCELLED
    total_amount = Column(Numeric(18, 2), nullable=False)
    expires_at = Column(TIMESTAMP, nullable=False)
    created_at = Column(TIMESTAMP, server_default=text("CURRENT_TIMESTAMP"))
    updated_at = Column(TIMESTAMP, server_default=text("CURRENT_TIMESTAMP ON UPDATE CURRENT_TIMESTAMP"))

class OutboxEvent(Base):
    __tablename__ = "outbox_event"

    id = Column(Integer, primary_key=True, autoincrement=True)
    event_id = Column(String(36), nullable=False, unique=True)
    aggregate_type = Column(String(64), nullable=False)
    aggregate_id = Column(String(64), nullable=False)
    event_type = Column(String(128), nullable=False)
    payload = Column(JSON, nullable=False)
    status = Column(String(16), nullable=False, default="NEW")
    created_at = Column(TIMESTAMP, server_default=text("CURRENT_TIMESTAMP"))
    published_at = Column(TIMESTAMP, nullable=True)

class ProcessedEvent(Base):
    __tablename__ = "processed_event"

    consumer_name = Column(String(128), primary_key=True)
    event_id = Column(String(36), primary_key=True)
    processed_at = Column(TIMESTAMP, server_default=text("CURRENT_TIMESTAMP"))
