from sqlalchemy import create_engine, Column, String, DateTime, Integer, ForeignKey, Enum as SQLEnum
from sqlalchemy.ext.declarative import declarative_base
from sqlalchemy.orm import sessionmaker, relationship
from datetime import datetime
import enum
import uuid

Base = declarative_base()

class RaceStatus(str, enum.Enum):
    PROCESSING = "Processing"
    FAILED = "Failed"
    READY = "Ready"

class Race(Base):
    __tablename__ = "races"

    race_id = Column(String, primary_key=True, default=lambda: str(uuid.uuid4()))
    created_at = Column(DateTime, default=datetime.utcnow, nullable=False)
    updated_at = Column(DateTime, default=datetime.utcnow, onupdate=datetime.utcnow, nullable=False)
    status = Column(SQLEnum(RaceStatus), default=RaceStatus.PROCESSING, nullable=False)
    raw_data_path = Column(String, nullable=True)

    # Relationship to laps
    laps = relationship("Lap", back_populates="race", cascade="all, delete-orphan")

    def __repr__(self):
        return f"<Race(race_id={self.race_id}, status={self.status}, created_at={self.created_at})>"

class Lap(Base):
    __tablename__ = "laps"

    id = Column(Integer, primary_key=True, autoincrement=True)
    lap_uuid = Column(String, unique=True, nullable=False, default=lambda: str(uuid.uuid4()))
    race_id = Column(String, ForeignKey("races.race_id", ondelete="CASCADE"), nullable=False)
    lap_number = Column(Integer, nullable=False)
    raw_data_path = Column(String, nullable=True)
    processed_data_path = Column(String, nullable=True)

    # Relationship to race
    race = relationship("Race", back_populates="laps")

    # Ensure unique lap numbers per race
    __table_args__ = (
        {"sqlite_autoincrement": True},
    )

    def __repr__(self):
        return f"<Lap(id={self.id}, lap_uuid={self.lap_uuid}, race_id={self.race_id}, lap_number={self.lap_number})>"
