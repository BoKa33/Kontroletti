from sqlalchemy import Column, Integer, String, Float, DateTime, ForeignKey, Boolean, Index
from sqlalchemy.orm import relationship, declarative_base
import datetime

Base = declarative_base()

class StationRegistry(Base):
    """
    The Single Source of Truth for all stations.
    Maps Community IDs (GTFS.DE) and Official IDs (DELFI) to a stable Canonical ID.
    """
    __tablename__ = 'station_registry'

    canonical_id = Column(String, primary_key=True)  # e.g., 'de:06412:1502' or 'synth:a1b2c3d4'
    name = Column(String, nullable=False, index=True)
    lat = Column(Float, nullable=False)
    lon = Column(Float, nullable=False)
    
    # Metadata
    is_synthetic = Column(Boolean, default=False)
    source = Column(String)  # 'delfi' or 'gtfs_de'
    match_score = Column(Float, default=0.0)
    last_updated = Column(DateTime, default=datetime.datetime.utcnow)

    # Relationships to raw data
    gtfs_mappings = relationship("GtfsMapping", back_populates="registry")


class GtfsMapping(Base):
    """
    Maps external IDs (like numeric GTFS.DE IDs) to our Canonical ID.
    """
    __tablename__ = 'gtfs_mappings'

    id = Column(Integer, primary_key=True, autoincrement=True)
    canonical_id = Column(String, ForeignKey('station_registry.canonical_id'), index=True)
    external_id = Column(String, nullable=False, index=True)  # e.g. '12345'
    source_feed = Column(String, nullable=False)  # e.g. 'gtfs_de', 'delfi'
    
    registry = relationship("StationRegistry", back_populates="gtfs_mappings")


# --- Raw Data Models (Optimized for Storage) ---

class Trip(Base):
    __tablename__ = 'trips'
    id = Column(String, primary_key=True)
    route_id = Column(String, index=True)
    headsign = Column(String)
    # Storing the "Canonical DNA" as a simple hash or JSON for fast lookups
    stop_sequence_hash = Column(String, index=True) 

class RealtimePosition(Base):
    """
    Live vehicle positions, normalized to Canonical IDs.
    """
    __tablename__ = 'realtime_positions'
    
    id = Column(Integer, primary_key=True, autoincrement=True)
    trip_id = Column(String, ForeignKey('trips.id'), index=True)
    vehicle_id = Column(String, index=True)
    lat = Column(Float)
    lon = Column(Float)
    bearing = Column(Float)
    speed = Column(Float)
    timestamp = Column(DateTime, default=datetime.datetime.utcnow)
    
    # The crucial link: Where is it right now?
    current_stop_canonical_id = Column(String, ForeignKey('station_registry.canonical_id'), nullable=True)
    current_status = Column(String) # 'INCOMING_AT', 'STOPPED_AT', 'IN_TRANSIT_TO'
