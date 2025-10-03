import psycopg2
import os

# Use Docker service name 'db' as host when running from another container
# Use 'localhost' when running from host machine
host = os.getenv('DB_HOST', 'localhost')

conn = psycopg2.connect(
    dbname="cerbyd_triplogger",
    user='postgres',
    password='x836vzm7dI',
    host=host,
    port=5432
)

with conn.cursor() as cur:
    cur.execute("""
        -- Enable UUID extension
        CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
        
        -- trips_events
        CREATE UNLOGGED TABLE IF NOT EXISTS trip_events_cache (
            event_id SERIAL PRIMARY KEY,
            trip_id TEXT NOT NULL,
            rider_id TEXT NOT NULL,
            driver_id TEXT NOT NULL,
            timestamp TIMESTAMP NOT NULL,
            location_lat DOUBLE PRECISION NOT NULL,
            location_lon DOUBLE PRECISION NOT NULL,
            address TEXT NOT NULL,
            status TEXT CHECK (status IN ('in_progress', 'completed', 'cancelled')),
            checkpoint TEXT CHECK (checkpoint IN ('pickup', 'dropoff', 'cancelled'))
        );

        CREATE TABLE IF NOT EXISTS trip_events (
            event_id SERIAL PRIMARY KEY,
            trip_id TEXT NOT NULL,
            rider_id TEXT NOT NULL,
            driver_id TEXT NOT NULL,
            timestamp TIMESTAMP NOT NULL,
            location_lat DOUBLE PRECISION NOT NULL,
            location_lon DOUBLE PRECISION NOT NULL,
            address TEXT NOT NULL,
            status TEXT,
            checkpoint TEXT
        );
        
        -- trips table (main lifecycle record)
        CREATE TABLE IF NOT EXISTS trips (
            trip_id UUID PRIMARY KEY,
            start_time TIMESTAMP NOT NULL,
            end_time TIMESTAMP,
            pickup JSONB,
            dropoff JSONB,
            distance_km DOUBLE PRECISION,
            status TEXT CHECK (status IN ('started', 'completed', 'cancelled')),
            route JSONB
        );
        
        CREATE TABLE IF NOT EXISTS trip_tracker (
            trip_id UUID PRIMARY KEY,
            is_complete BOOLEAN DEFAULT FALSE,
            flushed_at TIMESTAMP
        );
        
        -- Create indexes for better performance
        CREATE INDEX IF NOT EXISTS idx_trip_events_cache_trip_id ON trip_events_cache(trip_id);
        CREATE INDEX IF NOT EXISTS idx_trip_events_cache_timestamp ON trip_events_cache(timestamp);
        CREATE INDEX IF NOT EXISTS idx_trip_events_trip_id ON trip_events(trip_id);
        CREATE INDEX IF NOT EXISTS idx_trips_start_time ON trips(start_time);
        CREATE INDEX IF NOT EXISTS idx_trips_status ON trips(status);
    """)

conn.commit()
conn.close()
print("✅ Tables created successfully.")