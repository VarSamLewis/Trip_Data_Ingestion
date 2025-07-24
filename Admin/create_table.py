import psycopg2
import os

conn = psycopg2.connect(
	dbname="cerbyd_triplogger",
	user='postgres',
	password='x836vzm7dI',
	host='localhost',
	port = 5432
)


with conn.cursor() as cur:
    cur.execute("""
        -- -- trips_events
        CREATE UNLOGGED TABLE IF NOT EXISTS trip_events_cache (
            event_id SERIAL PRIMARY KEY,
            trip_id TEXT NOT NULL,
            rider_id TEXT NOT NULL,
            driver_id TEXT NOT NULL,
            timestamp TIMESTAMP NOT NULL,
            location_lat DOUBLE PRECISION NOT NULL,
            location_lon DOUBLE PRECISION NOT NULL,
            address TEXT NOT NULL,
            status TEXT CHECK (status IN ('started', 'completed', 'cancelled')),
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
        CREATE TABLE trip_tracker (
            trip_id UUID PRIMARY KEY,
            is_complete BOOLEAN DEFAULT FALSE,
            flushed_at TIMESTAMP
        );
   
    """)

conn.commit()
cur.close()
conn.close()
print("✅ Tables created successfully.")