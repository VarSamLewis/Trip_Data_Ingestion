import psycopg2
from psycopg2.pool import ThreadedConnectionPool
from generate_data import generate_batch
import logging
import time
import threading

logger = logging.getLogger("cerbyd_triplogger")
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

db_pool = ThreadedConnectionPool(
    minconn=1,
    maxconn=10,
    dbname="cerbyd_triplogger",
    user="postgres",
    password="x836vzm7dI",
    host="localhost",
    port=5432
)

def get_connection():
    conn = db_pool.getconn()
    if not conn:
        logger.error("Failed to get connection from pool.")
        raise Exception("No available database connections.")
    logger.info("Connected to the database.")
    return conn

def return_connection(conn):
    db_pool.putconn(conn)

def insert_trip_events(events):
    conn = get_connection()
    try:
        cur = conn.cursor()

        for event in events:
            loc = event["location"]
            cur.execute("""
                INSERT INTO trip_events_cache (
                    trip_id, rider_id, driver_id, timestamp,
                    location_lat, location_lon, address, status, checkpoint
                )
                VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s)
            """,(
                event["trip_id"],
                event["rider_id"],
                event["driver_id"],
                event["timestamp"],
                loc["lat"],
                loc["lon"],
                loc["address"],
                event["status"],
                event["checkpoint"]
            ))

        conn.commit()
    finally:
        cur.close()
        return_connection(conn)
    print(f"✅ Trip {len(events)} events inserted successfully.")


def write_to_tracker():
    conn = get_connection()
    try:
        cur = conn.cursor()

        cur.execute("""
            INSERT INTO trip_tracker (trip_id, is_complete, flushed_at)
            SELECT 
            trip_id::UUID, 
            TRUE AS is_complete,
            NOW() AS flushed_at
            FROM trip_events_cache
            GROUP BY trip_id
            HAVING COUNT(*) = 2
            ON CONFLICT (trip_id) DO NOTHING
        """)
        tracker = cur.rowcount
        conn.commit()
    finally:
        cur.close()
        return_connection(conn)
    print(f"✅ {tracker} Trip tracker updated with completed trips.")

def cache_to_perm():
    conn = get_connection()
    try:
        cur = conn.cursor()
        cur.execute("""
            INSERT INTO trip_events (
                trip_id, rider_id, driver_id, timestamp,
                location_lat, location_lon, address, status, checkpoint
            )
            SELECT 
                trip_events_cache.trip_id::UUID,
                rider_id, driver_id, timestamp,
                location_lat, location_lon, address, status, checkpoint
            FROM trip_events_cache
            INNER JOIN trip_tracker
                ON trip_events_cache.trip_id::UUID = trip_tracker.trip_id
            WHERE trip_tracker.is_complete = TRUE
              AND trip_tracker.flushed_at IS NULL
        """)

        cur.execute("""
            DELETE FROM trip_events_cache
            WHERE trip_id::UUID IN (
                SELECT trip_id FROM trip_tracker WHERE flushed_at IS NULL
            )
        """)

        cur.execute("""
            UPDATE trip_tracker
            SET flushed_at = NOW()
            WHERE flushed_at IS NULL AND is_complete = TRUE
        """)
        flush_count = cur.rowcount
        conn.commit()
    finally:
        cur.close()
        return_connection(conn)
    print(f"✅ {flush_count} Cache data transferred to permanent table.")

def ingest_loop():
    start = time.time()
    while time.time() - start < 60:
        batch = generate_batch(10)
        insert_trip_events(batch)
        time.sleep(1)

def tracker_loop():
    while True:
        write_to_tracker()
        time.sleep(5)

def flush_loop():
    while True:
        cache_to_perm()
        time.sleep(30)


if __name__ == "__main__":
    t1 = threading.Thread(target=ingest_loop)
    t2 = threading.Thread(target=tracker_loop)
    t3 = threading.Thread(target=flush_loop)

    t1.start()
    t2.start()
    t3.start()

    t1.join()
    t2.join()
    t3.join()

    
    db_pool.closeall()