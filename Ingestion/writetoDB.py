import psycopg2
import os
from generate_data import generate_batch
import logging
import time
import threading 

logger = logging.getLogger("cerbyd_triplogger")
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

def get_connection():
    
    conn = psycopg2.connect(
        dbname="cerbyd_triplogger",
        user="postgres",
        password='x836vzm7dI',
        host="localhost",
        port=5432
    )
    if not conn:
        logger.error("Failed to connect to the database.")
        raise Exception("Database connection failed")

    logger.info("Connected to the database successfully.")
    return conn 

def insert_trip_events(events):
    conn = get_connection()

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
    cur.close()
    conn.close()
    print(f"✅ Trip {len(events)} events inserted successfully.")

def flush_cache():
    conn = get_connection()
    cur = conn.cursor()
    cur.execute("TRUNCATE TABLE trip_events_cache")
    conn.commit()
    cur.close()
    conn.close()
    print("✅ Cache flushed successfully.")

def gen_insert_cache():
    for _ in range(90):  
        batch = generate_batch(2)
        insert_trip_events(batch)
        time.sleep(1)

def write_to_db():
    while True:
        time.sleep(30)
        cachetoperm()  # Validate & transfer
        flush_cache()
        print("✅ Cache flushed to permanent table.")

if __name__ == "__main__":
    t1 = threading.Thread(target=generate_and_insert)
    t2 = threading.Thread(target=flush_loop)

    t1.start()
    t2.start()

    t1.join()
    t2.join()
