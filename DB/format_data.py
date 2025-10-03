import psycopg2
import logging 
import os

logger = logging.getLogger("cerbyd_triplogger")
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')


def get_connection():
    try:
        conn = psycopg2.connect(
            dbname=os.getenv("DB_NAME", "cerbyd_triplogger"),
            user=os.getenv("DB_USER", "postgres"),
            password=os.getenv("DB_PASSWORD", "x836vzm7dI"),
            host=os.getenv("DB_HOST", "db"),
            port=int(os.getenv("DB_PORT", "5432"))
        )
        return conn
    except psycopg2.Error as e:
        logger.error(f"Database connection error: {e}")
        return None


def writetotrips():
    conn = get_connection()
    if not conn:
        return
    
    cur = conn.cursor()
    cur.execute("""
        INSERT INTO trips (trip_id, start_time, end_time, pickup, dropoff, distance_km, status, route)
        SELECT DISTINCT
            trip_id::UUID,
            MIN(timestamp) AS start_time,
            MAX(timestamp) AS end_time,
            JSONB_AGG(CASE WHEN checkpoint = 'pickup' THEN JSONB_BUILD_OBJECT(
                'lat', location_lat,
                'lon', location_lon,
                'address', address
            ) END) AS pickup,
            JSONB_AGG(CASE WHEN checkpoint = 'dropoff' THEN JSONB_BUILD_OBJECT(
                'lat', location_lat,
                'lon', location_lon,
                'address', address
            ) END) AS dropoff,
            RANDOM() * (50-2) + 2 AS distance_km,
            CASE 
                WHEN MAX(status) = 'cancelled' THEN 'cancelled'
                ELSE 'completed'
            END AS status,
            JSONB_BUILD_ARRAY(
                (SELECT JSONB_BUILD_OBJECT('lat', location_lat, 'lon', location_lon)
                 FROM trip_events_cache e1
                 WHERE e1.trip_id = trip_events_cache.trip_id AND checkpoint = 'pickup'
                 LIMIT 1),
         
                (SELECT JSONB_BUILD_OBJECT('lat', location_lat, 'lon', location_lon)
                 FROM trip_events_cache e2
                 WHERE e2.trip_id = trip_events_cache.trip_id AND checkpoint = 'dropoff'
                 LIMIT 1)
            ) AS route
            FROM trip_events_cache
            GROUP BY trip_id
            ON CONFLICT (trip_id) DO NOTHING

        """)
    conn.commit()
    cur.close()
    conn.close()


def main():
    try: 
        writetotrips()
        logger.info("Data successfully written to trips table.")
    except Exception as e:
        logger.error(f"Error writing data to trips table: {e}")


if __name__ == "__main__":
    main()
    logger.info("Cerbyd trip logger data formatting complete.")