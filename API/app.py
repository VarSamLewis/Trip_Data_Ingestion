import os
from fastapi import FastAPI, Query
import psycopg2
import psycopg2.extras
import logging


logger = logging.getLogger("cerbyd_triplogger")
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

def get_connection():
    try:
        conn = psycopg2.connect(
            dbname=os.getenv("DB_NAME", "cerbyd_triplogger"),
            user=os.getenv("DB_USER", "postgres"),
            password=os.getenv("DB_PASSWORD", "x836vzm7dI"),
            host=os.getenv("DB_HOST", "db"),  # Use 'db' service name for Docker
            port=int(os.getenv("DB_PORT", "5432"))
        )
        return conn
    except psycopg2.Error as e:
        logger.error(f"Database connection error: {e}")
        return None  # Explicitly return None


app = FastAPI(title="Cerbyd_Trip_Ingestion_Service")

@app.get("/")
async def root():
    return {"message": "Cerbyd Trip Ingestion Service", "status": "running"}

@app.get("/health")
async def health_check():
    try:
        conn = get_connection()
        if conn:
            conn.close()
            return {"status": "healthy", "database": "connected"}
        return {"status": "unhealthy", "database": "disconnected"}
    except Exception as e:
        logger.error(f"Health check error: {e}")
        return {"status": "unhealthy", "error": str(e)}

@app.get("/Cerbyd_Trip_Ingestion_Service")
async def get_trips(
    start_time: str = Query(None, description="Start time for filtering trips (ISO format)"),
    end_time: str = Query(None, description="End time for filtering trips (ISO format)"),
    distance_eq: float = Query(None, description="Minimum distance in kilometers for filtering trips"),
    number_of_trips: int = Query(10, description="Number of trips to return"),
):
    try:
        conn = get_connection()
        if not conn:  # Check if connection is None
            return {"error": "Database connection failed"}
        
        cur = conn.cursor(cursor_factory=psycopg2.extras.DictCursor)

        query = "SELECT * FROM trips WHERE TRUE"
        params = []

        if start_time:
            query += " AND start_time >= %s"
            params.append(start_time)

        if end_time:
            query += " AND end_time <= %s"
            params.append(end_time)

        if distance_eq:
            query += " AND ROUND(distance_km) = %s"
            params.append(distance_eq)

        if number_of_trips:
            query += " LIMIT %s"
            params.append(number_of_trips)

        cur.execute(query, tuple(params))
        trips = [dict(row) for row in cur.fetchall()]
        cur.close()
        conn.close()
        return {"trips": trips}

    except Exception as e:
        logger.error(f"Error executing query: {e}")
        return {"error": str(e)}
