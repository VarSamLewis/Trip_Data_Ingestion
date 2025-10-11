import os
from fastapi import FastAPI, Query
import psycopg2
import psycopg2.extras
import logging
from prometheus_client import Counter, Histogram, generate_latest, CONTENT_TYPE_LATEST
from fastapi.responses import Response
import time

# Metrics
REQUEST_COUNT = Counter('http_requests_total', 'Total HTTP requests', ['method', 'endpoint'])
REQUEST_DURATION = Histogram('http_request_duration_seconds', 'HTTP request duration')
DB_QUERIES = Counter('database_queries_total', 'Total database queries', ['status'])

logger = logging.getLogger("cerbyd_triplogger")
logging.basicConfig(level=logging.INFO,
                    format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')

def get_connection():
    try:
        database_url = os.getenv("DATABASE_URL")
        if database_url:
            conn = psycopg2.connect(database_url, sslmode='require')
        else:
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

app = FastAPI(title="Cerbyd_Trip_Ingestion_Service")

@app.middleware("http")
async def metrics_middleware(request, call_next):
    start_time = time.time()
    response = await call_next(request)
    REQUEST_COUNT.labels(method=request.method, endpoint=request.url.path).inc()
    REQUEST_DURATION.observe(time.time() - start_time)
    return response

@app.get("/metrics")
async def metrics():
    return Response(generate_latest(), media_type=CONTENT_TYPE_LATEST)

@app.get("/")
async def root():
    return {"message": "Cerbyd Trip Ingestion Service", "status": "running"}

@app.get("/health")
async def health_check():
    try:
        conn = get_connection()
        if conn:
            conn.close()
            DB_QUERIES.labels(status='success').inc()
            return {"status": "healthy", "database": "connected"}
        DB_QUERIES.labels(status='failed').inc()
        return {"status": "unhealthy", "database": "disconnected"}
    except Exception as e:
        logger.error(f"Health check error: {e}")
        DB_QUERIES.labels(status='error').inc()
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
        if not conn:
            DB_QUERIES.labels(status='connection_failed').inc()
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
        
        DB_QUERIES.labels(status='success').inc()
        logger.info(f"Retrieved {len(trips)} trips")
        return {"trips": trips}

    except Exception as e:
        logger.error(f"Error executing query: {e}")
        DB_QUERIES.labels(status='error').inc()
        return {"error": str(e)}
