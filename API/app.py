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
				dbname="cerbyd_triplogger",
				user='postgres',
				password='x836vzm7dI',
				host='localhost',
				port = 5432
			)
		return conn
	except psycopg2.Error as e:
		logger.error(f"Database connection error: {e}")
		return 


app = FastAPI(title="Cerbyd_Trip_Ingestion_Service")

@app.get("/Cerbyd_Trip_Ingestion_Service")
async def get_trips(
    start_time: str = Query(None, description="Start time for filtering trips (ISO format)"),
    end_time: str = Query(None, description="End time for filtering trips (ISO format)"),
    distance_eq: float = Query(None, description="Minimum distance in kilometers for filtering trips"),
):
    try:
        conn = get_connection()
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

        cur.execute(query, tuple(params))
        trips = [dict(row) for row in cur.fetchall()]
        cur.close()
        conn.close()
        return {"trips": trips}

    except Exception as e:
        logger.error(f"Error executing query: {e}")
        return {"error": str(e)}
