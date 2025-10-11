# Trip_Data_Ingestion
## How to run 

# Terminal 1: Start PostgreSQL
net start postgresql-x64-15
# Or
pg_ctl start -D "C:\Program Files\PostgreSQL\15\data"

# Terminal 2: Start Data Ingestion
cd Ingestion
python writetoDB.py

# Terminal 3: Start API
cd API
uvicorn app:app --host 127.0.0.1 --port 8000 --reload


# 1. Navigate to project directory
cd Trip_Data_Ingestion

# 2. Create observability configuration directories
mkdir -p observability/{prometheus,loki,promtail,tempo,grafana/{provisioning/{datasources,dashboards},dashboards}}

# 3. Build and start all services
docker-compose up --build -d

# 4. Check all services are running
docker-compose ps

# 1. Copy table creation script to API container
docker cp DB/create_table.py cerbyd_api:/app/create_table.py

# 2. Create database tables
docker-compose exec api python create_table.py

# 3. Verify tables created
docker-compose exec db psql -U postgres -d cerbyd_triplogger -c "\dt"

# View logs for all services
docker-compose logs -f

# View specific service logs
docker-compose logs api
docker-compose logs ingestion
docker-compose logs db

# Check service health
docker-compose ps

# Test API health
curl http://localhost:8000/health

# Test main endpoint
curl "http://localhost:8000/Cerbyd_Trip_Ingestion_Service"

# Test with filters
curl "http://localhost:8000/Cerbyd_Trip_Ingestion_Service?distance_eq=2&number_of_trips=5"#

# Grafana Dashboard
# URL: http://localhost:3000
# Login: admin / admin123

# Prometheus Metrics
# URL: http://localhost:9090

# API Documentation
# URL: http://localhost:8000/docs

# One-command setup
docker-compose up --build -d && \
docker cp DB/create_table.py cerbyd_api:/app/create_table.py && \
docker-compose exec api python create_table.py && \
echo "✅ Setup complete! Testing endpoints..." && \
curl -s http://localhost:8000/health