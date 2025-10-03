FROM python:3.11-slim

WORKDIR /app

# Install system dependencies
RUN apt-get update && apt-get install -y \
    gcc \
    && rm -rf /var/lib/apt/lists/*

# Copy requirements
COPY DB/requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy DB scripts
COPY DB/ ./DB/

# Set working directory to DB
WORKDIR /app/DB

# Command to run table creation and then data formatting
CMD ["sh", "-c", "python create_table.py && python format_data.py"]