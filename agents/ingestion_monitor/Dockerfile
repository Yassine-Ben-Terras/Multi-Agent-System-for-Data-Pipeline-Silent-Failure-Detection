FROM python:3.11-slim

WORKDIR /app

# Install dependencies
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Copy source
COPY config/ config/
COPY agents/__init__.py agents/__init__.py
COPY agents/ingestion_monitor/ agents/ingestion_monitor/

# Non-root user for security
RUN useradd -m -u 1000 mas && chown -R mas:mas /app
USER mas

CMD ["python", "-m", "agents.ingestion_monitor.main"]
