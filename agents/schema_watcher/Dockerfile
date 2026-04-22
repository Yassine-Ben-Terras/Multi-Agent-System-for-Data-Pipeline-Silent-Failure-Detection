FROM python:3.11-slim
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY config/ config/
COPY agents/__init__.py agents/__init__.py
COPY agents/schema_watcher/ agents/schema_watcher/
RUN useradd -m -u 1000 mas && chown -R mas:mas /app
USER mas
CMD ["python", "-m", "agents.schema_watcher.main"]
