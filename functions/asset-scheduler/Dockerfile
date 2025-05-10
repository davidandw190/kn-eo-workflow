FROM python:3.10-slim
RUN apt-get update && apt-get install -y --no-install-recommends \
    libsqlite3-0 \
    && rm -rf /var/lib/apt/lists/*
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt
COPY func.py .
ENV PYTHONUNBUFFERED=1
CMD ["python", "-m", "parliament", "."]