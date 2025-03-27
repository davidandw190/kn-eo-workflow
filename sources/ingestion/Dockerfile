FROM python:3.11-slim-bullseye
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1 \
    PYTHONHASHSEED=random \
    PIP_NO_CACHE_DIR=1 \
    PIP_DISABLE_PIP_VERSION_CHECK=1
RUN groupadd -r appuser && useradd -r -g appuser appuser
WORKDIR /app
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt && \
    rm -rf /root/.cache/pip
COPY app.py .
RUN sed -i 's/\r$//' app.py && \
    chmod +x app.py && \
    sed -i '1s|^#!.*|#!/usr/bin/env python3|' app.py

RUN chown -R appuser:appuser /app
USER appuser
CMD ["python", "app.py"]