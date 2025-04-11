FROM python:3.9-slim

WORKDIR /app

# # Install SQLite and other required system libraries
# RUN apt-get update && apt-get install -y \
#     sqlite3 \
#     libsqlite3-dev \
#     && rm -rf /var/lib/apt/lists/*

# Copy your function code and requirements
COPY requirements.txt .
RUN pip install --no-cache-dir --upgrade pip && \
    pip install --no-cache-dir -r requirements.txt && \
    rm -rf /root/.cache/pip

COPY func.py .

# Install additional required packages
RUN pip install --no-cache-dir functions-framework==3.* cloudevents==1.11.0 parliament-functions==0.1.0

# Set environment variables for better Python behavior
ENV PYTHONUNBUFFERED=1 \
    PYTHONDONTWRITEBYTECODE=1

# Create a non-root user to run the app
RUN useradd -m appuser
USER appuser





# Use Functions Framework to start the server
CMD ["functions-framework", "--target=main", "--signature-type=http"]