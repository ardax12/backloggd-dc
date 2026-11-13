FROM python:3.12-slim

# Set working directory
WORKDIR /app

# Set Python environment flags
ENV PYTHONDONTWRITEBYTECODE=1 \
    PYTHONUNBUFFERED=1

# Install locked Python dependencies using prebuilt wheels (no compiler required)
COPY requirements.lock .
RUN pip install --no-cache-dir --no-deps --only-binary=:all: -r requirements.lock \
    && groupadd --system --gid 10001 app \
    && useradd --system --uid 10001 --gid app --home-dir /app --shell /usr/sbin/nologin app

# Copy project files
COPY . .

# Ensure default SQLite storage directory is writable by non-root runtime user
RUN mkdir -p /app/data \
    && chown 10001:10001 /app/data

# Drop root privileges
USER app

# Run bot
CMD ["python", "main.py"]
