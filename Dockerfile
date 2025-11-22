FROM python:3.10-slim
WORKDIR /app

# Install build deps
RUN apt-get update && apt-get install -y --no-install-recommends \
    build-essential \
    && rm -rf /var/lib/apt/lists/*

# Copy project files
COPY . /app

# Install runtime requirements and dev tools
RUN python -m pip install --upgrade pip
RUN pip install -r requirements.txt || true
RUN pip install -r requirements-dev.txt || true

# Install package editable
RUN pip install -e .

CMD ["pytest", "-q"]
