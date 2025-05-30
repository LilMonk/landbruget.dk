# Build stage
FROM ghcr.io/osgeo/gdal:ubuntu-small-3.10.0 as builder

# Install system dependencies
RUN apt-get update && apt-get install -y \
    python3 \
    python3-dev \
    python3-pip \
    python3-venv \
    python3-setuptools \
    gcc \
    g++ \
    procps \
    libgdal-dev \
    && rm -rf /var/lib/apt/lists/*

# Create and use virtual environment
RUN python3 -m venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

# Set GDAL environment variables
ENV GDAL_VERSION=3.10.0
ENV CPLUS_INCLUDE_PATH=/usr/include/gdal
ENV C_INCLUDE_PATH=/usr/include/gdal

# Install dependencies in order of change frequency
WORKDIR /install

# 1. Install build tools (rarely changes)
RUN pip install --no-cache-dir setuptools wheel

# 2. Install core dependencies (changes occasionally)
RUN pip install --no-cache-dir \
    numpy>=1.26.4 \
    pandas>=2.2.0 \
    shapely>=2.0.0 \
    geopandas>=0.14.0 \
    pyproj>=3.0.0 \
    fiona>=1.9.0

# 3. Install API dependencies (changes occasionally)
RUN pip install --no-cache-dir \
    fastapi>=0.100.0 \
    uvicorn>=0.15.0 \
    pydantic>=2.0.0

# 4. Copy requirements and install remaining deps (changes most frequently)
COPY requirements.txt .
RUN pip install --no-cache-dir -r requirements.txt

# Final stage
FROM ghcr.io/osgeo/gdal:ubuntu-small-3.10.0

RUN apt-get update && apt-get install -y \
    python3 \
    python3-venv \
    procps \
    && rm -rf /var/lib/apt/lists/*

COPY --from=builder /opt/venv /opt/venv
ENV PATH="/opt/venv/bin:$PATH"

WORKDIR /app
COPY . .

CMD ["uvicorn", "app.main:app", "--host", "0.0.0.0", "--port", "8080"]
