FROM python:3.12.16-slim-bookworm
ENV PYTHONDONTWRITEBYTECODE=1 PYTHONUNBUFFERED=1 \
    SPARK_LOCAL_IP=127.0.0.1 WAREHOUSE_DATA_DIR=/warehouse-data
RUN apt-get update \
    && apt-get install -y --no-install-recommends openjdk-17-jre-headless \
    && rm -rf /var/lib/apt/lists/* \
    && groupadd --gid 10001 warehouse \
    && useradd --uid 10001 --gid 10001 --create-home --shell /usr/sbin/nologin warehouse \
    && mkdir -p /warehouse-data \
    && chown 10001:10001 /warehouse-data
WORKDIR /app
COPY requirements-warehouse.txt ./
RUN pip install --no-cache-dir -r requirements-warehouse.txt
COPY warehouse ./warehouse
COPY ticketing/__init__.py ticketing/db.py ticketing/bootstrap.py ./ticketing/
COPY migrations ./migrations
USER 10001:10001
ENTRYPOINT ["python", "-m", "warehouse"]
CMD ["--help"]
