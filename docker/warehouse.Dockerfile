FROM python:3.12.16-slim-bookworm AS runtime
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

# 测试目标单独复制交易夹具；运行镜像不带测试所需代码。
FROM runtime AS tests
COPY ticketing/booking.py ticketing/payment.py ./ticketing/
COPY tests/python/support.py ./tests/support.py
COPY tests/warehouse ./tests/warehouse
ENV PYTHONPATH=/app/tests
ENTRYPOINT ["python", "-m", "unittest"]
CMD ["discover", "-s", "tests/warehouse", "-v"]
