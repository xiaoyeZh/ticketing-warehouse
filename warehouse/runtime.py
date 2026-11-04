"""离线样例检查 Java、Spark、Decimal、时区与 Parquet 的互操作。"""
from datetime import datetime, timezone
from decimal import Decimal
from importlib.metadata import version
import os
from pathlib import Path
import subprocess
import tempfile


def check_environment():
    import pyarrow as pa
    import pyarrow.parquet as pq
    from pyspark.sql import SparkSession, functions as F

    java = subprocess.run(['java', '-version'], capture_output=True, text=True, check=True, timeout=10)
    directory = Path(os.environ.get('WAREHOUSE_DATA_DIR', '/warehouse-data'))
    directory.mkdir(parents=True, exist_ok=True)
    spark = (SparkSession.builder.master('local[2]').appName('ticketing-warehouse-environment-check')
             .config('spark.ui.enabled', 'false').config('spark.sql.session.timeZone', 'UTC')
             .config('spark.sql.parquet.outputTimestampType', 'TIMESTAMP_MICROS')
             .config('spark.sql.shuffle.partitions', '2')
             .config('spark.driver.bindAddress', '127.0.0.1').config('spark.driver.host', '127.0.0.1')
             .getOrCreate())
    spark.sparkContext.setLogLevel('ERROR')
    try:
        with tempfile.TemporaryDirectory(prefix='environment-check-', dir=directory) as temporary:
            root = Path(temporary)
            schema = pa.schema([('id', pa.int64()), ('event_at', pa.timestamp('us', tz='UTC')),
                                ('amount', pa.decimal128(18, 2))])
            table = pa.Table.from_pylist([
                {'id': 1, 'event_at': datetime(2026, 11, 3, 15, 59, tzinfo=timezone.utc), 'amount': Decimal('100.10')},
                {'id': 2, 'event_at': datetime(2026, 11, 3, 16, 0, tzinfo=timezone.utc), 'amount': Decimal('0.00')},
            ], schema=schema)
            pq.write_table(table, root / 'input.parquet')
            frame = spark.read.parquet(str(root / 'input.parquet'))
            rows = (frame.withColumn('business_date', F.to_date(F.from_utc_timestamp('event_at', 'Asia/Shanghai')))
                    .orderBy('id').collect())
            if [str(row.business_date) for row in rows] != ['2026-11-03', '2026-11-04']:
                raise RuntimeError('上海业务日期转换检查失败')
            total = frame.agg(F.sum('amount').alias('amount')).first().amount
            if total != Decimal('100.10') or not isinstance(total, Decimal):
                raise RuntimeError('Decimal 金额检查失败')
            # 真正执行 Python worker，检查 Python 与 Java 之间的运行链路。
            if spark.sparkContext.parallelize([1, 2], 2).map(lambda n: n * 2).collect() != [2, 4]:
                raise RuntimeError('Spark Python worker 检查失败')
            frame.write.parquet(str(root / 'output'))
            restored = pq.read_table(root / 'output').sort_by('id').cast(schema)
            if restored.to_pylist() != table.to_pylist():
                raise RuntimeError('Spark 与 PyArrow 的 Parquet 往返检查失败')
        return {'status': 'passed', 'spark_master': spark.sparkContext.master,
                'java': (java.stderr or java.stdout).splitlines()[0],
                'versions': {name: version(name) for name in ('pyspark', 'py4j', 'pyarrow', 'psycopg')},
                'checks': ['decimal', 'Asia/Shanghai date boundary', 'Parquet round trip', 'Python worker'],
                'business_data_accessed': False}
    finally:
        spark.stop()
