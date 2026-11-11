"""本地 SparkSession；业务加工显式使用 UTC、Decimal 溢出报错和较小分区。"""


def create_session(name):
    from pyspark.sql import SparkSession

    spark = (SparkSession.builder.master('local[2]').appName(name)
             .config('spark.ui.enabled', 'false').config('spark.sql.session.timeZone', 'UTC')
             .config('spark.sql.ansi.enabled', 'true')
             .config('spark.sql.parquet.outputTimestampType', 'TIMESTAMP_MICROS')
             .config('spark.sql.shuffle.partitions', '2')
             .config('spark.driver.bindAddress', '127.0.0.1').config('spark.driver.host', '127.0.0.1')
             .getOrCreate())
    spark.sparkContext.setLogLevel('ERROR')
    return spark
