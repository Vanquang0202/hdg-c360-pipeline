# Mục đích: Kiểm tra Spark local nạp được cả Oracle và PostgreSQL JDBC driver.
# Đầu vào: Classpath do run_spark.ps1 truyền vào; không cần mật khẩu DB.
# Đầu ra: Log class nạp ở driver/executor; không mở kết nối database.
"""Check local Spark JDBC classpaths without opening database connections."""
import sys
from pathlib import Path
sys.path.insert(0, str(Path(__file__).resolve().parents[2] / "jobs"))
from spark_common import create_spark, log
from pyspark.sql import functions as F


def main():
    # BƯỚC 1: Mở Spark, nạp hai class JDBC ở driver và task executor rồi dừng; không gọi connect DB.
    spark = create_spark("HDG-check-jdbc")
    try:
        drivers = ["oracle.jdbc.OracleDriver", "org.postgresql.Driver"]
        loader = spark.sparkContext._jvm.java.lang.Thread.currentThread().getContextClassLoader()
        for driver in drivers:
            loader.loadClass(driver).newInstance()
            log("jdbc_driver_loaded", driver=driver)
        # Depend on task input so reflection is evaluated in executor tasks.
        tasks = spark.range(2, numPartitions=2).select(
            F.when(F.col("id") == 0, F.lit(drivers[0]))
            .otherwise(F.lit(drivers[1])).alias("driver_class")
        )
        rows = tasks.selectExpr(
            "java_method('java.lang.Class', 'forName', driver_class) AS loaded_class"
        ).collect()
        if {row.loaded_class for row in rows} != {"class " + d for d in drivers}:
            raise RuntimeError("Executor JDBC class loading returned unexpected results")
        distributed_jars = spark.sparkContext._jsc.sc().listJars().size()
        if distributed_jars != 0:
            raise RuntimeError("Unexpected distributed JARs: " + str(distributed_jars))
        log("jdbc_check_complete", executor_classes=[r.loaded_class for r in rows],
            distributed_jars=distributed_jars, database_connections=0)
    finally:
        spark.stop()


if __name__ == "__main__":
    main()
