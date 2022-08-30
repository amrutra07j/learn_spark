from pyspark.sql import SparkSession
from pyspark.sql.functions import spark_partition_id

from lib.logger import Log4j

if __name__ == "__main__":
    print("Sky is not the limit")
    spark = SparkSession.builder \
        .appName("sink") \
        .master("local[3]") \
        .getOrCreate()

    logger = Log4j(spark)

    par_df = spark.read \
        .format("parquet") \
        .load("data/flight-time.parquet")



    # par_df.write \
    #     .format("avro") \
    #     .mode("overwrite") \
    #     .option("path", "written/avro/") \
    #     .save()

    par_df.write \
        .format("json") \
        .mode("overwrite") \
        .partitionBy("OP_CARRIER", "ORIGIN") \
        .option("path", "written/json/") \
        .option("maxRecordsPerFile", 10000) \
        .save()
    input("enter: ")
