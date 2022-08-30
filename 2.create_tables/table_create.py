from pyspark.sql import SparkSession

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("create_table") \
        .master("local[5]") \
        .enableHiveSupport() \
        .getOrCreate()

    spark.sql("create database if not exists flight_db")
    spark.catalog.setCurrentDatabase("flight_db")

    par_df = spark.read \
        .format("parquet") \
        .load("data/flight-time.parquet")

    par_df.write \
        .format("csv") \
        .mode("overwrite") \
        .bucketBy(5, ["OP_CARRIER", "ORIGIN"]) \
        .sortBy("OP_CARRIER", "ORIGIN") \
        .saveAsTable("flight_details")
    print(spark.catalog.listTables("flight_db"))
    input("enter: ")
