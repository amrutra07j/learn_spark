from pyspark.sql import SparkSession

if __name__ == "__main__":
    print("Sky is not the limit")
    spark = SparkSession.builder \
            .appName("sink") \
            .master("local[3]") \
            .getOrCreate()
    par_df = spark.read \
                .format("parquet") \
                .load("data/flight-time.parquet")
    print(par_df.rdd.getNumPartitions())
    par_df.write \
        .format("parquet") \
        .mode("overwrite") \
        .option("path", "written/parquet/") \
        .save()
    input("enter: ")