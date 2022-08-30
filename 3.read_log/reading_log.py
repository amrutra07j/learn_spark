from pyspark.sql import SparkSession
from lib.logger import *
from pyspark.sql.functions import regexp_extract, substring_index

if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("log_reader") \
        .master("local[5]") \
        .getOrCreate()

    log = Log4j(spark)

    log_df = spark.read.text("data/apache_logs.txt")
    log_reg = r'^(\S+) (\S+) (\S+) \[([\w:/]+\s[+\-]\d{4})\] "(\S+) (\S+) (\S+)" (\d{3}) (\S+) "(\S+)" "([^"]*)'
    log_df = log_df.select(regexp_extract('value', log_reg, 1).alias('ip'),
                           regexp_extract('value', log_reg, 4).alias('date'),
                           regexp_extract('value', log_reg, 6).alias('request'),
                           regexp_extract('value', log_reg, 10).alias('referrer'))
    log_df.where("trim(referrer) != '-'").withColumn("referrer", substring_index("referrer", "/", 3)).groupBy("referrer").count().show(100)
    input("enter: ")
