import re

from keyring.backends import null
from pyspark.sql import SparkSession
from pyspark.sql.functions import col, udf
from pyspark.sql.types import StringType


def gender_udf(gender):
    male_reg = r'^m$|m.l|ma'
    female_reg = r'^f$|f.m|w.m'

    if re.search(male_reg, gender.lower()):
        gender = 'Female'
    elif re.search(female_reg, gender.lower()):
        gender = 'Male'
    else:
        gender = 'Unknown'
    return gender


if __name__ == "__main__":
    spark = SparkSession.builder \
        .appName("udf") \
        .master("local[3]") \
        .getOrCreate()

    csv_df = spark.read \
        .format("csv") \
        .option("inferSchema", "true") \
        .option("header", "true") \
        .load("data/survey.csv")

    rename_gender = udf(gender_udf, returnType=StringType())

    csv_df = csv_df.withColumn('Gender', rename_gender(col("Gender"))) \
        .select("Gender").groupBy("Gender").count()
    csv_df.show()
    spark.udf.register('parse_gender', gender_udf, StringType())
    input("enter: ")
