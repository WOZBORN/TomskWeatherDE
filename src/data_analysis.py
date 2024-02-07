from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import avg, min, max, col, round, asc
from pyspark.sql.types import StringType

def analyze_temperatures(df: DataFrame) -> DataFrame:
    months_temps = (df
                    .withColumn("month", col("date").substr(6, 2).cast("string"))
                    .groupBy("month")
                    .agg(round(avg("tavg"), 1).alias("avg_temp"),
                         min("tmin").alias("min_temp"),
                         max("tmax").alias("max_temp"))
                    .orderBy(asc("month")))
    return months_temps
