from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.types import StructType, StructField, StringType
from pyspark.sql.functions import col, from_json
from src import data_processing as dp

api_key = "7VXUSLXTEGN5JVXA"
api_secret = "raLAAcKn+MLUI6shCBbBQ8gq/w80mpxmPdxKZp2JaGHhrcBs/++qw2vGdnsFf1tW"

def process_end_show(df: DataFrame, batch_id):
    new_df = dp.delete_null_columns(df)
    new_df.show(5)
    return new_df

def read_kafka_stream():
    spark = (SparkSession
            .builder
            .appName("Kafka Weather Test")
            .getOrCreate())

    df = (spark.readStream
           .format("kafka")
           .option("kafka.bootstrap.servers", "pkc-4r087.us-west2.gcp.confluent.cloud:9092")
           .option("subscribe", "tomsk_weather")
           .option("kafka.sasl.jaas.config", f'org.apache.kafka.common.security.plain.PlainLoginModule required username="{api_key}" password="{api_secret}";')
           .option("kafka.security.protocol", "SASL_SSL")
           .option("kafka.sasl.mechanism", "PLAIN")
           .load())

    schema = StructType([
        StructField("date", StringType(), True),
        StructField("tavg", StringType(), True),
        StructField("tmin", StringType(), True),
        StructField("tmax", StringType(), True),
        StructField("prcp", StringType(), True),
        StructField("snow", StringType(), True),
        StructField("wdir", StringType(), True),
        StructField("wspd", StringType(), True),
        StructField("wpgt", StringType(), True),
        StructField("pres", StringType(), True),
        StructField("tsun", StringType(), True)
    ])

    df = df.selectExpr("CAST(value AS STRING)")
    df = df.select(from_json(col("value"), schema).alias("data"))
    df = df.select("data.*")

    return df

df = read_kafka_stream()
df = df.writeStream.foreachBatch(process_end_show).start()