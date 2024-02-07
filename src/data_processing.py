from pyspark.sql import SparkSession, DataFrame
from pyspark.sql.functions import col

def load_data(file_path: str) -> DataFrame:
    spark = (SparkSession.builder
             .master("local[*]")
             .appName("Data Processing")
             .getOrCreate())
    df = spark.read.csv(file_path, header=True, inferSchema=True)
    return df

def delete_null_columns(df: DataFrame) -> DataFrame:
    for column in df.columns:
        if df.filter(col(column).isNotNull()).count() == 0:
            df = df.drop(column)
    return df

