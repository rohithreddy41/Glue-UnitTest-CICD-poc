# src/etl_job.py
from pyspark.sql import DataFrame, SparkSession
from pyspark.sql.functions import col, upper, when

def extract(spark: SparkSession) -> DataFrame:
    data = [("alice", 17), ("bob", 25), ("carol", None)]
    return spark.createDataFrame(data, ["name", "age"])

def transform(df: DataFrame) -> DataFrame:
    return (
        df.filter(col("age").isNotNull())
          .withColumn("name", upper(col("name")))
          .withColumn("is_adult", when(col("age") >= 18, True).otherwise(False))
    )

def load(df: DataFrame, path: str):
    df.write.mode("overwrite").parquet(path)

def run_etl(spark: SparkSession, output_path: str) -> DataFrame:
    df_raw = extract(spark)
    df_transformed = transform(df_raw)
    load(df_transformed, output_path)
    return df_transformed
