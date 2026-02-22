from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import yaml
import os

from utils.spark_path import get_latest_year_month_path, get_current_year_month_path

# ============================================================
# Spark Session
# ============================================================
spark = (
    SparkSession.builder
    .appName("silver_clean_building")
    .master("spark://spark-master:7077")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.shuffle.partitions", "200")
    .getOrCreate()
)

# ============================================================
# Config
# ============================================================
CONFIG_PATH = "/opt/spark/jobs/config.yaml"

with open(CONFIG_PATH, "r", encoding="utf-8") as f:
    cfg = yaml.safe_load(f)

ROOT = cfg["data_lake"]["root"]
LAYERS = cfg["data_lake"]["layers"]

# input (bronze)
building_src_base = os.path.join(
    ROOT, LAYERS["bronze"]["domains"]["buildingLeader"]["paths"]["parquet"]
)
building_src_path = get_latest_year_month_path(spark, building_src_base)

# output (silver clean)
building_clean_base = os.path.join(
    ROOT, LAYERS["silver"]["clean"]["domains"]["building_clean"]["paths"]["parquet"]
)
building_clean_path = get_current_year_month_path(building_clean_base)

partition_cols = LAYERS["silver"]["clean"]["domains"]["building_clean"].get(
    "partition", ["region"]
)

# ============================================================
# LOAD
# ============================================================
building_df = spark.read.parquet(building_src_path)

# ============================================================
# CLEANING
# ============================================================
building_clean_df = (
    building_df
    .withColumn(
        "고유번호",
        F.concat(
            F.col("시군구_코드"),
            F.col("법정동_코드"),
            F.when(F.col("대지_구분_코드") == "0", F.lit("1"))
             .when(F.col("대지_구분_코드") == "1", F.lit("2"))
             .otherwise(F.col("대지_구분_코드")),
            F.lpad(F.col("번"), 4, "0"),
            F.lpad(F.col("지"), 4, "0")
        )
    )
    .select(
        F.col("관리_건축물대장_PK"),
        F.col("고유번호"),
        F.col("대장_구분_코드"),
        F.col("건축_면적(㎡)").alias("건축면적").cast("double"),
        F.col("옥외_자주식_면적(㎡)").alias("옥외자주식면적").cast("double"),
        F.col("region")
    )
)

# ============================================================
# SAVE
# ============================================================
(
    building_clean_df
    .write
    .mode("overwrite")
    .partitionBy(*partition_cols)
    .parquet(building_clean_path)
)

spark.stop()
