from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql import Window
import yaml
import os

from utils.spark_path import get_latest_year_month_path, get_current_year_month_path

# ============================================================
# Spark Session
# ============================================================
spark = (
    SparkSession.builder
    .appName("silver_clean_toji")
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
toji_src_path = get_latest_year_month_path(spark, os.path.join(
    ROOT,
    LAYERS["bronze"]["domains"]["tojiSoyuJeongbo"]["paths"]["parquet"],
))

# output (silver clean)
toji_clean_path = get_current_year_month_path(os.path.join(
    ROOT,
    LAYERS["silver"]["clean"]["domains"]["toji_clean"]["paths"]["parquet"],
))

partition_cols = LAYERS["silver"]["clean"]["domains"]["toji_clean"].get(
    "partition", ["region"]
)

# ============================================================
# LOAD
# ============================================================
toji_df = spark.read.parquet(toji_src_path)

# ============================================================
# CLEANING
# ============================================================
toji_clean_df = (
    toji_df
    .filter(F.col("소유구분코드") == "01")
    .select(
        F.col("고유번호").cast("string"),
        F.col("법정동명"),
        F.col("지번"),
        F.col("지목"),
        F.col("토지면적").cast("double"),
        F.col("공유인수"),
        F.col("소유권변동일자"),
        F.col("region"),
    )
    .withColumn(
        "소유권변동일자_dt",
        F.to_date("소유권변동일자", "yyyy-MM-dd"),
    )
    .withColumn(
        "rn",
        F.row_number().over(
            Window.partitionBy("고유번호")
                  .orderBy(F.col("소유권변동일자_dt").desc_nulls_last())
        ),
    )
    .filter(F.col("rn") == 1)
    .withColumn("본번", F.split("지번", "-").getItem(0))
    .withColumn(
        "부번",
        F.when(
            F.size(F.split("지번", "-")) > 1,
            F.split("지번", "-").getItem(1),
        ),
    )
    .drop("rn", "소유권변동일자_dt", "지번")
)

# ============================================================
# SAVE
# ============================================================
(
    toji_clean_df
    .write
    .mode("overwrite")
    .partitionBy(*partition_cols)
    .parquet(toji_clean_path)
)

spark.stop()
