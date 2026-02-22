from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import yaml
import os

from utils.spark_path import (
    get_current_year_month_week_path,
    get_latest_year_month_week_path,
)

# ============================================================
# Spark Session
# ============================================================
spark = (
    SparkSession.builder
    .appName("silver_clean_restaurant")
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

REGION = cfg["target"]["REGION"]

# Input
restaurant_src_base = os.path.join(
    ROOT,
    LAYERS["bronze"]["domains"]["restaurant_owner"]["paths"]["parquet"],
)
restaurant_src_path = get_latest_year_month_week_path(spark, restaurant_src_base)

# Output
restaurant_clean_base = os.path.join(
    ROOT,
    LAYERS["silver"]["clean"]["domains"]["restaurant_clean"]["paths"]["parquet"],
)
restaurant_clean_path = get_current_year_month_week_path(restaurant_clean_base)

partition_cols = LAYERS["silver"]["clean"]["domains"]["restaurant_clean"].get(
    "partition", ["region"]
)

# ============================================================
# LOAD
# ============================================================
rest_df = (
    spark.read.parquet(restaurant_src_path)
    .filter(F.col("region") == REGION)
    .select("업체명", "대표자", "소재지", "업종", "region")
)

# ============================================================
# CLEANING
# ============================================================
clean_rest_addr_df = (
    rest_df
    .filter(F.col("업체명").isNotNull())
    .filter(F.col("대표자").isNotNull())
    .filter(F.col("소재지").isNotNull())
    .withColumn("소재지", F.regexp_replace(F.col("소재지"), r"\s*\([^)]*\)", ""))
    .withColumn("소재지", F.regexp_replace(F.col("소재지"), r",.*$", ""))
    .withColumn(
        "소재지",
        F.regexp_replace(
            F.col("소재지"),
            r"\s+\S+(동|읍|면|리)\s+(?=\S+(로|길))",
            " "
        )
    )
    .withColumn("소재지", F.trim(F.regexp_replace(F.col("소재지"), r"\s+", " ")))
)

clean_rest_df = (
    clean_rest_addr_df
    .withColumn(
        "대표자수",
        F.when(
            F.col("대표자").rlike(r"외\s*\d+"),
            1 + F.regexp_extract(F.col("대표자"), r"외\s*(\d+)", 1).cast("int")
        )
        .when(
            F.col("대표자").rlike(r"[,/·]"),
            F.size(F.split(F.col("대표자"), r"\s*[,/·]\s*"))
        )
        .otherwise(F.lit(1))
    )
    .withColumn("대표자", F.trim(F.col("대표자")))
    .withColumn("대표자", F.regexp_replace(F.col("대표자"), r"\s*\([^)]*\)\s*", ""))
    .withColumn("대표자", F.regexp_replace(F.col("대표자"), r"\s*[,/].*$", ""))
    .withColumn("대표자", F.regexp_replace(F.col("대표자"), r"\s*외\s*\d+\s*(인|명)\s*$", ""))
    .withColumn("대표자", F.regexp_replace(F.col("대표자"), r"\s*외\s*\d+\s*$", ""))
    .withColumn("대표자", F.regexp_replace(F.col("대표자"), r"\s+", " "))
    .withColumn("대표자", F.trim(F.col("대표자")))
    .dropDuplicates(["업체명", "소재지"])
)

# ============================================================
# SAVE
# ============================================================
(
    clean_rest_df
    .write
    .mode("overwrite")
    .partitionBy(*partition_cols)
    .parquet(restaurant_clean_path)
)

spark.stop()
