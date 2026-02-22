from pyspark.sql import SparkSession
import pyspark.sql.functions as F
import yaml
import os

from utils.spark_path import (
    get_latest_year_month_path,
    get_current_year_month_week_path,
    get_latest_year_month_week_path,
)

# ============================================================
# Spark Session
# ============================================================
spark = (
    SparkSession.builder
    .appName("silver_s0_to_s1_chk")
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

# Input
restaurant_clean_base = os.path.join(
    ROOT,
    LAYERS["silver"]["clean"]["domains"]["restaurant_clean"]["paths"]["parquet"],
)
restaurant_clean_path = get_latest_year_month_week_path(spark, restaurant_clean_base)

s0_address_base = os.path.join(
    ROOT,
    LAYERS["silver"]["stages"]["s0"]["domains"]["address"]["paths"]["parquet"],
)
s0_address_path = get_latest_year_month_path(spark, s0_address_base)

# Output
restaurant_coord_src_base = os.path.join(
    ROOT,
    LAYERS["silver"]["stages"]["s1"]["domains"]["restaurant_coord"]["paths"]["parquet"],
)
restaurant_coord_src_path = get_current_year_month_week_path(restaurant_coord_src_base)

partition_cols = LAYERS["silver"]["stages"]["s1"]["domains"]["restaurant_coord"].get("partition")

# ============================================================
# 데이터 로드
# ============================================================
rest_df = spark.read.parquet(restaurant_clean_path)

addr_df = (
    spark.read.parquet(s0_address_path)
    .select("PNU코드", "도로명주소", "longitude", "latitude")
)

# ============================================================
# 식당 + 주소 Join (소재지 == 도로명주소)
# ============================================================
joined_df = (
    rest_df.alias("r")
    .join(
        addr_df.alias("a"),
        F.col("r.소재지") == F.col("a.도로명주소"),
        "left",
    )
)

joined_rest_df = (
    joined_df
    .filter(F.col("PNU코드").isNotNull())
    .select(
        F.col("r.업체명").alias("업체명"),
        F.col("r.업종").alias("업종"),
        F.col("r.대표자").alias("대표자"),
        F.col("r.대표자수").alias("대표자_수"),
        F.col("r.소재지").alias("도로명주소"),
        F.col("r.region").alias("region"),
        F.col("a.PNU코드").alias("PNU코드"),
        F.col("a.longitude").alias("longitude"),
        F.col("a.latitude").alias("latitude"),
    )
)

# ============================================================
# 결과 저장
# ============================================================
(
    joined_rest_df
    .write
    .mode("overwrite")
    .partitionBy(*partition_cols)
    .parquet(restaurant_coord_src_path)
)

spark.stop()
