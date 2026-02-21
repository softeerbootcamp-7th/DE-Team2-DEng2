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
    .appName("silver_clean_to_s0_toji_building")
    .master("spark://spark-master:7077")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    .config("spark.sql.shuffle.partitions", "400")
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

# input
building_clean_src_base = os.path.join(
    ROOT,
    LAYERS["silver"]["clean"]["domains"]["building_clean"]["paths"]["parquet"],
)
building_clean_src_path = get_latest_year_month_path(spark, building_clean_src_base)

toji_clean_src_base = os.path.join(
    ROOT,
    LAYERS["silver"]["clean"]["domains"]["toji_clean"]["paths"]["parquet"],
)
toji_clean_src_path = get_latest_year_month_path(spark, toji_clean_src_base)

# output
s0_toji_building_base = os.path.join(
    ROOT,
    LAYERS["silver"]["stages"]["s0"]["domains"]["toji_building"]["paths"]["parquet"],
)
s0_toji_building_path = get_current_year_month_path(s0_toji_building_base)

s0_partition_cols = LAYERS["silver"]["stages"]["s0"]["domains"]["toji_building"].get("partition")

# ============================================================
# LOAD
# ============================================================
building_clean_df = spark.read.parquet(building_clean_src_path)
toji_clean_df = spark.read.parquet(toji_clean_src_path)

# ============================================================
# 토지 + 건물 Join & 필터링 (건물 0개 또는 1개만)
# ============================================================
t = toji_clean_df.alias("t")
b = building_clean_df.drop("region").alias("b")

toji_building_df = (
    t.join(b, F.col("t.고유번호") == F.col("b.고유번호"), "left")
     .drop(F.col("b.고유번호"))
)

pk_cnt_df = (
    toji_building_df
    .groupBy("고유번호")
    .agg(F.count("관리_건축물대장_PK").alias("pk_cnt"))
)

toji_binary_building_df = (
    toji_building_df
    .join(
        pk_cnt_df.filter(
            (F.col("pk_cnt") == 0) | (F.col("pk_cnt") == 1)
        ),
        on="고유번호",
        how="inner",
    )
    .drop("pk_cnt", "has_general_building")
)

# 지목 필터
toji_binary_building_df = toji_binary_building_df.filter(
    F.col("지목").isin("대", "잡종지", "주차장")
)

# 유휴부지 면적 계산
toji_binary_building_df = (
    toji_binary_building_df
    .withColumn(
        "유휴부지_면적",
        F.when(F.col("건축면적").isNull(), F.col("토지면적"))
         .otherwise(F.greatest(
             F.col("토지면적") - F.col("건축면적"),
             F.coalesce(F.col("옥외자주식면적"), F.lit(0)),
             F.lit(0),
         ))
    )
    .drop("토지면적", "건축면적", "옥외자주식면적")
)

# ============================================================
# 결과 저장
# ============================================================
(
    toji_binary_building_df
    .write.mode("overwrite")
    .partitionBy(*s0_partition_cols)
    .parquet(s0_toji_building_path)
)

spark.stop()
