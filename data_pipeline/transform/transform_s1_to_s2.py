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
    .appName("silver_s1_to_s2")
    .master("spark://spark-master:7077")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
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

# Input Path
s1_toji_list_base = os.path.join(
    ROOT,
    LAYERS["silver"]["stages"]["s1"]["domains"]["toji_list"]["paths"]["parquet"],
)
s1_toji_list_path = get_latest_year_month_week_path(spark, s1_toji_list_base)

s2_ownership_base = os.path.join(
    ROOT,
    LAYERS["silver"]["stages"]["s2"]["domains"]["ownership_inference"]["paths"]["parquet"],
)
s2_ownership_path = get_latest_year_month_week_path(spark, s2_ownership_base)

# Output Path
s2_toji_owner_match_base = os.path.join(
    ROOT,
    LAYERS["silver"]["stages"]["s2"]["domains"]["toji_owner_match"]["paths"]["parquet"],
)
s2_toji_owner_match_path = get_current_year_month_week_path(s2_toji_owner_match_base)

s2_toji_owner_match_partition_cols = LAYERS["silver"]["stages"]["s2"]["domains"]["toji_owner_match"].get("partition")

# ============================================================
# LOAD
# ============================================================
toji_df = spark.read.parquet(s1_toji_list_path)

owner_df = (
    spark.read.parquet(s2_ownership_path)
    .dropDuplicates(["주소", "본번", "소유권변동일자"])
)

# ============================================================
# 토지 + 소유자 Join
# ============================================================
t = toji_df.alias("t")
o = owner_df.alias("o")

toji_with_owner_df = (
    t.join(
        o,
        (F.col("t.법정동명") == F.col("o.주소"))
        & (F.col("t.본번") == F.col("o.본번"))
        & (F.col("t.소유권변동일자") == F.col("o.소유권변동일자"))
        & (F.col("t.region") == F.col("o.region")),
        how="inner",
    )
    .select(
        "t.*",
        F.col("o.지주").alias("지주"),
    )
)

# ============================================================
# 결과 저장
# ============================================================
(
    toji_with_owner_df
    .write.mode("overwrite")
    .partitionBy(*s2_toji_owner_match_partition_cols)
    .parquet(s2_toji_owner_match_path)
)

spark.stop()
