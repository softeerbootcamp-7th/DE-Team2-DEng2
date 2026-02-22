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
    .appName("silver_s2_to_gold")
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
s2_toji_owner_match_base = os.path.join(
    ROOT,
    LAYERS["silver"]["stages"]["s2"]["domains"]["toji_owner_match"]["paths"]["parquet"],
)
s2_toji_owner_match_path = get_latest_year_month_week_path(spark, s2_toji_owner_match_base)

# Output Path
gold_restaurant_base = os.path.join(
    ROOT,
    LAYERS["gold"]["domains"]["restaurant"]["paths"]["parquet"],
)
gold_restaurant_path = get_current_year_month_week_path(gold_restaurant_base)

gold_restaurant_partition_cols = LAYERS["gold"]["domains"]["restaurant"].get("partition")

# ============================================================
# LOAD
# ============================================================
toji_owner_df = spark.read.parquet(s2_toji_owner_match_path)

toji_owner_deduplicate_df = (
    toji_owner_df
    .drop("부번", "고유번호", "유휴부지_면적")
    .dropDuplicates()
)

# ============================================================
# 유휴부지 면적 집계
# ============================================================
parking_max_df = (
    toji_owner_df
    .groupBy("법정동명", "본번", "지주", "부번", "region")
    .agg(F.max("유휴부지_면적").alias("유휴부지_면적(max)"))
)

parking_sum_df = (
    parking_max_df
    .groupBy("법정동명", "본번", "지주", "region")
    .agg(F.sum("유휴부지_면적(max)").alias("유휴부지_면적"))
)

final_df = (
    toji_owner_deduplicate_df.alias("r")
    .join(
        parking_sum_df.alias("p"),
        on=["법정동명", "본번", "지주", "region"],
        how="left",
    )
    .filter(F.col("업체명").isNotNull())
)

# ============================================================
# 지표 계산
# ============================================================

# 1. 영업 적합도
업종_score = (
    F.when(F.col("업종").isin("일반음식점", "제과점영업"), 0.4)
     .when(F.col("업종") == "휴게음식점", 0.1)
     .when(F.col("업종").isin("집단급식소", "위탁급식영업"), 0.0)
     .otherwise(0.0)
)

rep_clean = F.regexp_replace(F.col("대표자"), r"\s+", "")
owner_clean = F.regexp_replace(F.col("지주"), r"\s+", "")

name_match = (
    F.col("대표자").isNotNull()
    & F.col("지주").isNotNull()
    & (F.length(rep_clean) >= 2)
    & (F.length(rep_clean) == F.length(owner_clean))
    & (F.substring(rep_clean, 1, 1) == F.substring(owner_clean, 1, 1))
    & (
        (F.length(owner_clean) == 2)
        | (
            F.substring(rep_clean, F.length(rep_clean), 1)
            == F.substring(owner_clean, F.length(owner_clean), 1)
        )
    )
)
지주대표자_score = F.when(name_match, 0.3).otherwise(0.0)

대장_score = F.when(F.col("대장_구분_코드") == "1", 0.15).otherwise(0.0)
공유인_score = F.lit(0.1) * F.pow(F.lit(0.5), F.col("공유인수"))
대표자수_score = F.when(F.col("대표자_수") == 1, 0.05).otherwise(0.0)

영업_적합도 = 업종_score + 지주대표자_score + 대장_score + 공유인_score + 대표자수_score

# 2. 최대 수익성
수익성 = F.log(F.col("유휴부지_면적") + 1) / 9.2

# 3. 주차_적합도
주차_적합도 = F.lit(3)

# 4. 총점
총점 = 주차_적합도 * 수익성 * (주차_적합도 / 5) * 100

final_df = (
    final_df
    .withColumn("영업_적합도", F.round(영업_적합도, 2))
    .withColumn("주차_적합도", 주차_적합도)
    .withColumn("수익성", F.round(수익성, 2))
    .withColumn("총점", F.round(총점, 2))
)

# ============================================================
# 시군구 파싱 + 최종 컬럼 선택
# ============================================================
final_df = (
    final_df
    .withColumn(
        "sigungu",
        F.when(
            F.col("법정동명").rlike(r"[가-힣]+시\s+[가-힣]+구"),
            F.concat_ws(
                " ",
                F.regexp_extract(F.col("법정동명"), r"([가-힣]+시)\s+[가-힣]+구", 1),
                F.regexp_extract(F.col("법정동명"), r"[가-힣]+시\s+([가-힣]+구)", 1),
            ),
        ).otherwise(
            F.regexp_extract(F.col("법정동명"), r"([가-힣]+(?:시|군))", 1)
        ),
    )
    .select(
        "sigungu", "총점", "영업_적합도", "수익성", "주차_적합도",
        "업체명", "도로명주소", "유휴부지_면적", "longitude", "latitude", "region",
    )
)

# ============================================================
# 결과 저장
# ============================================================
(
    final_df
    .write.mode("overwrite")
    .partitionBy(*gold_restaurant_partition_cols)
    .parquet(gold_restaurant_path)
)

spark.stop()
