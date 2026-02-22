from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql import Window
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
    .appName("silver_s0_to_s1")
    .master("spark://spark-master:7077")
    .config("spark.sql.adaptive.enabled", "true")
    .config("spark.sql.adaptive.coalescePartitions.enabled", "true")
    .config("spark.sql.shuffle.partitions", "40")
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

ONLY_CHANGED = cfg["option"]["crawling_list"]["only_changed"]
CHANGED_DAYS = cfg["option"]["crawling_list"]["changed_days"]

# Input Path
restaurant_coord_src_base = os.path.join(
    ROOT,
    LAYERS["silver"]["stages"]["s1"]["domains"]["restaurant_coord"]["paths"]["parquet"],
)
restaurant_coord_src_path = get_latest_year_month_week_path(spark, restaurant_coord_src_base)

s0_toji_building_base = os.path.join(
    ROOT,
    LAYERS["silver"]["stages"]["s0"]["domains"]["toji_building"]["paths"]["parquet"],
)
s0_toji_building_path = get_latest_year_month_path(spark, s0_toji_building_base)

# Output Path
s1_crawling_list_base = os.path.join(
    ROOT,
    LAYERS["silver"]["stages"]["s1"]["domains"]["crawling_list"]["paths"]["parquet"],
)
s1_crawling_list_path = get_current_year_month_week_path(s1_crawling_list_base)

s1_toji_list_base = os.path.join(
    ROOT,
    LAYERS["silver"]["stages"]["s1"]["domains"]["toji_list"]["paths"]["parquet"],
)
s1_toji_list_path = get_current_year_month_week_path(s1_toji_list_base)

s1_crawling_list_partition_cols = LAYERS["silver"]["stages"]["s1"]["domains"]["crawling_list"].get("partition")
s1_toji_list_partition_cols = LAYERS["silver"]["stages"]["s1"]["domains"]["toji_list"].get("partition")

# ============================================================
# 데이터 로드 (식당 좌표 + S0 토지_건축물)
# ============================================================
rest_clean_df = spark.read.parquet(restaurant_coord_src_path).drop("region")

toji_building_df = (
    spark.read.parquet(s0_toji_building_path)
    .drop("year", "month")
)

# ============================================================
# 토지 그룹 / 필터 리스트 추출
# ============================================================

# 건물이 1개 있는 토지만 필터링
toji_with_1_building_df = toji_building_df.filter(F.col("관리_건축물대장_PK").isNotNull())

# 토지 + 식당 join
t = toji_with_1_building_df.alias("t")
r = rest_clean_df.alias("r")

toji_building_restaurant_df = (
    t.join(r, F.col("t.고유번호") == F.col("r.PNU코드"), how="left")
    .drop(F.col("r.PNU코드"))
    .filter(F.col("업체명").isNotNull())
)

# 건물 없는 필지에 null 컬럼 추가 후 union
toji_with_0_building_df = (
    toji_building_df
    .filter(F.col("관리_건축물대장_PK").isNull())
    .withColumn("업체명", F.lit(None).cast("string"))
    .withColumn("업종", F.lit(None).cast("string"))
    .withColumn("대표자", F.lit(None).cast("string"))
    .withColumn("대표자_수", F.lit(None).cast("integer"))
    .withColumn("도로명주소", F.lit(None).cast("string"))
    .withColumn("longitude", F.lit(None).cast("double"))
    .withColumn("latitude", F.lit(None).cast("double"))
)

final_toji_df = toji_building_restaurant_df.unionByName(toji_with_0_building_df)

# 식당이 있는 그룹만 필터
# group_has_restaurant_df = (
#     final_toji_df
#     .groupBy("법정동명", "본번")
#     .agg(
#         F.max(F.when(F.col("업체명").isNotNull(), 1).otherwise(0)).alias("has_restaurant")
#     )
#     .filter(F.col("has_restaurant") == 1)
#     .select("법정동명", "본번")
# )

# filtered_final_toji_df = final_toji_df.join(
#     group_has_restaurant_df, on=["법정동명", "본번"], how="inner"
# )

w = Window.partitionBy("법정동명", "본번")

filtered_final_toji_df = (
    final_toji_df
    .withColumn(
        "has_restaurant",
        F.max(F.when(F.col("업체명").isNotNull(), 1).otherwise(0)).over(w)
    )
    .filter(F.col("has_restaurant") == 1)
    .drop("has_restaurant")
)

# 크롤링 리스트용 그룹
toji_group_df = (
    filtered_final_toji_df
    .groupBy("법정동명", "본번", "소유권변동일자", "region")
    .agg(F.min("부번").alias("부번"))
    .distinct()
)

if ONLY_CHANGED:
    toji_group_df = (
        toji_group_df
        .filter(F.col("소유권변동일자") >= F.date_sub(F.current_date(),CHANGED_DAYS))
    )

# ============================================================
# 결과 저장
# ============================================================
(
    filtered_final_toji_df
    .write.mode("overwrite")
    .partitionBy(*s1_toji_list_partition_cols)
    .parquet(s1_toji_list_path)
)

(
    toji_group_df
    .write.mode("overwrite")
    .partitionBy(*s1_crawling_list_partition_cols)
    .parquet(s1_crawling_list_path)
)

spark.stop()
