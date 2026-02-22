from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql.types import StructType, StructField, DoubleType
from pyspark.sql.functions import pandas_udf

import pandas as pd
import yaml
import os

from utils.spark_path import get_latest_year_month_path, get_current_year_month_path

# ============================================================
# Spark Session
# ============================================================
spark = (
    SparkSession.builder
    .appName("silver_clean_to_s0_address")
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

# input
address_clean_src_base = os.path.join(
    ROOT,
    LAYERS["silver"]["clean"]["domains"]["address_clean"]["paths"]["parquet"],
)
address_clean_src_path = get_latest_year_month_path(spark, address_clean_src_base)

coord_clean_src_base = os.path.join(
    ROOT,
    LAYERS["silver"]["clean"]["domains"]["coord_clean"]["paths"]["parquet"],
)
coord_clean_src_path = get_latest_year_month_path(spark, coord_clean_src_base)

# output
s0_address_base = os.path.join(
    ROOT,
    LAYERS["silver"]["stages"]["s0"]["domains"]["address"]["paths"]["parquet"],
)
s0_address_path = get_current_year_month_path(s0_address_base)

s0_partition_cols = LAYERS["silver"]["stages"]["s0"]["domains"]["address"].get("partition")

# ============================================================
# LOAD
# ============================================================
addr_clean_df = spark.read.parquet(address_clean_src_path)
coord_clean_df = spark.read.parquet(coord_clean_src_path)

# ============================================================
# 도로명주소 + 위치정보 Join
# ============================================================
joined_df = addr_clean_df.join(coord_clean_df.drop("region"), on="도로명주소", how="left")

# ============================================================
# 좌표 변환 (EPSG:5179 -> EPSG:4326)
# ============================================================
schema = StructType([
    StructField("latitude", DoubleType(), True),
    StructField("longitude", DoubleType(), True),
])


@pandas_udf(schema)
def utmk5179_to_wgs84(x: pd.Series, y: pd.Series) -> pd.DataFrame:
    from pyproj import Transformer
    transformer = Transformer.from_crs("EPSG:5179", "EPSG:4326", always_xy=True)

    mask = x.isna() | y.isna()
    xx = x.astype("float64")
    yy = y.astype("float64")

    lon, lat = transformer.transform(xx, yy)

    out = pd.DataFrame({"latitude": lat, "longitude": lon})
    out.loc[mask, ["latitude", "longitude"]] = None
    return out


converted_df = (
    joined_df
    .withColumn("wgs84", utmk5179_to_wgs84(F.col("x_utmk"), F.col("y_utmk")))
    .withColumn("latitude", F.col("wgs84.latitude"))
    .withColumn("longitude", F.col("wgs84.longitude"))
    .drop("wgs84")
)

result_df = (
    converted_df
    .drop("x_utmk", "y_utmk")
    .select("region", "PNU코드", "도로명주소", "longitude", "latitude")
)

# ============================================================
# 결과 저장
# ============================================================
(
    result_df
    .write.mode("overwrite")
    .partitionBy(*s0_partition_cols)
    .parquet(s0_address_path)
)

spark.stop()
