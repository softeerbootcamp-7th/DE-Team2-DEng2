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
    .appName("silver_clean_address")
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
address_src_base = os.path.join(
    ROOT, LAYERS["bronze"]["domains"]["address"]["paths"]["parquet"]
)
address_src_path = get_latest_year_month_path(spark, address_src_base)

# output (silver clean)
address_clean_base = os.path.join(
    ROOT, LAYERS["silver"]["clean"]["domains"]["address_clean"]["paths"]["parquet"]
)
address_clean_path = get_current_year_month_path(address_clean_base)

partition_cols = LAYERS["silver"]["clean"]["domains"]["address_clean"].get(
    "partition", ["region"]
)

# ============================================================
# LOAD
# ============================================================
addr_df = spark.read.parquet(address_src_path)

# ============================================================
# CLEANING
# ============================================================
addr_clean_df = (
    addr_df
    .withColumn(
        "_pnu_land_gb",
        F.when(F.col("산여부").cast("string") == F.lit("1"), F.lit("2")).otherwise(F.lit("1"))
    )
    .withColumn(
        "PNU코드",
        F.concat(
            F.col("법정동코드").cast("string"),
            F.col("_pnu_land_gb"),
            F.lpad(F.col("지번본번(번지)").cast("string"), 4, "0"),
            F.lpad(F.coalesce(F.col("지번부번(호)"), F.lit("0")).cast("string"), 4, "0"),
        )
    )
    .drop("_pnu_land_gb")
    .withColumn(
        "도로명주소",
        F.concat_ws(
            " ",
            F.col("시도명"),
            F.col("시군구명"),
            F.col("도로명"),
            F.concat(
                F.col("건물본번").cast("string"),
                F.when(
                    (F.col("건물부번").isNotNull()) & (F.col("건물부번") != "0") & (F.col("건물부번") != 0),
                    F.concat(F.lit("-"), F.col("건물부번").cast("string"))
                ).otherwise(F.lit(""))
            )
        )
    )
    .select(
        F.col("PNU코드"),
        F.col("도로명주소"),
        F.col("region")
    )
    .dropDuplicates(["도로명주소"])
)

# ============================================================
# SAVE
# ============================================================
(
    addr_clean_df
    .write
    .mode("overwrite")
    .partitionBy(*partition_cols)
    .parquet(address_clean_path)
)

spark.stop()
