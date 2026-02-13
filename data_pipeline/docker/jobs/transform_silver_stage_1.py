import datetime as dt
import argparse
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql import Window

def main():
    # 1. Command Line Arguments 설정
    parser = argparse.ArgumentParser(description="Spark Silver Stage 1 Processing")
    parser.add_argument("--region", type=str, default="경기", help="지역명 (기본값: 경기)")
    parser.add_argument("--sigungu_code", type=str, default="41461", help="시군구코드 5자리 (기본값: 41461)")
    args = parser.parse_args()

    region = args.region
    sigungu_code = args.sigungu_code

    # 2. Spark Session 설정
    spark = SparkSession.builder \
        .appName(f'silver_stage_1_{region}_{sigungu_code}') \
        .master("spark://spark-master:7077") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.memory.fraction", "0.8") \
        .config("spark.executor.memory", "4g") \
        .config("spark.driver.memory", "4g") \
        .config("spark.sql.shuffle.partitions", "400") \
        .getOrCreate()

    # 3. 날짜 및 경로 계산
    today = dt.date.today()
    cur_q = (today.month - 1) // 3 + 1
    prev_q = cur_q - 1
    prev_q_year = today.year
    
    if prev_q == 0:
        prev_q = 4
        prev_q_year -= 1

    prev_q_last_month = prev_q * 3

    BUILDING_BASE = "/opt/spark/data/buildingLeader/parquet"
    TOJI_BASE     = "/opt/spark/data/tojiSoyuJeongbo/parquet"
    REST_BASE     = "/opt/spark/data/restaurant/parquet"
        
    building_path = f"{BUILDING_BASE}/year={prev_q_year}/month={prev_q_last_month:02d}"
    toji_path     = f"{TOJI_BASE}/year={prev_q_year}/month={prev_q_last_month:02d}"
    rest_path     = f"{REST_BASE}/dataset={prev_q_year}Q{prev_q}"

    # 4. 데이터 로드 및 필터링
    building_df = spark.read.option("mergeSchema", "false").parquet(building_path)
    building_gg_df = building_df \
        .filter(F.col("region") == region) \
        .filter(F.col("시군구_코드") == sigungu_code)

    toji_df = spark.read.option("mergeSchema", "false").parquet(toji_path)
    toji_gg_df = toji_df \
        .filter(F.col("region") == region) \
        .filter(F.col("법정동코드").startswith(sigungu_code))

    restaurant_df = spark.read.option("mergeSchema", "false").parquet(rest_path)
    restaurant_gg_df = restaurant_df \
        .filter(F.col("region") == region) \
        .filter(F.col("법정동코드").startswith(sigungu_code))

    # 5. Building Pruning
    building_gg_df = building_gg_df.withColumn(
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
    ).filter(F.col("대장_구분_코드") == "1") \
     .select(
        F.col("관리_건축물대장_PK"),
        F.col("고유번호"),
        F.col("옥외_자주식_면적(㎡)").alias("옥외자주식면적")
    )

    # 6. Toji Pruning
    toji_gg_df = toji_gg_df \
        .filter(F.col("공유인수") == 0) \
        .filter(F.col("소유구분코드") == "01") \
        .select(
            F.col("고유번호").cast("string"),
            F.col("법정동명"), F.col("지번"),
            F.col("소유권변동원인"), F.col("소유권변동일자"),
            F.col("토지면적"), F.col("지목"), F.col("공시지가")
        ) \
        .withColumn("소유권변동일자_dt", F.to_date(F.col("소유권변동일자"), "yyyy-MM-dd")) \
        .withColumn("rn", F.row_number().over(
            Window.partitionBy("고유번호").orderBy(F.col("소유권변동일자_dt").desc_nulls_last())
        )) \
        .filter(F.col("rn") == 1) \
        .drop("rn", "소유권변동일자_dt") \
        .withColumn("본번", F.split(F.col("지번"), "-").getItem(0)) \
        .withColumn("부번", F.when(F.size(F.split(F.col("지번"), "-")) > 1, 
                                F.split(F.col("지번"), "-").getItem(1)).otherwise(F.lit(None)))

    # 7. Restaurant Pruning
    restaurant_gg_df = restaurant_gg_df \
        .filter(F.col("상권업종대분류명") == "음식") \
        .select(
            "상호명", "지점명", "도로명", "상권업종대분류명", 
            "상권업종중분류명", "상권업종소분류명", "지번코드",
            F.col("경도").cast("double"), F.col("위도").cast("double")
        )

    # 8. Join & Filtering
    t = toji_gg_df.alias("t")
    b = building_gg_df.alias("b")
    toji_building_gg_df = t.join(b, F.col("t.고유번호") == F.col("b.고유번호"), "left").drop(F.col("b.고유번호"))

    pk_cnt_df = toji_building_gg_df.groupBy("고유번호").agg(F.count("관리_건축물대장_PK").alias("pk_cnt"))
    toji_binary_building_gg_df = toji_building_gg_df \
        .join(pk_cnt_df.filter(F.col("pk_cnt") <= 1), on="고유번호", how="inner") \
        .drop("pk_cnt")

    # 9. Restaurant Join
    toji_with_1_building_gg_df = toji_binary_building_gg_df.filter(F.col("관리_건축물대장_PK").isNotNull())
    
    t_1 = toji_with_1_building_gg_df.alias("t")
    r = restaurant_gg_df.alias("r")
    toji_building_restaurant_gg_df = t_1.join(r, F.col("t.고유번호") == F.col("r.지번코드"), how="left") \
        .drop(F.col("r.지번코드")) \
        .filter(F.col("상호명").isNotNull())

    # 10. Concat & Final Clean
    toji_with_0_building_gg_df = toji_binary_building_gg_df.filter(F.col("관리_건축물대장_PK").isNull()) \
        .withColumn("상호명", F.lit(None).cast("string")) \
        .withColumn("지점명", F.lit(None).cast("string")) \
        .withColumn("도로명", F.lit(None).cast("string")) \
        .withColumn("상권업종대분류명", F.lit(None).cast("string")) \
        .withColumn("상권업종중분류명", F.lit(None).cast("string")) \
        .withColumn("상권업종소분류명", F.lit(None).cast("string")) \
        .withColumn("경도", F.lit(None).cast("double")) \
        .withColumn("위도", F.lit(None).cast("double"))

    final_toji_df = toji_building_restaurant_gg_df.unionByName(toji_with_0_building_gg_df)

    all_null_group_df = final_toji_df \
        .groupBy("법정동명", "본번") \
        .agg(F.sum(F.col("상호명").isNotNull().cast("int")).alias("non_null_cnt")) \
        .filter(F.col("non_null_cnt") == 0)

    clean_final_toji_df = final_toji_df.join(
        all_null_group_df.select("법정동명", "본번"),
        on=["법정동명", "본번"],
        how="left_anti"
    )

    # 11. 메인 결과 저장
    out_base = "/opt/spark/data/output/silver_stage_1"
    main_out_path = f"{out_base}/main/region={region}/sigungu={sigungu_code}/year={prev_q_year}/month={prev_q_last_month:02d}"

    clean_final_toji_df.write.mode("overwrite").parquet(main_out_path)
    print(f"[SUCCESS] Main data saved to: {main_out_path}")

    # 12. 부산물 저장 (법정동명, 본번, 소유권변동일자로 그룹화하여 부번 리스트 생성)
    byproduct_out_path = f"{out_base}/byproduct/region={region}/sigungu={sigungu_code}/year={prev_q_year}/month={prev_q_last_month:02d}"

    byproduct_df = (
        clean_final_toji_df
        .groupBy("법정동명", "본번", "소유권변동일자")
        .agg(F.collect_list("부번").alias("부번_리스트"))
    )

    byproduct_df.write.mode("overwrite").parquet(byproduct_out_path)
    print(f"[SUCCESS] Byproduct saved to: {byproduct_out_path}")

    spark.stop()

if __name__ == "__main__":
    main()