import datetime as dt
import argparse
import os
import sys
from pathlib import Path
from pyspark.sql import SparkSession
import pyspark.sql.functions as F
from pyspark.sql import Window

# common 폴더 내 파일들 사용을 위한 path 설정
COMMON_DIR_CANDIDATES = [
    Path("/opt/spark/common"),
]
for common_dir in COMMON_DIR_CANDIDATES:
    if common_dir.exists() and str(common_dir) not in sys.path:
        sys.path.insert(0, str(common_dir))

from s3_common import build_data_root, configure_s3a_for_spark
from postgres_orm import store_silver_stage_1


def main():
    # 1. Command Line Arguments 설정
    parser = argparse.ArgumentParser(description="Spark Silver Stage 1 Processing")
    parser.add_argument("--region", type=str, default="경기", help="지역명 (기본값: 경기)")
    # default를 None으로 설정하여 입력이 없을 경우를 대비합니다.
    parser.add_argument("--sigungu_code", type=str, default=None, help="시군구코드 5자리 (입력 안 할 시 지역 전체)")
    parser.add_argument("--input_mode", choices=["local", "s3"], default="local", help="입력 데이터 저장소")
    parser.add_argument(
        "--output_mode",
        choices=["local", "s3", "local_db", "rds"],
        default="local",
        help="최종 결과 저장소",
    )
    parser.add_argument("--local_data_root", type=str, default="/opt/spark/data", help="로컬 데이터 루트 경로")
    parser.add_argument("--s3_bucket", type=str, default=None, help="입력 S3 bucket 이름 (input_mode=s3 일 때 필수)")
    parser.add_argument("--s3_prefix", type=str, default="data", help="입력 S3 prefix (예: data)")
    parser.add_argument("--output_s3_prefix", type=str, default="data", help="output_mode=s3 일 때 출력 S3 prefix")

    args = parser.parse_args()

    region = args.region
    sigungu_code = args.sigungu_code
    need_s3_support = (args.input_mode == "s3") or (args.output_mode == "s3")

    # 2. Spark Session 설정
    # 앱 이름에 sigungu_code가 없을 경우 'all'로 표시
    app_sigungu = sigungu_code if sigungu_code else "all"
    spark_builder = SparkSession.builder \
        .appName(f'silver_stage_1_{region}_{app_sigungu}') \
        .master("spark://spark-master:7077") \
        .config("spark.sql.adaptive.enabled", "true") \
        .config("spark.sql.adaptive.coalescePartitions.enabled", "true") \
        .config("spark.memory.fraction", "0.8") \
        .config("spark.executor.memory", "4g") \
        .config("spark.driver.memory", "4g") \
        .config("spark.sql.shuffle.partitions", "400")

    if need_s3_support:
        spark_builder = configure_s3a_for_spark(
            spark_builder,
            aws_access_key_id=os.getenv("AWS_ACCESS_KEY_ID"),
            aws_secret_access_key=os.getenv("AWS_SECRET_ACCESS_KEY"),
            aws_session_token=os.getenv("AWS_SESSION_TOKEN"),
        )

    spark = spark_builder.getOrCreate()

    # 3. 날짜 및 경로 계산
    today = dt.date.today()
    cur_q = (today.month - 1) // 3 + 1
    prev_q = cur_q - 1
    prev_q_year = today.year
    
    if prev_q == 0:
        prev_q = 4
        prev_q_year -= 1

    prev_q_last_month = prev_q * 3

    input_data_root = build_data_root(
        storage=args.input_mode,
        local_data_root=args.local_data_root,
        s3_bucket=args.s3_bucket,
        s3_prefix=args.s3_prefix,
    )

    BUILDING_BASE = f"{input_data_root}/buildingLeader/parquet"
    TOJI_BASE = f"{input_data_root}/tojiSoyuJeongbo/parquet"
    REST_BASE = f"{input_data_root}/restaurant/parquet"
    REST_OWNER_BASE = f"{input_data_root}/restaurant_owner/parquet"

    building_path = f"{BUILDING_BASE}/year={prev_q_year}/month={prev_q_last_month:02d}"
    toji_path     = f"{TOJI_BASE}/year={prev_q_year}/month={prev_q_last_month:02d}"
    rest_path     = f"{REST_BASE}/dataset={prev_q_year}Q{prev_q}"
    owner_path     = f"{REST_OWNER_BASE}/year={prev_q_year}/month={prev_q_last_month:02d}"

    # 4. 데이터 로드 및 지역 필터링
    # 우선 region으로 공통 필터링을 수행합니다.
    building_df = spark.read.option("mergeSchema", "false").parquet(building_path)
    building_gg_df = building_df.filter(F.col("region") == region)

    toji_df = spark.read.option("mergeSchema", "false").parquet(toji_path)
    toji_gg_df = toji_df.filter(F.col("region") == region)

    restaurant_df = spark.read.option("mergeSchema", "false").parquet(rest_path)
    restaurant_gg_df = restaurant_df.filter(F.col("region") == region)

    owner_df = spark.read.option("mergeSchema", "false").parquet(owner_path)
    owner_gg_df = owner_df.filter(F.col("region") == region)

    # sigungu_code가 인자로 들어온 경우에만 추가 필터링 수행
    if sigungu_code:
        building_gg_df = building_gg_df.filter(F.col("시군구_코드") == sigungu_code)
        toji_gg_df = toji_gg_df.filter(F.col("법정동코드").startswith(sigungu_code))
        restaurant_gg_df = restaurant_gg_df.filter(F.col("법정동코드").startswith(sigungu_code))

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
            "상호명", "지점명", "도로명주소", "상권업종대분류명", 
            "상권업종중분류명", "상권업종소분류명", "지번코드",
            F.col("경도").cast("double"), F.col("위도").cast("double")
        )

    # =========================
    # Owner 소재지 괄호 제거 (맨 끝 "(...)"만 제거)
    # =========================
    owner_gg_df_clean = (
        owner_gg_df
        .select("소재지", "대표자")
        .withColumn(
            "소재지_정제",
            F.regexp_replace(F.col("소재지"), r"\s*\([^)]*\)$", "")
        )
    )

    # =========================
    # Left join: restaurant.도로명 == owner.소재지_정제
    # =========================
    r = restaurant_gg_df.alias("r")
    o = owner_gg_df_clean.alias("o")

    restaurant_gg_df = (
        r.join(
            o,
            F.col("r.도로명주소") == F.col("o.소재지_정제"),
            how="left"
        )
        .drop("소재지", "소재지_정제")
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
        .withColumn("대표자", F.lit(None).cast("string")) \
        .withColumn("도로명주소", F.lit(None).cast("string")) \
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

    # 11. 결과 공통 메타
    # sigungu_code가 없으면 경로명을 'all'로 지정하여 명확성 확보
    sigungu_dir = sigungu_code if sigungu_code else "all"

    byproduct_df = (
        clean_final_toji_df
        .groupBy("법정동명", "본번", "소유권변동일자")
        .agg(F.collect_list("부번").alias("부번_리스트"))
    )

    if args.output_mode in ("local", "s3"):
        if args.output_mode == "local":
            output_data_root = build_data_root(storage="local", local_data_root=args.local_data_root)
        else:
            output_s3_bucket = args.s3_bucket
            if not output_s3_bucket:
                raise ValueError(
                    "--output_mode s3 인 경우 --s3_bucket 값을 지정해야 합니다."
                )
            output_data_root = build_data_root(
                storage="s3",
                s3_bucket=output_s3_bucket,
                s3_prefix=args.output_s3_prefix,
            )

        out_base = f"{output_data_root}/output/silver_stage_1"
        main_out_path = (
            f"{out_base}/main/year={prev_q_year}/month={prev_q_last_month:02d}"
            f"/region={region}/sigungu={sigungu_dir}"
        )
        byproduct_out_path = (
            f"{out_base}/byproduct/year={prev_q_year}/month={prev_q_last_month:02d}"
            f"/region={region}/sigungu={sigungu_dir}"
        )

        clean_final_toji_df.write.mode("overwrite").parquet(main_out_path)
        byproduct_df.write.mode("overwrite").parquet(byproduct_out_path)
        print(f"[SUCCESS] Main data saved to: {main_out_path}")
        print(f"[SUCCESS] Byproduct saved to: {byproduct_out_path}")
    elif args.output_mode in ("local_db", "rds"):
        db_mode = "local" if args.output_mode == "local_db" else "aws"
        row_count = store_silver_stage_1(
            main_df=clean_final_toji_df,
            mode=db_mode,
            source_year=prev_q_year,
            source_month=prev_q_last_month,
            region=region,
            sigungu=sigungu_dir,
        )
        print(
            f"[SUCCESS] Postgres load completed: "
            f"inserted_rows={row_count}, output_mode={args.output_mode}"
        )
    else:
        raise ValueError(f"Unsupported output_mode: {args.output_mode}")

    spark.stop()

if __name__ == "__main__":
    main()
