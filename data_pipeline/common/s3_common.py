from typing import Optional


def _normalize_prefix(prefix: str) -> str:
    return prefix.strip("/")

def build_data_root(
    storage: str,
    local_data_root: str = "/opt/spark/data",
    s3_bucket: Optional[str] = None,
    s3_prefix: str = "data",
) -> str:
    """Return the root URI for data paths used by Spark jobs."""
    if storage == "local":
        return local_data_root.rstrip("/")

    if storage != "s3":
        raise ValueError(f"Unsupported storage mode: {storage}")

    if not s3_bucket:
        raise ValueError("s3_bucket is required when storage='s3'")

    prefix = _normalize_prefix(s3_prefix)
    if not prefix:
        return f"s3a://{s3_bucket}"
    return f"s3a://{s3_bucket}/{prefix}"


def configure_s3a_for_spark(
    spark_builder,
    aws_access_key_id: Optional[str] = None,
    aws_secret_access_key: Optional[str] = None,
    aws_session_token: Optional[str] = None,
    region: Optional[str] = "ap-northeast-2",
    path_style_access: bool = False,
):
    """
    Apply s3 settings to SparkSession.builder.
    """
    builder = spark_builder

    # 2. S3A 파일 시스템 구현체 지정
    # s3a:// 프로토콜을 사용할 때 호출될 실제 Java 클래스를 정의
    builder = builder.config("spark.hadoop.fs.s3a.impl", "org.apache.hadoop.fs.s3a.S3AFileSystem")

    # 3. S3 경로 스타일 설정
    # True면 s3.amazonaws.com/bucket 형식, False(기본)면 bucket.s3.amazonaws.com 형식을 사용
    builder = builder.config("spark.hadoop.fs.s3a.path.style.access", str(path_style_access).lower())

    # 4. 인증 정보 설정
    # Provider는 S3A 기본 체인을 사용하고, 명시 키가 있으면 conf에 주입합니다.
    if aws_access_key_id and aws_secret_access_key:
        builder = builder.config("spark.hadoop.fs.s3a.access.key", aws_access_key_id)
        builder = builder.config("spark.hadoop.fs.s3a.secret.key", aws_secret_access_key)
        if aws_session_token:
            builder = builder.config("spark.hadoop.fs.s3a.session.token", aws_session_token)

    # 5. 리전 설정
    # S3 버킷이 위치한 물리적 지역을 지정 (기본값: 서울 리전)
    if region:
        builder = builder.config("spark.hadoop.fs.s3a.endpoint.region", region)

    return builder
