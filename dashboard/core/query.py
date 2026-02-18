import pandas as pd
import streamlit as st
from sqlalchemy import text
from core.db import get_engine

# --- 로드 함수들 ---
@st.cache_data
def load_chajoo_data():
    engine = get_engine()

    # 1. 가장 최신 파티션(year, month) 정보를 먼저 조회
    latest_info_query = """
        SELECT "year", "month"
        FROM chajoo_dist
        ORDER BY "year" DESC, "month" DESC
        LIMIT 1
    """

    with engine.connect() as conn:
        latest_partition = conn.execute(text(latest_info_query)).fetchone()

    if not latest_partition:
        print("❌ DB에 데이터가 존재하지 않습니다.")
        return pd.DataFrame(), None, None

    latest_year, latest_month = latest_partition

    # 2. 조회된 최신 날짜로 데이터 로드
    query = """
        SELECT "sido", "sigungu", "SHP_CD" AS shp_cd, "cargo_sales_count" AS value
        FROM chajoo_dist
        WHERE "year" = %s
          AND "month" = %s
          AND "SHP_CD" IS NOT NULL
    """

    df = pd.read_sql(query, engine, params=(latest_year, latest_month))
    print(f"✅ 최신 데이터 로드 완료: {latest_year}년 {latest_month}월 (rows: {len(df):,})")

    # 데이터프레임과 함께 사용된 날짜 정보를 튜플로 반환
    return df, latest_year, latest_month

@st.cache_data
def load_parking_data():
    query = 'SELECT "공영차고지명" AS name, "주소" AS address, lat, lon FROM truckhelper_parking_area'
    return pd.read_sql(query, get_engine())

@st.cache_data
def load_restaurants():
    query = """
    WITH latest_date AS (
        SELECT year, month FROM restaurant_master ORDER BY year DESC, month DESC LIMIT 1
    )
    SELECT m.*, s.large_vehicle_access, s.contract_status, s.remarks
    FROM restaurant_master m
    CROSS JOIN latest_date ld
    LEFT JOIN restaurant_status s ON m."업체명" = s."업체명" AND m."도로명주소" = s."도로명주소"
    WHERE m.year = ld.year AND m.month = ld.month
      AND m.longitude IS NOT NULL AND m.latitude IS NOT NULL
    """
    return pd.read_sql(query, get_engine())

# --- 업데이트 함수 ---
def update_restaurant_status(name, address, access, status, remarks):
    engine = get_engine()
    with engine.begin() as conn:
        query = text("""
            INSERT INTO restaurant_status ("업체명", "도로명주소", large_vehicle_access, contract_status, remarks)
            VALUES (:name, :address, :access, :status, :remarks)
            ON CONFLICT ("업체명", "도로명주소")
            DO UPDATE SET
                large_vehicle_access = EXCLUDED.large_vehicle_access,
                contract_status = EXCLUDED.contract_status,
                remarks = EXCLUDED.remarks,
                updated_at = CURRENT_TIMESTAMP
        """)
        conn.execute(query, {"name": name, "address": address, "access": access, "status": status, "remarks": remarks})
    st.cache_data.clear()