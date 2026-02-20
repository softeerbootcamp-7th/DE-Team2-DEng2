import pandas as pd
import streamlit as st
from datetime import datetime
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
    LEFT JOIN restaurant_status s 
        ON m."업체명" = s."업체명" AND m."도로명주소" = s."도로명주소"
    -- 신고 테이블과 LEFT JOIN
    LEFT JOIN report_history r
        ON m."업체명" = r."업체명" AND m."도로명주소" = r."도로명주소"
    WHERE m.year = ld.year AND m.month = ld.month
      AND m.longitude IS NOT NULL AND m.latitude IS NOT NULL
      -- 신고 테이블(r)에 매칭되는 데이터가 없는 경우만 선택
      AND r."업체명" IS NULL
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

def load_zscore_hotspots(selected_shp_cd):
    engine = get_engine()

    # 앞 5자리 시군구 코드를 비교하여 필터링 (sigungu_cd 컬럼 활용)
    # 만약 DB 적재 시 앞 5자리를 sigungu_cd로 저장했다면 아래 쿼리가 동작합니다.
    query = f"""
        SELECT lat, lon, value, z_score 
        FROM cargo_zscore_hotspots 
        WHERE sigungu_cd = '{selected_shp_cd}'
    """
    return pd.read_sql(query, engine)

def save_report(company_name, road_address):
    """
    업체명과 도로명주소를 받아 report_history 테이블에 저장합니다.
    """
    engine = get_engine()

    # 1. SQL 쿼리 작성 (복합키 충돌 시 신고일자만 업데이트하거나 무시)
    # :name, :address 등은 SQL Injection 방지를 위한 파라미터 바인딩 방식입니다.
    query = text("""
        INSERT INTO report_history (업체명, 도로명주소, 신고일자)
        VALUES (:name, :address, :report_date)
        ON CONFLICT (업체명, 도로명주소)
        DO UPDATE SET 신고일자 = EXCLUDED.신고일자;
    """)

    try:
        with engine.begin() as conn:
            conn.execute(query, {
                "name": company_name,
                "address": road_address,
                "report_date": datetime.now().date()
            })
        print(f"신고 완료: {company_name}")
        return True
    except Exception as e:
        print(f"신고 저장 중 오류 발생: {e}")
        return False