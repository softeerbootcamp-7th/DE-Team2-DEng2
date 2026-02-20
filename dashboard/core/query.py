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
        SELECT "sido", "sigungu", "SHP_CD" AS shp_cd, "cargo_count" AS value, "전략적_중요도" AS score
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
def load_restaurants(target_sigungu: str):
    search_terms = target_sigungu.split()

    # LIKE 조건을 동적으로 생성
    like_clauses = []
    params = {}

    for i, term in enumerate(search_terms):
        key = f"term_{i}"
        like_clauses.append(f"m.법정동명 LIKE %({key})s")
        params[key] = f"%{term}%"

    where_like_sql = " AND ".join(like_clauses)

    query = f"""
    WITH latest_date AS (
        SELECT year, month
        FROM restaurant
        ORDER BY year DESC, month DESC
        LIMIT 1
    )
    SELECT
        m.*
    FROM restaurant m
    CROSS JOIN latest_date ld
    WHERE
        m.year = ld.year
        AND m.month = ld.month
        AND {where_like_sql}
    """

    return pd.read_sql(query, get_engine(), params=params)

# --- 업데이트 함수 ---
def update_restaurant(name, address, access, status, remarks):
    engine = get_engine()
    with engine.begin() as conn:
        query = text("""
            INSERT INTO restaurant ("업체명", "도로명주소", "대형차_접근성", contract_status, remarks)
            VALUES (:name, :address, :access, :status, :remarks)
            ON CONFLICT ("업체명", "도로명주소")
            DO UPDATE SET
                "대형차_접근성" = EXCLUDED."대형차_접근성",
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
