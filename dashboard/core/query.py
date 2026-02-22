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

def load_restaurants(target_sigungu: str):
    tokens = target_sigungu.split()
    if not tokens: return pd.DataFrame()

    region_val = tokens[0].strip()
    sigungu_val = " ".join(tokens[1:]).strip()

    # 💡 쿼리에서 컬럼명을 명시적으로 "쌍따옴표"와 함께 작성합니다.
    query = text("""
    WITH latest_date AS (
        SELECT year, month, week
        FROM restaurant
        WHERE region = :region
        ORDER BY year DESC, month DESC, week DESC
        LIMIT 1
    )
    SELECT
        m."업체명",
        m."도로명주소",
        m.latitude,
        m.longitude,
        m."총점",
        m."수익성",
        m."영업_적합도",
        m."주차_적합도",
        m."유휴부지_면적",
        m.contract_status,
        m.remarks,
        m.year, m.month, m.week
    FROM restaurant m
    INNER JOIN latest_date ld ON 
        m.year = ld.year AND 
        m.month = ld.month AND 
        m.week = ld.week
    WHERE
        m.region = :region
        AND m.sigungu LIKE :sigungu
    ORDER BY m."총점" DESC
    LIMIT 15
    """)

    with get_engine().connect() as conn:
        df = pd.read_sql(query, conn, params={"region": region_val, "sigungu": f"%{sigungu_val}%"})

    return df

# --- 업데이트 함수 ---
def update_restaurant(name, address, access, status, remarks):
    engine = get_engine()
    with engine.begin() as conn:
        # 💡 SET 절에서 "총점" 계산식을 직접 수행합니다.
        # 주의: "화물차_접근성"이 정수형이므로 5.0으로 나누어 실수 연산을 유도합니다.
        query = text("""
            UPDATE restaurant
            SET
                "주차_적합도" = :access,
                contract_status = :status,
                remarks = :remarks,
                "총점" = "영업_적합도" * "수익성" * (:access / 5.0) * 100
            WHERE "업체명" = :name AND "도로명주소" = :address
        """)

        result = conn.execute(query, {
            "name": name, 
            "address": address, 
            "access": access, 
            "status": status, 
            "remarks": remarks
        })

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


def get_last_viewed_sigungu():
    """
    DB에서 ID 1번에 저장된 마지막 조회 지역명을 가져옵니다.
    데이터가 없으면 기본값인 '경기도 평택시'를 반환합니다.
    """
    engine = get_engine()
    query = text("SELECT sigungu FROM user_view_history WHERE id = 1")

    try:
        df = pd.read_sql(query, engine)
        if not df.empty:
            return df.iloc[0]['sigungu']
        return "경기도 평택시"
    except Exception:
        # 테이블이 없거나 연결 오류 시 대비
        return "경기도 평택시"

def save_view_history(sigungu):
    """
    ID 1번 행에 현재 조회 중인 지역명을 업데이트(UPSERT)합니다.
    """
    engine = get_engine()

    # f-string 대신 파라미터 바인딩(:sigungu)을 사용하는 것이 보안상 더 안전합니다.
    query = text("""
        INSERT INTO user_view_history (id, sigungu, viewed_at)
        VALUES (1, :sigungu, CURRENT_TIMESTAMP)
        ON CONFLICT (id) DO UPDATE 
        SET sigungu = EXCLUDED.sigungu,
            viewed_at = CURRENT_TIMESTAMP;
    """)

    with engine.begin() as conn:
        # text() 객체와 함께 파라미터를 딕셔너리 형태로 전달합니다.
        conn.execute(query, {"sigungu": sigungu})