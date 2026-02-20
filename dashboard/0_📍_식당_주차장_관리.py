import os

import geopandas as gpd
import streamlit as st
from dotenv import load_dotenv

from core.settings import SHP_PATH
from core.query import load_restaurants
from restaurant_map import render_restaurant_editor, render_restaurant_map, render_restaurant_grid

# ------------------------------------------------------------------------------
# 1. 환경 설정 및 초기화
# ------------------------------------------------------------------------------
current_dir = os.path.dirname(os.path.abspath(__file__))
env_path = os.path.join(current_dir, "..", ".env")
load_dotenv(dotenv_path=env_path, override=True)

MAPBOX_API_KEY = os.getenv("MAPBOX_API_KEY")

st.set_page_config(
    page_title="식당 주차장 야간 화물 차고지",
    page_icon="🚀",
    layout="wide"
)

# ------------------------------------------------------------------------------
# 2. 데이터 로딩 함수 (Caching)
# ------------------------------------------------------------------------------
@st.cache_resource
def load_shp():
    gdf = gpd.read_parquet(SHP_PATH)
    return gdf

# ------------------------------------------------------------------------------
# 1. 코드 내부 고정 필터 설정 (사용자 입력 대신 여기서 제어)
# ------------------------------------------------------------------------------
TARGET_SIGUNGU = "경기 용인시 처인구"

# ------------------------------------------------------------------------------
# 2. 메인 대시보드 레이아웃
# ------------------------------------------------------------------------------
def main():
    st.title("🚀 식당 주차장 야간 화물 차고지")

    gdf_shp = load_shp()

    # SHP 코드 미리 추출
    matching_row = gdf_shp[
        (gdf_shp["sido"] + " " + gdf_shp["SIGUNGU_NM"]) == TARGET_SIGUNGU
    ]
    selected_shp_cd = matching_row["SIGUNGU_CD"].iloc[0] if not matching_row.empty else None

    # ------------------------------------------------------------------------------
    # 데이터 로드 우선 실행
    # ------------------------------------------------------------------------------
    df = load_restaurants(TARGET_SIGUNGU)


    # --- 상단: Grid + Map ---
    col_grid, col_map = st.columns([6, 4])

    with col_grid:
        st.subheader("📋 식당 리스트")
        render_restaurant_grid(df)

    with col_map:
        map_header_col1, map_header_col2 = st.columns([7, 3])
        with map_header_col1:
            st.subheader("📍 위치 확인")
        with map_header_col2:
            use_satellite = st.toggle("🛰️ 위성 지도", value=False)

        if df is not None and not df.empty:
            render_restaurant_map(
                df=df,
                selected_shp_cd=selected_shp_cd,
                gdf_boundary=gdf_shp,
                use_satellite=use_satellite,
                mapbox_api_key=MAPBOX_API_KEY
            )

    st.divider()
    st.subheader("📝 식당 정보 수정")
    render_restaurant_editor(df)

if __name__ == "__main__":
    main()