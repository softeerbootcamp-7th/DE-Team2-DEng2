import os

import geopandas as gpd
import streamlit as st
from dotenv import load_dotenv

from core.settings import SHP_PATH
from core.query import load_restaurants, get_last_viewed_sigungu, save_view_history
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
# 1. Target 지역 설정
# ------------------------------------------------------------------------------

# 1. 세션 우선 확인, 없으면 DB 확인
if "target_sigungu" in st.session_state:
    TARGET_SIGUNGU = st.session_state["target_sigungu"]
else:
    TARGET_SIGUNGU = get_last_viewed_sigungu()
    st.session_state["target_sigungu"] = TARGET_SIGUNGU

# 2. 현재 결정된 지역을 DB에 최종 업데이트 (한 줄만 유지)
save_view_history(TARGET_SIGUNGU)

# ------------------------------------------------------------------------------
# 2. 메인 대시보드 레이아웃
# ------------------------------------------------------------------------------
def main():
    st.title("🚀 식당 주차장 야간 화물 차고지")
    st.divider()
    # 1. 세션 상태 초기화 (최초 실행 시)
    if "picked" not in st.session_state:
        st.session_state["picked"] = None
    if "_need_rerun" not in st.session_state:
        st.session_state["_need_rerun"] = False

    gdf_shp = load_shp()
    df = load_restaurants(TARGET_SIGUNGU)

    # 2. SHP 코드 추출 로직
    matching_row = gdf_shp[
        (gdf_shp["sido"] + " " + gdf_shp["SIGUNGU_NM"]) == TARGET_SIGUNGU
    ]
    selected_shp_cd = matching_row["SIGUNGU_CD"].iloc[0] if not matching_row.empty else None

    # 3. 레이아웃 배치
    col_grid, col_map = st.columns([6, 4])

    with col_grid:
        st.subheader(f"📋 {TARGET_SIGUNGU}의 후보 식당 리스트")
        # 그리드 함수에서 클릭 감지 및 세션 업데이트 수행
        render_restaurant_grid(df)

    with col_map:
        map_header_col1, map_header_col2 = st.columns([7, 3])
        with map_header_col1:
            st.subheader("📍 위치 확인")
        with map_header_col2:
            # key를 지정하면 사용자가 클릭한 상태가 session_state에 박제됩니다.
            st.toggle("🛰️ 위성 지도", value=False, key="use_satellite_toggle")

        # 지도는 세션의 "picked" 좌표를 최우선으로 그림
        render_restaurant_map(
            df=df,
            selected_shp_cd=selected_shp_cd,
            gdf_boundary=gdf_shp,
            mapbox_api_key=MAPBOX_API_KEY
        )

    st.divider()

    # 에디터는 세션의 "editing_data"를 그림
    render_restaurant_editor(df)


if __name__ == "__main__":
    main()