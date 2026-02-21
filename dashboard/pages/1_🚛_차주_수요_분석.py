import streamlit as st
import geopandas as gpd
import pydeck as pdk
import os
from dotenv import load_dotenv
import pandas as pd

from chajoo_map import load_shp, prepare_chajoo_data, render_chajoo_grid, render_chajoo_map

# 1. 환경 설정 및 페이지 설정
load_dotenv()
MAPBOX_API_KEY = os.getenv("MAPBOX_API_KEY")

st.set_page_config(
    page_title="차주 수요 분석",
    page_icon="🚛",
    layout="wide"
)

# ------------------------------------------------------------------------------
# 실행 로직
# ------------------------------------------------------------------------------
def main():
    st.title("🚛 전국 화물 차주 수요 및 인프라 분석")
    st.divider()

    # 1. 데이터 준비
    gdf_base = load_shp()
    merged_df, df_parking = prepare_chajoo_data(gdf_base)

    if merged_df is not None:
        # 2. 레이아웃 분할 (그리드 4 : 지도 6)
        col_left, col_right = st.columns([6, 4])

        with col_left:
            st.subheader("📊 지역별 수요 순위")
            render_chajoo_grid(merged_df)

        with col_right:
            # 지도 상단 헤더 공간 (제목과 토글 분리)
            map_header_left, map_header_right = st.columns([7, 3])
            with map_header_left:
                st.subheader("📍 전국 밀집도 지표")
            with map_header_right:
                # ✅ 고유 key를 부여하여 리런 시에도 상태가 유지되도록 설정
                use_satellite = st.toggle("🛰️ 위성 지도", value=False, key="chajoo_map_satellite")

            render_chajoo_map(merged_df, df_parking, MAPBOX_API_KEY)

        # --- 🚀 메인 분석 페이지 연결 섹션 ---
        st.divider()

        if "target_sigungu" in st.session_state:
            target = st.session_state["target_sigungu"]

            # 레이아웃을 중앙으로 잡아 집중도 향상
            _, center_col, _ = st.columns([1, 2, 1])

            with center_col:
                # 파일 경로를 정확히 입력해야 합니다 (pages/ 생략 가능)
                if st.button(f"🚀 {target} 식당 분석 페이지로 이동", width="stretch", type="primary"):
                    st.switch_page("0_📍_식당_주차장_관리.py")

        else:
            st.info("💡 왼쪽 표에서 분석을 원하는 지역을 선택해 주세요.")


if __name__ == "__main__":
    main()