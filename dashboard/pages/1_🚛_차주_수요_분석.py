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
            render_chajoo_grid(merged_df)

        with col_right:
            render_chajoo_map(merged_df, df_parking, MAPBOX_API_KEY)
    else:
        st.error("데이터를 로드하는 중 오류가 발생했습니다.")

if __name__ == "__main__":
    main()