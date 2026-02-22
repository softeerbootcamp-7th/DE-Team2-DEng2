import streamlit as st
import geopandas as gpd
import pydeck as pdk
import os
from dotenv import load_dotenv
import pandas as pd

from chajoo_map import prepare_chajoo_data, render_chajoo_grid, render_chajoo_map
from shp_loader import load_shp

# 1. 환경 설정 및 페이지 설정
load_dotenv()
MAPBOX_API_KEY = os.getenv("MAPBOX_API_KEY")

st.set_page_config(
    page_title="차주 수요 분석",
    page_icon="🚛",
    layout="wide"
)

st.markdown("""
    <style>
    /* 사이드바 네비게이션 */
    [data-testid="stSidebarNav"] span {
        font-size: 20px !important;
        font-weight: 600 !important;
    }
    [data-testid="stSidebarNav"] svg {
        width: 24px !important;
        height: 24px !important;
    }
    [data-testid="stSidebarNav"] li {
        padding-top: 5px !important;
        padding-bottom: 5px !important;
    }

    /* ── 반응형 디자인 ── */

    @media (max-width: 1000px) {
        [data-testid="stHorizontalBlock"] {
            flex-direction: column !important;
        }
        [data-testid="stHorizontalBlock"] > div {
            width: 100% !important;
            flex: 1 1 100% !important;
        }
    }

    @media (max-width: 1200px) {
        .ag-header-cell-label {
            font-size: 14px !important;
        }
        .ag-cell {
            font-size: 14px !important;
        }
        [data-testid="stSidebarNav"] span {
            font-size: 16px !important;
        }
        div[data-testid="stWidgetLabel"] p {
            font-size: 16px !important;
        }
    }

    @media (min-width: 1201px) and (max-width: 1600px) {
        .ag-header-cell-label {
            font-size: 17px !important;
        }
        .ag-cell {
            font-size: 18px !important;
        }
    }

    iframe[title="pydeck_chart"] {
        max-height: 70vh !important;
    }

    .ag-root-wrapper {
        max-height: 65vh !important;
    }
    </style>
""", unsafe_allow_html=True)

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
            st.subheader("📊 지역별 수요 순위 Top 10")
            render_chajoo_grid(merged_df)

        with col_right:
            # 지도 상단 헤더 공간 (제목과 토글 분리)
            map_header_left, map_header_right = st.columns([7, 3])
            with map_header_left:
                st.subheader("📍 전국 밀집도 지표")
            with map_header_right:
                # ✅ 고유 key를 부여하여 리런 시에도 상태가 유지되도록 설정
                st.markdown("""
                    <style>
                    /* 토글 텍스트 - 반응형 */
                    div[data-testid="stWidgetLabel"] p {
                        font-size: clamp(12px, 1vw, 16px) !important;
                        font-weight: 700 !important;
                        color: #ffffff !important;
                    }
                    div[data-testid="stCheckbox"] > label > div:first-child {
                        transform: scale(1.3);
                        margin-right: 10px;
                    }
                    @media (max-width: 1200px) {
                        div[data-testid="stCheckbox"] > label > div:first-child {
                            transform: scale(1.0);
                            margin-right: 6px;
                        }
                    }
                    </style>
                """, unsafe_allow_html=True)
                use_satellite = st.toggle("🛰️ 위성 지도", value=False, key="chajoo_map_satellite")

            render_chajoo_map(merged_df, df_parking, MAPBOX_API_KEY)

        # --- 🚀 메인 분석 페이지 연결 섹션 ---
        st.divider()

        if "target_sigungu" in st.session_state:
            target = st.session_state["target_sigungu"]

            # 레이아웃을 중앙으로 잡아 집중도 향상
            _, center_col, _ = st.columns([1, 2, 1])

            with center_col:
                # 🎨 커스텀 스타일 적용 (높이 증가, 글씨 크기 확대, 너비 제한)
                st.markdown(f"""
                    <style>
                    div.stButton > button {{
                        height: clamp(60px, 8vh, 100px) !important;
                        width: 100% !important;
                        border-radius: 20px !important;
                        background-color: #FF4B4B !important;
                        border: none !important;
                        transition: all 0.3s ease !important;
                    }}
                    div.stButton > button p {{
                        font-size: clamp(18px, 2vw, 30px) !important;
                        font-weight: 800 !important;
                        color: white !important;
                    }}
                    div.stButton > button:hover {{
                        transform: scale(1.05) !important;
                        background-color: #FF3333 !important;
                        box-shadow: 0 10px 20px rgba(0,0,0,0.4) !important;
                    }}
                    </style>
                """, unsafe_allow_html=True)

                if st.button(f"🚀 {target} 식당 분석하기", use_container_width=True):
                    # 파일명 이모지 포함 주의 (0_📍_식당_주차장_관리.py)
                    st.switch_page("0_🍽️_식당_주차장_관리.py")

        else:
            st.info("💡 왼쪽 표에서 분석을 원하는 지역을 선택해 주세요.")


if __name__ == "__main__":
    main()