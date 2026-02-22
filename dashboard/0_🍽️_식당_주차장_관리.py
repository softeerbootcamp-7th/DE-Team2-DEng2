import os

import geopandas as gpd
import streamlit as st
from dotenv import load_dotenv

from core.query import load_restaurants, get_last_viewed_sigungu, save_view_history, load_chajoo_data, load_contract_restaurants
from restaurant_map import render_restaurant_editor, render_restaurant_map, render_restaurant_grid, render_contract_management

from shp_loader import load_shp

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

    /* 좁은 화면: 그리드+지도 세로 스택 */
    @media (max-width: 1000px) {
        [data-testid="stHorizontalBlock"] {
            flex-direction: column !important;
        }
        [data-testid="stHorizontalBlock"] > div {
            width: 100% !important;
            flex: 1 1 100% !important;
        }
    }

    /* 작은 화면 폰트 축소 */
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

    /* 중간 화면 */
    @media (min-width: 1201px) and (max-width: 1600px) {
        .ag-header-cell-label {
            font-size: 17px !important;
        }
        .ag-cell {
            font-size: 18px !important;
        }
    }

    /* PyDeck 지도 반응형 높이 */
    iframe[title="pydeck_chart"] {
        max-height: 70vh !important;
    }

    /* AgGrid 반응형 높이 */
    .ag-root-wrapper {
        max-height: 65vh !important;
    }
    </style>
""", unsafe_allow_html=True)

# ------------------------------------------------------------------------------
# 1. Target 지역 설정
# ------------------------------------------------------------------------------

# 차주 데이터 로드
df_chajoo_raw, _, _ = load_chajoo_data()

# '지역명' 컬럼 생성 (시도 + 시군구) 후 score 기준 내림차순 정렬
df_chajoo_raw["full_sigungu"] = df_chajoo_raw["sido"] + " " + df_chajoo_raw["sigungu"]
df_sorted = df_chajoo_raw.sort_values(by="score", ascending=False)

# 중복 제거 후 순서(Score 높은 순) 유지한 리스트 생성
sigungu_options = df_sorted["full_sigungu"].unique().tolist()


# [Step 1] 초기 세션 상태 설정 (세션 -> DB 순서)
if "target_sigungu" not in st.session_state:
    db_last_viewed = get_last_viewed_sigungu()
    
    # DB 기록이 리스트에 있으면 사용, 없으면 점수가 가장 높은(첫 번째) 지역 사용
    if db_last_viewed in sigungu_options:
        st.session_state["target_sigungu"] = db_last_viewed
    else:
        st.session_state["target_sigungu"] = sigungu_options[0]

# [Step 2] 사이드바 셀렉트박스 배치
with st.sidebar:
    st.header("📍 지역 설정")
    st.caption("🔥 전략적 중요도가 높은 순서입니다.")

    # 현재 세션값의 인덱스 찾기
    current_index = sigungu_options.index(st.session_state["target_sigungu"])

    selected_sigungu = st.selectbox(
        "대상 시군구를 선택하세요",
        options=sigungu_options,
        index=current_index,
        help="리스트 상단일수록 차주 활동이 활발한 지역입니다."
    )

    # [Step 3] 사용자가 지역을 변경했을 경우 처리
    if selected_sigungu != st.session_state["target_sigungu"]:
        st.session_state["target_sigungu"] = selected_sigungu
        # 변경된 지역을 DB에 최종 업데이트
        save_view_history(selected_sigungu)
        # 즉시 반영을 위해 페이지 리런
        st.rerun()


TARGET_SIGUNGU = st.session_state["target_sigungu"]

save_view_history(TARGET_SIGUNGU) 

# ------------------------------------------------------------------------------
# 2. 메인 대시보드 레이아웃
# ------------------------------------------------------------------------------
def main():


    st.title(f"🍽️ {TARGET_SIGUNGU}의 후보 식당")
    st.divider()
    # 1. 세션 상태 초기화 (최초 실행 시)
    if "picked" not in st.session_state:
        st.session_state["picked"] = None

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
        st.markdown(f"""
            <div style="display: flex; align-items: center; gap: 10px;">
                <h3 style="margin: 0;">📊 Top 15</h3>
            </div>
        """, unsafe_allow_html=True)

        # # (선택 사항) 주요 지표 요약
        # m1, m2, m3 = st.columns(3)
        # with m1:
        #     st.metric("평균 수익성", f"{df['수익성'].mean():.1f}점")
        # with m2:
        #     st.metric("평균 성공확률", f"{df['영업_적합도'].mean():.1f}%")
        # with m3:
        #     st.metric("최대 면적", f"{df['유휴부지_면적'].max():,.0f}㎡")

        # 그리드 함수 호출
        render_restaurant_grid(df)

    with col_map:
        map_header_col1, map_header_col2 = st.columns([7, 3])
        with map_header_col1:
            st.markdown(f"""
                <div style="display: flex; align-items: center; gap: 8px; flex-wrap: wrap;">
                    <h3 style="margin: 0; font-size: clamp(16px, 1.5vw, 24px);">📍 위치 및 접근성 확인</h3>
                    <div style="display: flex; align-items: center; color: #6c757d; font-size: clamp(11px, 0.9vw, 14px);">
                        <span style="margin-right: 4px;">🖱️</span>
                        <span><b>리스트 클릭 시</b> 해당 위치로 지도가 이동합니다.</span>
                    </div>
                </div>
            """, unsafe_allow_html=True)

        with map_header_col2:

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
    render_contract_management(TARGET_SIGUNGU)

if __name__ == "__main__":
    main()