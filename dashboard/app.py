import os
from pathlib import Path

import geopandas as gpd
import streamlit as st
from dotenv import load_dotenv

from chajoo_heatmap import load_chajoo_data, render_chajoo_map
from core.settings import SHP_PATH
from restaurant_map import render_restaurant_editor, render_restaurant_map

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

# 세션 상태 초기화
if "selected_sigungu" not in st.session_state:
    st.session_state.selected_sigungu = "경기 용인시 처인구"
if "selected_statuses" not in st.session_state:
    st.session_state.selected_statuses = ["미입력", "후보 식당", "계약 성공", "계약 실패"]
if "min_parking" not in st.session_state:
    st.session_state.min_parking = 0
if "access_level" not in st.session_state:
    st.session_state.access_level = ["미입력", "1", "2", "3", "4", "5"]


# ------------------------------------------------------------------------------
# 2. 데이터 로딩 함수 (Caching)
# ------------------------------------------------------------------------------
@st.cache_resource
def load_shp():
    """시군구 경계 SHP 파일을 로드하고 최적화합니다."""
    gdf = gpd.read_file(SHP_PATH)

    # CRS 설정 (WGS84)
    if gdf.crs is None or gdf.crs.to_epsg() != 4326:
        gdf = gdf.to_crs(epsg=4326)

    # 데이터 타입 통일 및 최적화
    if "SIGUNGU_CD" in gdf.columns:
        gdf["SIGUNGU_CD"] = gdf["SIGUNGU_CD"].astype(str)

    # 지도 렌더링 성능을 위해 지오메트리 단순화
    gdf["geometry"] = gdf["geometry"].simplify(tolerance=0.005, preserve_topology=True)

    return gdf[["SIGUNGU_CD", "SIGUNGU_NM", "geometry"]]


# ------------------------------------------------------------------------------
# 3. 사이드바 필터 레이아웃
# ------------------------------------------------------------------------------
with st.sidebar:
    st.header("🔍 필터 설정")

    # 지역 목록 준비
    df_chajoo, latest_year, latest_month = load_chajoo_data()
    df_chajoo["full_address"] = df_chajoo["sido"] + " " + df_chajoo["sigungu"]
    sigungu_list = sorted(df_chajoo["full_address"].dropna().unique().tolist())

    # 폼을 사용한 일괄 필터링
    with st.form("filter_form"):
        # 1. 시군구 선택
        try:
            curr_idx = sigungu_list.index(st.session_state.selected_sigungu)
        except (ValueError, AttributeError):
            curr_idx = 0

        selected_sigungu = st.selectbox(
            "📍 관심 지역 선택",
            options=sigungu_list,
            index=curr_idx,
            key="temp_sigungu"
        )

        # 2. 계약 상태 필터
        selected_statuses = st.multiselect(
            "🤝 계약 상태 선택",
            options=["미입력", "후보 식당", "계약 성공", "계약 실패"],
            default=st.session_state.selected_statuses,
            key="temp_statuses"
        )

        # 3. 주차장 면적 필터
        min_parking = st.slider(
            "🅿️ 최소 주차장 면적 (㎡)", 
            0, 500, 
            value=st.session_state.min_parking,
            key="temp_parking"
        )

        # 4. 대형차 접근성 필터
        access_level = st.multiselect(
            "🚚 대형차 접근성", 
            options=["미입력", "1", "2", "3", "4", "5"], 
            default=st.session_state.access_level,
            key="temp_access"
        )

        submitted = st.form_submit_button("🔍 검색 및 지도 업데이트")

    # 폼 제출 로직 처리 (선택된 데이터 저장)
    if submitted:
        st.session_state.selected_sigungu = selected_sigungu
        st.session_state.selected_statuses = selected_statuses
        st.session_state.min_parking = min_parking
        st.session_state.access_level = access_level

    # SHP 코드 매칭 (폼 외부에서 로직 처리)
    selected_shp_cd = None
    if st.session_state.selected_sigungu != "전체":
        matching_row = df_chajoo[df_chajoo["full_address"] == st.session_state.selected_sigungu]
        if not matching_row.empty:
            selected_shp_cd = matching_row["shp_cd"].iloc[0]
    st.session_state.selected_shp_cd = selected_shp_cd


# ------------------------------------------------------------------------------
# 4. 메인 대시보드 레이아웃
# ------------------------------------------------------------------------------
st.title("🚀 식당 주차장 야간 화물 차고지 대시보드")
gdf_shp = load_shp()

col1, col2 = st.columns([6, 4])

# --- 왼쪽 컬럼: 식당 분포 지도 ---
with col1:
    st.subheader("🍳 식당 분포 지도")
    
    # 상단 뱃지 및 컨트롤러
    c1, c2 = st.columns([7, 3])
    with c1:
        st.info(f"📍 현재 **{st.session_state.selected_sigungu}** 지역을 탐색 중입니다.")
    with c2:
        use_satellite = st.toggle("🛰️ 위성 지도 모드", value=False)

    # 지도 렌더링 (현재 세션 상태 기준)
    filtered_df = render_restaurant_map(
        selected_sigungu=st.session_state.selected_sigungu,
        selected_shp_cd=st.session_state.selected_shp_cd,
        selected_statuses=st.session_state.selected_statuses,
        min_parking=st.session_state.min_parking,
        access_level=st.session_state.access_level,
        gdf_boundary=gdf_shp,
        use_satellite=use_satellite,
        mapbox_api_key=MAPBOX_API_KEY
    )

# --- 오른쪽 컬럼: 차주 분포 히트맵 ---
with col2:
    st.subheader("🚚 차주 분포 지도")
    st.info(f"전국 등록 화물차 통계 ({latest_year}년 {latest_month}월)")
    render_chajoo_map(gdf_shp, mapbox_api_key=MAPBOX_API_KEY)

# --- 하단 섹션: 데이터 에디터 ---
st.markdown("---")
render_restaurant_editor(filtered_df)