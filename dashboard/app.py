import streamlit as st
from core.settings import SHP_PATH
import geopandas as gpd
from restaurant_map import render_restaurant_map, update_restaurant_status
from chajoo_heatmap import render_chajoo_map

@st.cache_resource
def load_shp():
    # 1. 파일 읽기
    gdf = gpd.read_file(SHP_PATH)

    # 2. CRS 설정 (식당 맵의 좌표 오류 방지)
    if gdf.crs is None or gdf.crs.to_epsg() != 4326:
        gdf = gdf.to_crs(epsg=4326)

    # 3. 데이터 타입 및 최적화 (차주 맵의 코드 매칭용 + 속도용)
    if "SIGUNGU_CD" in gdf.columns:
        gdf["SIGUNGU_CD"] = gdf["SIGUNGU_CD"].astype(str)

    # 성능 최적화 (0.01 정도면 시군구 경계가 뭉개지지 않으면서 매우 가벼워집니다)
    gdf["geometry"] = gdf["geometry"].simplify(tolerance=0.02, preserve_topology=True)

    # 양쪽 지도에서 필요한 모든 컬럼 유지
    return gdf[["SIGUNGU_CD", "SIGUNGU_NM", "geometry"]]

st.set_page_config(layout="wide")
st.title("🚀 식당 주차장 야간 화물 차고지")

# -------------------------
# 사이드바 필터 구성 (Form 사용)
# -------------------------
with st.sidebar.form("filter_form"):
    st.header("🔍 필터 설정")

    # 1. 지역 선택
    gdf_shp = load_shp()
    sigungu_list = sorted(gdf_shp["SIGUNGU_NM"].unique().tolist())
    try:
        default_idx = sigungu_list.index("용인시 처인구") + 1
    except ValueError:
        default_idx = 0

    selected_sigungu = st.selectbox(
        "📍 관심 지역 선택",
        options=["전체"] + sigungu_list,
        index=default_idx
    )

    st.markdown("---")

    # 2. 계약 상태 필터
    status_options = ["후보 식당", "연락 시도", "계약 성공", "계약 실패", "진행중", "기타"]
    selected_statuses = st.multiselect(
        "🤝 계약 상태 선택",
        options=status_options,
        default=status_options
    )

    # 3. 주차장 면적 필터
    min_parking = st.slider("🅿️ 최소 주차장 면적 (㎡)", 0, 500, 0)

    # 4. WDS 등급 필터
    wds_level = st.multiselect("📦 WDS 등급", options=[1, 2, 3, 4, 5], default=[1, 2, 3, 4, 5])

    # 5. 대형차 접근성 필터
    access_level = st.multiselect("🚚 대형차 접근성", options=[1, 2, 3, 4, 5], default=[1, 2, 3, 4, 5])

    # -------------------------
    # 검색 버튼 (이 버튼을 눌러야만 반영됨)
    # -------------------------
    # 위성 지도 토글 추가
    use_satellite = st.toggle("🛰️ 위성 지도 보기", value=False)

    submitted = st.form_submit_button("🔍 검색 및 지도 업데이트")

# --- 데이터 로드 (최상단에서 한 번만) ---
# 캐싱 덕분에 최초 1회만 읽고, 이후 리런 시에는 메모리에서 즉시 가져옵니다.
gdf_common = load_shp()

# -------------------------
# 레이아웃 구성
# -------------------------
col1, col2 = st.columns([6, 4])

with col1:

    st.subheader("🍳 식당 분포 지도")
    col1_1, col1_2 = st.columns([7, 3])
    with col1_1:
        st.info(f"📍 현재 **{selected_sigungu}** 지역의 식당 정보를 표시하고 있습니다.")
    with col1_2:
        use_satellite = st.toggle("🛰️ 위성 지도로 보기", value=False)

    # 1. 지도 렌더링 (이 부분은 조회용이므로 폼 밖에 두거나 안에 두어도 무관하나, 
    # 조회가 먼저 일어난 뒤 수정을 위해 폼을 시작합니다.)
    filtered_df = render_restaurant_map(
        selected_sigungu=selected_sigungu,
        selected_statuses=selected_statuses,
        min_parking=min_parking,
        wds_level=wds_level,
        access_level=access_level,
        gdf_boundary=gdf_common,
        use_satellite=use_satellite
    )

with col2:
    st.subheader("🚚 차주 분포 지도")
    st.info("전국의 등록된 화물차 대수를 알려줍니다.")
    render_chajoo_map(gdf_common)


st.markdown("---")

# ---------------------------------------------------------
# 2. st.form 도입: 셀 수정 시 리런 방지
# ---------------------------------------------------------
with st.form("batch_update_form"):
    st.subheader("📝 식당 정보 수정 (Batch Update)")

    if filtered_df is not None and not filtered_df.empty:
        # 에디터 배치 (폼 안에서는 수정해도 즉시 리런되지 않음)
        edited_df = st.data_editor(
            filtered_df,
            column_order=("restaurant_name", "road_address", "large_vehicle_access", "contract_status", "remarks"),
            column_config={
                "restaurant_name": st.column_config.Column("상호명", disabled=True),
                "road_address": st.column_config.Column("주소", disabled=True),
                "large_vehicle_access": st.column_config.SelectboxColumn(
                    "🚚 접근성", options=[1, 2, 3, 4, 5]
                ),
                "contract_status": st.column_config.SelectboxColumn(
                    "🤝 상태", options=["후보 식당", "연락 시도", "계약 성공", "계약 실패", "진행중", "기타"]
                ),
                "remarks": st.column_config.TextColumn("📝 비고", help="특이사항을 입력하세요")
            },
            hide_index=True,
            use_container_width=True,
            key="editor_inside_form" # 세션 키 변경
        )

        # 폼 전용 제출 버튼 (이 버튼을 눌러야만 리런이 발생하며 로직 실행)
        submit_btn = st.form_submit_button("💾 모든 변경사항 DB 반영", use_container_width=True)

        if submit_btn:
            # 에디터의 세션 상태에서 변경사항 확인
            # form 내부의 위젯은 st.session_state[key]로 접근 가능합니다.
            editor_state = st.session_state.editor_inside_form
            edited_rows = editor_state.get("edited_rows", {})

            if not edited_rows:
                st.warning("변경사항이 없습니다. 셀을 수정한 후 버튼을 눌러주세요.")
            else:
                with st.spinner("DB에 일괄 저장 중..."):
                    for row_idx, changes in edited_rows.items():
                        # 현재 출력된 데이터프레임에서 원본 행 식별
                        target_row = edited_df.iloc[int(row_idx)]

                        # DB 업데이트 함수 호출
                        update_restaurant_status(
                            restaurant_name=target_row["restaurant_name"],
                            road_address=target_row["road_address"],
                            new_access=int(changes.get("large_vehicle_access", target_row["large_vehicle_access"])),
                            new_status=changes.get("contract_status", target_row["contract_status"]),
                            new_remarks=changes.get("remarks", target_row.get("remarks", ""))
                        )
                
                st.success(f"✅ 총 {len(edited_rows)}건의 변경사항이 DB에 반영되었습니다!")
                # 반영 후 최신 데이터를 불러오기 위해 리런
                st.rerun()
    else:
        st.write("표시할 데이터가 없습니다.")
        # 폼을 닫기 위한 더미 버튼 (st.form 사용 시 버튼이 반드시 하나 이상 필요)
        st.form_submit_button("데이터 확인", disabled=True)
