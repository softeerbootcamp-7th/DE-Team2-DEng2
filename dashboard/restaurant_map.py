import streamlit as st
import pandas as pd
import geopandas as gpd
import pydeck as pdk
from core.db import get_engine
from sqlalchemy import text
from core.settings import SHP_PATH

# 식당 정보 업데이트 함수 (remarks 추가 버전)
def update_restaurant_status(restaurant_name, road_address, new_access, new_status, new_remarks):
    engine = get_engine()
    with engine.begin() as conn:
        query = text("""
            UPDATE restaurant_for_db
            SET large_vehicle_access = :access,
                contract_status = :status,
                remarks = :remarks  -- 추가된 부분
            WHERE restaurant_name = :name AND road_address = :address
        """)
        conn.execute(query, {
            "access": new_access,
            "status": new_status,
            "remarks": new_remarks,
            "name": restaurant_name,
            "address": road_address
        })
    st.cache_data.clear()

# 데이터 로드 시 remarks 컬럼도 가져오기
@st.cache_data
def load_restaurants():
    return pd.read_sql(
        """
        SELECT
            restaurant_name, road_address, owner_name, longitude, latitude,
            total_parking_area, wds, large_vehicle_access, contract_status, remarks
        FROM restaurant_for_db
        WHERE longitude IS NOT NULL AND latitude IS NOT NULL
        """,
        get_engine()
    )

# 1. 시군구 경계 로드 (내부 채우기 없음)
@st.cache_resource
def load_shp():
    gdf = gpd.read_file(SHP_PATH)
    if gdf.crs is None or gdf.crs.to_epsg() != 4326:
        gdf = gdf.to_crs(epsg=4326)
    # 성능 최적화
    gdf["geometry"] = gdf["geometry"].simplify(tolerance=0.005, preserve_topology=True)
    return gdf[["SIGUNGU_NM", "geometry"]]


# -----------------------------
# 데이터 로드
# -----------------------------
@st.cache_data
def load_restaurants():
    return pd.read_sql(
        """
        SELECT
            restaurant_name, road_address, owner_name, longitude, latitude,
            total_parking_area, wds, large_vehicle_access, contract_status, remarks
        FROM restaurant_for_db
        WHERE longitude IS NOT NULL AND latitude IS NOT NULL
        """,
        get_engine()
    )



def render_restaurant_map(selected_sigungu, selected_statuses, min_parking, wds_level, access_level, 
                          gdf_boundary,  # 외부에서 받아옴
                          use_satellite=False):

    df = load_restaurants() # SQL 쿼리에 remarks가 포함되어 있어야 함

    if df.empty:
        st.warning("데이터가 없습니다.")
        return

    # -------------------------
    # 1. 시군구 하이라이트 색상 설정
    # -------------------------
    gdf_boundary["fill_color"] = [[0, 0, 0, 0]] * len(gdf_boundary) # 초기화

    if selected_sigungu != "전체":
        mask = gdf_boundary["SIGUNGU_NM"].str.contains(selected_sigungu, na=False)
        target_count = mask.sum()
        if target_count > 0:
            # 수정: [[...]] * n 을 Series로 감싸서 전달
            gdf_boundary.loc[mask, "fill_color"] = pd.Series([[0, 212, 255, 40]] * target_count, index=gdf_boundary.index[mask])
            gdf_boundary.loc[mask, "line_color"] = pd.Series([[255, 255, 255, 200]] * target_count, index=gdf_boundary.index[mask])

        # 나머지(하이라이트 되지 않은 부분) 선 색상 처리
        gdf_boundary.loc[~mask, "line_color"] = pd.Series([[255, 255, 255, 60]] * (~mask).sum(), index=gdf_boundary.index[~mask])
    else:
        gdf_boundary["line_color"] = [[255, 255, 255, 60]] * len(gdf_boundary)

    # -------------------------
    # 2. 필터링 로직 적용
    # -------------------------
    # 지역 필터
    if selected_sigungu != "전체":
        df = df[df["road_address"].str.contains(selected_sigungu, na=False)]

    # 계약 상태 필터
    if selected_statuses:
        # contract_status의 결측치를 '기타'로 채운 결과가 selected_statuses에 포함되는지 확인
        df = df[df['contract_status'].fillna('기타').isin(selected_statuses)]

    # 주차장 면적 필터
    df = df[df["total_parking_area"].fillna(0) >= min_parking]

    # WDS 등급 필터
    if wds_level:
        df = df[df["wds"].fillna(0).isin(wds_level)]

    # 대형차 접근성 필터
    if access_level:
        df = df[df["large_vehicle_access"].fillna(0).isin(access_level)]

    if df.empty:
        st.error("선택한 필터 조건에 맞는 식당이 없습니다.")
        return

    # -------------------------
    # 3. 시각화 설정 (색상 및 아이콘)
    # -------------------------
    color_map = {
        "후보 식당": [255, 235, 59, 230],    # 🟡 노랑
        "연락 시도": [33, 150, 243, 230],    # 🔵 파랑
        "계약 성공": [76, 175, 80, 230],     # 🟢 초록
        "계약 실패": [244, 67, 54, 230],     # 🔴 빨강
        "진행중": [255, 152, 0, 230],       # 🟠 주황
        "기타": [156, 39, 176, 230]          # 🟣 보라
    }
    df["color"] = df["contract_status"].apply(lambda x: color_map.get(x, color_map["기타"]))

    ICON_URL = "https://img.icons8.com/ios-filled/50/ffffff/marker.png"
    icon_data = {"url": ICON_URL, "width": 128, "height": 128, "anchorY": 128, "mask": True}
    df["icon_data"] = [icon_data for _ in range(len(df))]

    # -------------------------
    # 4. 툴팁 HTML 정의 (remarks 데이터 매핑 확인)
    # -------------------------
    tooltip_html = """
    <div style="font-family: 'Malgun Gothic', sans-serif; padding: 10px; min-width: 200px;">
        <div style="font-size: 16px; font-weight: bold; color: #00d4ff; margin-bottom: 5px;">
            {restaurant_name}
        </div>
        <div style="font-size: 12px; color: #bbb; margin-bottom: 10px; border-bottom: 1px solid #555; padding-bottom: 5px;">
            {road_address}
        </div>
        <div style="font-size: 13px; line-height: 1.6;">
            <b>👤 대표자:</b> {owner_name}<br/>
            <b>🅿️ 주차면적:</b> {total_parking_area} ㎡<br/>
            <b>📦 WDS:</b> {wds}<br/>
            <b>🚚 대형차 접근성:</b> {large_vehicle_access} / 5<br/>
            <b>🤝 계약상태:</b> {contract_status} <br/>
            <b>🗒️ 비고:</b> {remarks}
        </div>
    </div>
    """

    # -------------------------
    # 5. Pydeck 지도 생성 및 출력
    # -------------------------
    if use_satellite:
        current_map_style = "mapbox://styles/mapbox/satellite-streets-v11" # 도로 정보 포함 위성
        line_boundary_color = [255, 255, 255, 200] # 위성 지도에선 경계선이 더 진해야 보임
    else:
        current_map_style = "mapbox://styles/mapbox/dark-v11" # 다크 모드
        line_boundary_color = [255, 255, 255, 60]

    layers = [
        pdk.Layer(
            "GeoJsonLayer",
            gdf_boundary,
            stroked=True,
            filled=True,
            get_fill_color="fill_color",
            get_line_color=line_boundary_color,
            line_width_min_pixels=1,
        ),
        pdk.Layer(
            "IconLayer",
            df,
            get_icon="icon_data",
            get_position='[longitude, latitude]',
            get_color="color",
            get_size=4,
            size_scale=10,
            pickable=True,
        )
    ]

    st.pydeck_chart(
        pdk.Deck(
            layers=layers,
            initial_view_state=pdk.ViewState(
                latitude=df["latitude"].mean() if not df.empty else 37.2,
                longitude=df["longitude"].mean() if not df.empty else 127.2,
                zoom=10 if selected_sigungu != "전체" else 7,
            ),
            map_style=current_map_style,
            tooltip={"html": tooltip_html, "style": {"backgroundColor": "rgba(33, 33, 33, 0.9)", "color": "white"}}
        ),
        use_container_width=True
    )

    st.markdown("🟡 **후보** | 🔵 **연락시도** | 🟢 **성공** | 🔴 **실패** | 🟠 **진행중** | 🟣 **기타**")

    return df # app.py의 데이터 에디터로 전달됨