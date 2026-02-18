import pandas as pd
import geopandas as gpd
import pydeck as pdk
import streamlit as st
from core.query import load_restaurants, update_restaurant_status

# ------------------------------------------------------------------------------
# 지도 렌더링 함수
# ------------------------------------------------------------------------------
def render_restaurant_map(selected_sigungu, selected_shp_cd, selected_statuses, 
                          min_parking, access_level, gdf_boundary, 
                          use_satellite=False, mapbox_api_key=None):

    df = load_restaurants()
    if df.empty:
        st.warning("조회된 식당 데이터가 없습니다.")
        return None

    # 데이터 전처리
    df["contract_status_display"] = df["contract_status"].fillna("미입력")
    df["access_display"] = df["large_vehicle_access"].apply(lambda x: "미입력" if pd.isna(x) else f"{int(x)}/5")
    df["remarks_display"] = df["remarks"].fillna("미입력")

    # --- [필터링: 행정 구역 경계] ---
    target_gdf = gdf_boundary[gdf_boundary["SIGUNGU_CD"] == selected_shp_cd].copy()

    if not target_gdf.empty:
        # 선택된 지역에 대한 강조 색상 (RGBA)
        target_gdf["fill_color"] = [[0, 212, 255, 40]] * len(target_gdf)
        target_gdf["line_color"] = [[0, 212, 255, 200]] * len(target_gdf)
    else:
        # 데이터가 없을 경우 빈 컬럼 생성 (에러 방지용)
        target_gdf["fill_color"] = None
        target_gdf["line_color"] = None

    # --- [필터링: 식당 데이터] ---
    # 1. 주소 텍스트 필터
    search_terms = selected_sigungu.split()
    df = df[df["도로명주소"].apply(lambda x: all(term in str(x) for term in search_terms))]

    # 2. 계약 상태 필터
    if selected_statuses:
        df = df[df["contract_status_display"].isin(selected_statuses)]

    # 3. 유휴면적 필터
    df = df[df["유휴부지면적"].fillna(0) >= min_parking]

    # 4. 접근성 필터
    if access_level:
        formatted_levels = [f"{val}/5" if val != "미입력" else val for val in access_level]
        df = df[df["access_display"].isin(formatted_levels)]

    # --- [레이어 설정] ---
    color_map = {
        "미입력": [72, 141, 247],
        "후보 식당": [255, 215, 0, 200],
        "계약 성공": [76, 175, 80, 220],
        "계약 실패": [244, 67, 54, 220],
    }
    df["color"] = df["contract_status_display"].apply(lambda x: color_map.get(x, color_map["미입력"]))

    # 아이콘 및 툴팁 HTML
    ICON_URL = "https://img.icons8.com/ios-filled/50/ffffff/marker.png"
    df["icon_data"] = [{"url": ICON_URL, "width": 128, "height": 128, "anchorY": 128, "mask": True} for _ in range(len(df))]

    tooltip_html = """
    <div style="font-family: 'Malgun Gothic', sans-serif; padding: 10px;">
        <b style="font-size:15px; color:#00d4ff;">{업체명}</b><br/>
        <small style="color:#bbb;">{도로명주소}</small><hr style="margin:5px 0; border-color:#555;">
        <b>🅿️ 유휴면적:</b> {유휴부지면적}㎡ (신뢰도: {신뢰도점수})<br/>
        <b>🚚 접근성:</b> {access_display}<br/>
        <b>🤝 상태:</b> {contract_status_display}<br/>
        <b>🗒️ 비고:</b> {remarks_display}
    </div>
    """

    layers = [
        pdk.Layer(
            "GeoJsonLayer", target_gdf, stroked=True, filled=True,
            get_fill_color="fill_color", get_line_color=[255, 255, 255, 200],
            line_width_min_pixels=2,
        ),
        pdk.Layer(
            "IconLayer", df, get_icon="icon_data", get_position='[longitude, latitude]',
            get_color="color", get_size=4, size_scale=8, pickable=True,
        )
    ]

    # --- [지도 뷰 설정: SHP 경계 중심점 기준] ---
    if not target_gdf.empty:
        avg_lat = target_gdf.geometry.centroid.y.mean()
        avg_lon = target_gdf.geometry.centroid.x.mean()
        initial_zoom = 9.6
    else:
        avg_lat, avg_lon = 37.24, 127.17
        initial_zoom = 9

    st.pydeck_chart(pdk.Deck(
        layers=layers,
        initial_view_state=pdk.ViewState(
            latitude=avg_lat, longitude=avg_lon, zoom=initial_zoom, pitch=0
        ),
        map_style="mapbox://styles/mapbox/dark-v11" if not use_satellite else "mapbox://styles/mapbox/satellite-streets-v12",
        tooltip={"html": tooltip_html, "style": {"backgroundColor": "rgba(33, 33, 33, 0.9)", "color": "white"}}
    ))
    return df

# ------------------------------------------------------------------------------
# 4. 데이터 에디터 섹션
# ------------------------------------------------------------------------------
def render_restaurant_editor(filtered_df):
    """하단 데이터 수정 에디터 및 일괄 업데이트 기능을 제공합니다."""
    st.subheader("📝 식당 정보 수정 (Batch Update)")

    if "save_msg" in st.session_state:
        st.success(st.session_state.save_msg)
        del st.session_state.save_msg

    if filtered_df is None or filtered_df.empty:
        st.info("수정할 데이터가 없습니다. 필터를 통해 식당을 검색해주세요.")
        return

    with st.form("batch_update_form"):
        edited_df = st.data_editor(
            filtered_df,
            column_order=("업체명", "도로명주소", "유휴부지면적", "신뢰도점수", "large_vehicle_access", "contract_status", "remarks"),
            column_config={
                "업체명": st.column_config.Column("상호명", disabled=True),
                "도로명주소": st.column_config.Column("주소", disabled=True),
                "유휴부지면적": st.column_config.Column("주차장 면적", disabled=True),
                "신뢰도점수": st.column_config.Column("신뢰도", disabled=True),
                "large_vehicle_access": st.column_config.SelectboxColumn("🚚 접근성", options=[1, 2, 3, 4, 5]),
                "contract_status": st.column_config.SelectboxColumn("🤝 상태", options=["미입력", "후보 식당", "계약 성공", "계약 실패"]),
                "remarks": st.column_config.TextColumn("📝 비고")
            },
            hide_index=True, width="stretch", key="editor_inside_form"
        )

        submit_btn = st.form_submit_button("💾 모든 변경사항 DB 반영", use_container_width=True)

        if submit_btn:
            editor_state = st.session_state.editor_inside_form
            edited_rows = editor_state.get("edited_rows", {})

            if not edited_rows:
                st.warning("변경사항이 없습니다. 수정한 뒤 버튼을 눌러주세요.")
            else:
                with st.spinner("데이터 저장 중..."):
                    for row_idx, changes in edited_rows.items():
                        target_row = filtered_df.iloc[int(row_idx)]

                        # 데이터 정규화 및 업데이트
                        raw_access = changes.get("large_vehicle_access", target_row["large_vehicle_access"])
                        new_access = None if pd.isna(raw_access) else int(raw_access)

                        update_restaurant_status(
                            name=target_row["업체명"],
                            address=target_row["도로명주소"],
                            access=new_access,
                            status=changes.get("contract_status", target_row["contract_status"]),
                            remarks=changes.get("remarks", target_row.get("remarks", ""))
                        )

                st.session_state.save_msg = f"✅ 총 {len(edited_rows)}건의 변경사항이 성공적으로 반영되었습니다!"
                st.rerun()