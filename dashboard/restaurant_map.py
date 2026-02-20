import pandas as pd
import pydeck as pdk
import streamlit as st
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode
from core.query import update_restaurant

def render_restaurant_grid(display_df):

    GRID_COLUMNS = [
        "업체명",
        "유휴부지면적",
        "신뢰도점수",
        "대형차_접근성",
        "contract_status",
        "remarks",
    ]

    # ✅ index 보존
    grid_df = display_df[GRID_COLUMNS].copy()
    grid_df["_idx"] = display_df.index  # 원본 index 보관

    grid_df = grid_df.rename(columns={
        "업체명": "상호명",
        "유휴부지면적": "주차장 면적",
        "신뢰도점수": "신뢰도",
        "대형차_접근성": "대형차 접근성",
        "contract_status": "계약 상태",
        "remarks": "비고",
    })

    gb = GridOptionsBuilder.from_dataframe(grid_df)

    gb.configure_column("_idx", hide=True)  # 👈 화면에 안 보이게

    gb.configure_default_column(editable=False, resizable=True)
    gb.configure_selection(selection_mode="single", use_checkbox=False)

    gb.configure_column("상호명", pinned="left", width=300)
    gb.configure_column("주차장 면적", pinned="left", width=100)
    gb.configure_column("신뢰도", pinned="left", width=100)
    gb.configure_column("대형차 접근성", pinned="left", width=100)
    gb.configure_column("계약 상태", pinned="left", width=100)
    gb.configure_column("비고", wrapText=True, autoHeight=True)

    gb.configure_grid_options(
        domLayout="normal",
        rowHeight=42,
    )

    custom_css = {
        ".ag-root-wrapper": {
            "font-size": "16px",   # 전체 기본 글씨 크기
        },
        ".ag-header-cell-label": {
            "font-size": "16px",
            "font-weight": "600",
        },
        ".ag-header-cell": {
            "display": "flex",
            "align-items": "center",   # 헤더도 중앙
        },
        ".ag-cell": {
            "font-size": "18px",
            "display": "flex",
            "align-items": "center",   # 👈 위아래 중앙
        },
    }

    grid = AgGrid(
        grid_df,
        gridOptions=gb.build(),
        update_mode=GridUpdateMode.SELECTION_CHANGED,
        height=500,
        theme="streamlit",
        key="restaurant_picker",
        custom_css=custom_css
    )

    selected = grid.get("selected_rows")

    if selected is None:
        return

    # DataFrame 대응
    if isinstance(selected, pd.DataFrame):
        if selected.empty:
            return
        row = selected.iloc[0]
    else:
        return

    idx = row["_idx"]

    # ✅ 원본 display_df에서 위도/경도 조회
    lat = display_df.loc[idx, "latitude"]
    lon = display_df.loc[idx, "longitude"]

    new_picked = (lat, lon)

    # ✅ 처음 선택했거나, 다른 식당을 눌렀을 때만
    if st.session_state.get("picked") != new_picked:
        st.session_state["picked"] = new_picked
        st.session_state["_need_rerun"] = True

def render_restaurant_map(df, selected_shp_cd, gdf_boundary, use_satellite, mapbox_api_key):

    if df.empty:
        st.warning("조회된 식당 데이터가 없습니다.")
        return None

    # 데이터 전처리
    df["contract_status_display"] = df["contract_status"].fillna("후보")
    df["access_display"] = df["대형차_접근성"].apply(lambda x: "미입력" if pd.isna(x) else f"{int(x)}/5")
    df["remarks_display"] = df["remarks"].fillna("미입력")


    # --- [필터링: 행정 구역 경계] ---
    target_gdf = gdf_boundary[gdf_boundary["SIGUNGU_CD"] == selected_shp_cd].copy()


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


    # 2. 식당 레이어 (IconLayer)

    # 3. 구역 경계 레이어
    target_gdf = gdf_boundary[gdf_boundary["SIGUNGU_CD"] == selected_shp_cd].copy()
    boundary_layer = pdk.Layer(
        "GeoJsonLayer", target_gdf, stroked=True, filled=True,
        get_fill_color=[0, 212, 255, 30], get_line_color=[255, 255, 255, 200],
        line_width_min_pixels=2,
    )

    # 레이어 순서: 경계 -> 거점 -> 식당
    layers = [boundary_layer]

    if not df.empty:

        # 식당 데이터프레임에 전용 툴팁 생성
        df["tooltip_text"] = df.apply(lambda x: f"""
            <div style="font-family: 'Malgun Gothic', sans-serif; width: 220px; line-height: 1.6;">
                <b style="font-size:16px; color:#00d4ff;">🏠 {x['업체명']}</b><br/>
                <small style="color:#bbb;">{x['도로명주소']}</small>
                <hr style="margin:8px 0; border-color:#555;">
                <div style="font-size:13px;">
                    <b>🅿️ 주차장 면적:</b> {int(x['유휴부지면적']):,}㎡<br/>
                    <b>⭐ 신뢰도점수:</b> {x['신뢰도점수'] if pd.notna(x['신뢰도점수']) else '-'}<br/>
                    <b>🚚 접근성:</b> {x['access_display']}<br/>
                    <b>🤝 상태:</b>{x['contract_status_display']}<br/>
                    <hr style="margin:5px 0; border-style:dashed; border-color:#444;">
                    <b>📝 비고:</b> <i style="color:#ddd;">{x['remarks_display']}</i>
                </div>
            </div>
        """, axis=1)
        restaurant_layer = pdk.Layer(
            "IconLayer",
            df,
            id="restaurant-layer", # 툴팁 식별을 위한 ID,
            get_icon="icon_data",
            get_position='[longitude, latitude]',
            get_color="color",
            get_size=4,
            size_scale=8,
            pickable=True,
        )

        layers.append(restaurant_layer)


    # -------------------------------------------------------
    # 1️⃣ 우선: 클릭한 식당이 있으면 그 위치로 이동
    # -------------------------------------------------------
    if "picked" in st.session_state:
        picked_lat, picked_lon = st.session_state["picked"]

        if picked_lat is not None and picked_lon is not None:
            avg_lat = float(picked_lat)
            avg_lon = float(picked_lon)
            initial_zoom = 17
            map_key = f"map-{avg_lat:.5f}-{avg_lon:.5f}"
        else:
            map_key = "map-default"
    # -------------------------------------------------------
    # 2️⃣ 없으면 기존 행정구 중심
    # -------------------------------------------------------
    elif not target_gdf.empty:
        projected_gdf = target_gdf.to_crs(epsg=5179)
        centroids = projected_gdf.geometry.centroid.to_crs(epsg=4326)

        avg_lat = centroids.y.mean()
        avg_lon = centroids.x.mean()
        initial_zoom = 9.6
        map_key = "map-default"
    else:
        avg_lat, avg_lon = 37.24, 127.17
        initial_zoom = 9
        map_key = "map-default"

    st.pydeck_chart(pdk.Deck(
            layers=layers,
            initial_view_state=pdk.ViewState(
                latitude=avg_lat, longitude=avg_lon, zoom=initial_zoom, pitch=0
            ),
            height=600,
            map_style="mapbox://styles/mapbox/dark-v11" if not use_satellite else "mapbox://styles/mapbox/satellite-streets-v12",
            api_keys={"mapbox": mapbox_api_key},
            tooltip={
                "html": "{tooltip_text}",
                "style": {
                    "backgroundColor": "rgba(33, 33, 33, 0.95)",
                    "color": "white",
                    "borderRadius": "5px"
                }
            },
        ),
        key=map_key
    )


    return df

def render_restaurant_editor(df):
    """하단 데이터 수정 에디터 및 신고 기능을 제공합니다."""

    if df is None or df.empty:
        st.info("해당 지역에 후보 식당이 없습니다.")
        return

    if "save_msg" in st.session_state:
        st.success(st.session_state.save_msg)
        del st.session_state.save_msg

    display_df = df.copy()

    with st.form("batch_update_form"):
        edited_df = st.data_editor(
            display_df,
            column_order=("업체명", "유휴부지면적", "신뢰도점수", "대형차_접근성", "contract_status", "remarks"),
            column_config={
                "업체명": st.column_config.Column("상호명", disabled=True),
                "유휴부지면적": st.column_config.Column("주차장 면적", disabled=True),
                "신뢰도점수": st.column_config.Column("신뢰도", disabled=True),
                # 1. 접근성: 사용자가 선택을 해제하거나 지울 수 있음
                "대형차_접근성": st.column_config.SelectboxColumn("대형차 접근성", options=[1, 2, 3, 4, 5]),
                # 2. 계약 상태: 반드시 정해진 옵션 중 하나 (기본값 설정 권장)
                "contract_status": st.column_config.SelectboxColumn(
                    "계약 상태",
                    options=["후보 식당", "계약 성공", "계약 실패"],
                    required=True  # 필수 선택으로 설정
                ),
                "remarks": st.column_config.TextColumn("📝 비고"),
            },
            hide_index=True, width="stretch", height=350, key="editor_inside_form"
        )

        submit_btn = st.form_submit_button("💾 변경사항 반영", use_container_width=True)

        if submit_btn:
            editor_state = st.session_state.editor_inside_form
            edited_rows = editor_state.get("edited_rows", {})

            if not edited_rows:
                st.warning("변경사항이 없습니다.")
            else:
                with st.spinner("데이터 처리 중..."):
                    update_count = 0

                    for row_idx, changes in edited_rows.items():
                        target_row = display_df.iloc[int(row_idx)]

                        # --- [1. 대형차 접근성 처리: None 허용] ---
                        raw_access = changes.get("대형차_접근성", target_row.get("대형차_접근성"))
                        # NaN, "미입력", 혹은 에디터에서 지워진 경우(None) 처리
                        if pd.isna(raw_access) or raw_access == "미입력" or raw_access is None:
                            access_val = None
                        else:
                            access_val = int(float(raw_access))

                        # --- [2. 계약 상태 처리: 옵션 강제] ---
                        # 수정사항이 있으면 그것을 쓰고, 없으면 기존 값을 유지 (기존 값도 옵션 중 하나임)
                        status_val = changes.get("contract_status", target_row["contract_status"])
                        if status_val not in ["후보 식당", "계약 성공", "계약 실패"]:
                            status_val = "후보 식당" # 잘못된 값이 들어올 경우의 방어 로직

                        # --- [3. 비고 처리: None 허용] ---
                        # 수정사항이 없으면 기존 remarks를 가져오고, 그 값이 NaN이면 None으로 변환
                        raw_remarks = changes.get("remarks", target_row.get("remarks", ""))
                        remarks_val = None if (pd.isna(raw_remarks) or str(raw_remarks).strip() == "") else raw_remarks

                        # DB 업데이트 함수 호출
                        update_restaurant(
                            name=target_row["업체명"],
                            address=target_row["도로명주소"],
                            access=access_val,
                            status=status_val,
                            remarks=remarks_val
                        )
                        update_count += 1

                msg = f"✅ {update_count}건 업데이트 완료"
                st.session_state.save_msg = msg
                st.rerun()

    return