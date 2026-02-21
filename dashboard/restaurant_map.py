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

    # 그리드 렌더링
    grid = AgGrid(
        grid_df,
        gridOptions=gb.build(),
        update_mode=GridUpdateMode.SELECTION_CHANGED,
        key="restaurant_picker", # 키 고정 필수!
        theme="streamlit",
        height=500,
        custom_css=custom_css
    )

    selected = grid.get("selected_rows")

    # 선택된 행이 있을 때만 세션 업데이트
    if isinstance(selected, pd.DataFrame) and not selected.empty:
        row = selected.iloc[0]
        # AgGrid의 인덱스 대신 '상호명'으로 원본 데이터 재조회 (더 안전함)
        target_name = row["상호명"]
        actual_match = display_df[display_df["업체명"] == target_name]

        if not actual_match.empty:
            idx = actual_match.index[0]
            new_lat = actual_match.iloc[0]["latitude"]
            new_lon = actual_match.iloc[0]["longitude"]
            new_picked = (new_lat, new_lon)

            # 좌표가 실제로 바뀌었을 때만 세션 갱신 및 리런 예고
            if st.session_state.get("picked") != new_picked:
                st.session_state["picked"] = new_picked
                st.session_state["editing_data"] = display_df.loc[[idx], GRID_COLUMNS]
                st.session_state["editing_idx"] = idx

def render_restaurant_map(df, selected_shp_cd, gdf_boundary, mapbox_api_key):
    # --- [1. 데이터 전처리] ---
    # df가 비어있지 않을 때만 데이터 가공 수행
    if not df.empty:
        df["contract_status_display"] = df["contract_status"].fillna("후보")
        df["access_display"] = df["대형차_접근성"].apply(lambda x: "미입력" if pd.isna(x) else f"{int(x)}/5")
        df["remarks_display"] = df["remarks"].fillna("미입력")

        color_map = {
            "미입력": [72, 141, 247],
            "후보 식당": [255, 215, 0, 200],
            "계약 성공": [76, 175, 80, 220],
            "계약 실패": [244, 67, 54, 220],
        }
        df["color"] = df["contract_status_display"].apply(lambda x: color_map.get(x, color_map["미입력"]))

        ICON_URL = "https://img.icons8.com/ios-filled/50/ffffff/marker.png"
        df["icon_data"] = [{"url": ICON_URL, "width": 128, "height": 128, "anchorY": 128, "mask": True} for _ in range(len(df))]
        
        # 툴팁 생성
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

    # --- [2. 레이어 구성] ---
    layers = []

    # 구역 경계 레이어 (식당 유무와 상관없이 생성)
    target_gdf = gdf_boundary[gdf_boundary["SIGUNGU_CD"] == selected_shp_cd].copy()

    if not target_gdf.empty:
        boundary_layer = pdk.Layer(
            "GeoJsonLayer", target_gdf, 
            stroked=True, filled=True,
            get_fill_color=[0, 212, 255, 30], 
            get_line_color=[255, 255, 255, 200],
            line_width_min_pixels=2,
        )
        layers.append(boundary_layer)

    # 식당 레이어 (데이터가 있을 때만 추가)
    if not df.empty:
        restaurant_layer = pdk.Layer(
            "IconLayer",
            df,
            id="restaurant-layer",
            get_icon="icon_data",
            get_position='[longitude, latitude]',
            get_color="color",
            get_size=4,
            size_scale=8,
            pickable=True,
        )
        layers.append(restaurant_layer)
    else:
        # 지도는 띄우되 데이터가 없음을 알림 (지도 위에 겹치지 않게 안내만)
        st.warning("해당 구역에 등록된 식당 후보지가 없습니다.")

    # --- [3. 뷰포트 설정] ---
    # 1순위: 선택된(Picked) 식당 위치
    if st.session_state.get("picked") is not None:
        picked_lat, picked_lon = st.session_state["picked"]
        avg_lat, avg_lon = float(picked_lat), float(picked_lon)
        initial_zoom = 17
        map_key = f"map-{avg_lat:.5f}-{avg_lon:.5f}"

    # 2순위: 행정구 경계 중심
    elif not target_gdf.empty:
        projected_gdf = target_gdf.to_crs(epsg=5179)
        centroids = projected_gdf.geometry.centroid.to_crs(epsg=4326)
        avg_lat = centroids.y.mean()
        avg_lon = centroids.x.mean()
        initial_zoom = 10 # 9.6에서 조금 더 확대 (경계선을 잘 보기 위해)
        map_key = f"map-boundary-{selected_shp_cd}"

    # 3순위: 기본값
    else:
        avg_lat, avg_lon = 37.24, 127.17
        initial_zoom = 9
        map_key = "map-default"

    # --- [4. 지도 렌더링] ---
    is_satellite = st.session_state.get("use_satellite_toggle", False)
    st.pydeck_chart(pdk.Deck(
            layers=layers,
            initial_view_state=pdk.ViewState(
                latitude=avg_lat, longitude=avg_lon, zoom=initial_zoom, pitch=0
            ),
            height=600,
            map_style="mapbox://styles/mapbox/dark-v11" if not is_satellite else "mapbox://styles/mapbox/satellite-streets-v12",
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

def render_restaurant_editor(full_df):
    """선택된 1개의 식당 정보만 수정할 수 있는 전용 에디터를 제공합니다."""
    
    # 1. 세션에 선택된 식당 데이터가 있는지 확인
    if "editing_data" not in st.session_state:
        st.subheader("📝 식당 정보 수정")
        st.info("💡 위 리스트에서 식당을 선택하면 상세 정보를 수정할 수 있습니다.")
        return

    # 세션에서 데이터 가져오기
    editing_df = st.session_state["editing_data"]
    target_name = editing_df["업체명"].iloc[0]
    target_idx = st.session_state.get("editing_idx")

    st.subheader(f"📝 식당 정보 수정: {target_name}")

    if "save_msg" in st.session_state:
        st.success(st.session_state.save_msg)
        del st.session_state.save_msg

    # 2. 폼을 사용하여 1개의 행만 편집
    with st.form("single_update_form"):
        # 선택된 5개 컬럼만 에디터에 노출
        edited_df = st.data_editor(
            editing_df,
            column_config={
                "업체명": st.column_config.Column("상호명", disabled=True),
                "신뢰도점수": st.column_config.NumberColumn("신뢰도", disabled=True, format="%.1f"),
                "대형차_접근성": st.column_config.SelectboxColumn(
                    "🚚 대형차 접근성", 
                    options=[1, 2, 3, 4, 5],
                    help="1(매우나쁨) ~ 5(매우좋음)"
                ),
                "contract_status": st.column_config.SelectboxColumn(
                    "🤝 계약 상태",
                    options=["후보 식당", "계약 성공", "계약 실패"],
                    required=True
                ),
                "remarks": st.column_config.TextColumn("📝 비고 (특이사항)")
            },
            hide_index=True, 
            width="stretch",
            key="single_editor_widget"
        )

        submit_btn = st.form_submit_button("💾 이 식당 정보 업데이트", use_container_width=True)

        if submit_btn:
            with st.spinner("저장 중..."):
                # 에디터에서 수정한 최종 값 가져오기
                final_row = edited_df.iloc[0]

                # DB 업데이트 함수 호출
                update_restaurant(
                    name=final_row["업체명"],
                    # 원본 주소는 session_state나 원본 df에서 참조 (안전을 위해 editing_idx 활용 가능)
                    address=full_df.loc[target_idx, "도로명주소"], 
                    access=None if pd.isna(final_row["대형차_접근성"]) else int(final_row["대형차_접근성"]),
                    status=final_row["contract_status"],
                    remarks=None if (pd.isna(final_row["remarks"]) or str(final_row["remarks"]).strip() == "") else final_row["remarks"]
                )

                # 2. ✅ 중요: 리런 후에도 지도가 이 식당을 보게 좌표 고정
                new_lat = full_df.loc[target_idx, "latitude"]
                new_lon = full_df.loc[target_idx, "longitude"]
                st.session_state["picked"] = (new_lat, new_lon)

                # 3. 리런 시 그리드에서 다시 선택 이벤트를 타지 않도록 방어 (선택 사항)
                st.session_state["_need_rerun"] = False 

                st.session_state.save_msg = f"✅ '{target_name}' 정보 업데이트 완료!"
                st.rerun()
