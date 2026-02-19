import pandas as pd
import geopandas as gpd
import pydeck as pdk
import streamlit as st
from core.query import load_restaurants, update_restaurant_status, load_zscore_hotspots, save_report

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

    # --- [거점 데이터 필터링 로드] ---
    hotspot_df = load_zscore_hotspots(selected_shp_cd)

    if hotspot_df.empty:
        # 거점이 없을 경우 빈 레이어를 위해 빈 DF 유지 혹은 안내
        pass

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


    # 거점 데이터프레임에 전용 툴팁 생성
    hotspot_df["tooltip_text"] = hotspot_df.apply(lambda x: f"""
        <div style="font-family: sans-serif; border-left: 4px solid #e74c3c; padding-left: 8px;">
            <b style="font-size:15px; color:#e74c3c;">🔥 운송업 핵심 거점</b><br/>
            <hr style="margin:5px 0; border-color:#555;">
            <b>👥 종사자수:</b> {x['value']:.1f}명<br/>
        </div>
    """, axis=1)
    # 1. 거점 레이어 (Z-Score 기반 Scatterplot)
    # 식당 아이콘보다 아래에 깔리도록 먼저 선언
    hotspot_layer = pdk.Layer(
        "ScatterplotLayer",
        hotspot_df,
        id="hotspot-layer",  # 툴팁 식별을 위한 ID
        get_position='[lon, lat]',
        # 색상: Z-Score가 높을수록 더 진한 빨간색으로 표현 (선택 사항)
        get_fill_color='[231, 76, 60, 200]', 
        # 반경 설정: 
        # 1. get_radius: 실제 지도상의 미터(m) 단위 반경
        # 2. radius_min_pixels: 줌을 아웃해도 최소한 이 픽셀 크기보다는 작아지지 않음 (가장 중요!)
        get_radius='z_score * 100',  # Z-score에 비례해서 실제 크기 결정
        radius_min_pixels=8,         # 아무리 멀리서 봐도 최소 8픽셀 크기 유지
        radius_max_pixels=50,        # 너무 커지는 것 방지
        pickable=True,
        stroked=True,                # 테두리 추가
        line_width_min_pixels=1,
        get_line_color=[255, 255, 255], # 흰색 테두리로 가독성 확보
    )


    # 2. 식당 레이어 (IconLayer)

    # 3. 구역 경계 레이어
    target_gdf = gdf_boundary[gdf_boundary["SIGUNGU_CD"] == selected_shp_cd].copy()
    boundary_layer = pdk.Layer(
        "GeoJsonLayer", target_gdf, stroked=True, filled=True,
        get_fill_color=[0, 212, 255, 30], get_line_color=[255, 255, 255, 200],
        line_width_min_pixels=2,
    )

    # 레이어 순서: 경계 -> 거점 -> 식당
    layers = [boundary_layer, hotspot_layer]

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
                    <a href="https://your-report-form.com?name={x['업체명']}" target="_blank" 
                        style="color: #ff4b4b; text-decoration: none; font-weight: bold;">
                        🚩 신고 페이지로 이동
                    </a>
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


    # --- [지도 뷰 설정: SHP 경계 중심점 기준] ---
    if not target_gdf.empty:
        projected_gdf = target_gdf.to_crs(epsg=5179)
        centroids = projected_gdf.geometry.centroid.to_crs(epsg=4326)

        avg_lat = centroids.y.mean()
        avg_lon = centroids.x.mean()
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
        tooltip={
            "html": "{tooltip_text}",
            "style": {
                "backgroundColor": "rgba(33, 33, 33, 0.95)",
                "color": "white",
                "borderRadius": "5px"
            }
        },
    ))

    with st.expander("📍 식당 및 거점 분석 가이드", expanded=True):
        legend_restaurants = """
        <div style="background-color: #1e1e1e; padding: 20px 20px 20px 20px; border-radius: 10px; border: 1px solid #444; color: white; font-family: sans-serif;">
            <table style="width: 100%; border-collapse: collapse;">
                <tr>
                    <td style="vertical-align: top; width: 50%; padding-right: 15px;">
                        <h5 margin: 0 0 10px 0;">🤝 식당 계약 상태</h5>
                        <div style="margin-bottom: 6px; font-size: 14px; display: flex; align-items: center;">
                            <span style="display:inline-block; width:12px; height:12px; background:rgb(255, 215, 0); border-radius:50%; margin-right:8px;"></span>후보 식당
                        </div>
                        <div style="margin-bottom: 6px; font-size: 14px; display: flex; align-items: center;">
                            <span style="display:inline-block; width:12px; height:12px; background:rgb(76, 175, 80); border-radius:50%; margin-right:8px;"></span>계약 성공
                        </div>
                        <div style="margin-bottom: 6px; font-size: 14px; display: flex; align-items: center;">
                            <span style="display:inline-block; width:12px; height:12px; background:rgb(244, 67, 54); border-radius:50%; margin-right:8px;"></span>계약 실패
                        </div>
                        <div style="margin-bottom: 6px; font-size: 14px; display: flex; align-items: center;">
                            <span style="display:inline-block; width:12px; height:12px; background:rgb(72, 141, 247); border-radius:50%; margin-right:8px;"></span>미입력
                        </div>
                    </td>
                    <td style="vertical-align: top; width: 50%; border-left: 1px solid #333; padding-left: 20px;">
                        <h5 margin: 0 0 10px 0;">🔥 분석 핵심</h5>
                        <div style="margin-bottom: 12px; display: flex; align-items: center;">
                            <span style="display:inline-block; width:16px; height:16px; border: 2px solid #e74c3c; background:rgba(231, 76, 60, 0.4); border-radius:50%; margin-right:10px;"></span>
                            <b font-size: 14px;">운송업 핵심 거점</b>
                        </div>
                        <div style="font-size: 12px; color: #ccc; line-height: 1.5;">
                            붉은 원은 운송업 종사자 밀집 구역입니다.<br>
                            <span style="color:#00d4ff;"><b>거점 내 후보 식당</b></span>을 최우선으로 검토하세요.
                        </div>
                    </td>
                </tr>
            </table>
        </div>
        """
        st.markdown(
            legend_restaurants,
            unsafe_allow_html=True)
        st.write("")
    return df

# ------------------------------------------------------------------------------
# 4. 데이터 에디터 섹션
# ------------------------------------------------------------------------------
def render_restaurant_editor(filtered_df):
    """하단 데이터 수정 에디터 및 신고 기능을 제공합니다."""

    if "save_msg" in st.session_state:
        st.success(st.session_state.save_msg)
        del st.session_state.save_msg

    if filtered_df is None or filtered_df.empty:
        st.info("조건에 맞는 식당이 없습니다. 필터를 통해 식당을 검색해주세요.")
        return

    # --- [중요] 신고 체크박스 컬럼 임시 추가 ---
    # 기본값은 False로 설정합니다.
    display_df = filtered_df.copy()
    display_df["신고"] = False 

    with st.form("batch_update_form"):
        edited_df = st.data_editor(
            display_df,
            # 컬럼 순서에 "신고" 추가
            column_order=("업체명", "도로명주소", "유휴부지면적", "신뢰도점수", "large_vehicle_access", "contract_status", "remarks", "신고"),
            column_config={
                "업체명": st.column_config.Column("상호명", disabled=True),
                "도로명주소": st.column_config.Column("주소", disabled=True),
                "유휴부지면적": st.column_config.Column("주차장 면적", disabled=True),
                "신뢰도점수": st.column_config.Column("신뢰도", disabled=True),
                "large_vehicle_access": st.column_config.SelectboxColumn("🚚 접근성", options=[1, 2, 3, 4, 5]),
                "contract_status": st.column_config.SelectboxColumn("🤝 상태", options=["미입력", "후보 식당", "계약 성공", "계약 실패"]),
                "remarks": st.column_config.TextColumn("📝 비고"),
                "신고": st.column_config.CheckboxColumn("🚩 신고", default=False)
            },
            hide_index=True, width="stretch", key="editor_inside_form"
        )

        submit_btn = st.form_submit_button("💾 변경사항 반영 및 신고 접수", use_container_width=True)

        if submit_btn:
            editor_state = st.session_state.editor_inside_form
            edited_rows = editor_state.get("edited_rows", {})

            if not edited_rows:
                st.warning("변경사항이 없습니다.")
            else:
                with st.spinner("데이터 처리 중..."):
                    report_count = 0
                    update_count = 0

                    for row_idx, changes in edited_rows.items():
                        target_row = display_df.iloc[int(row_idx)]

                        # 1. 신고 처리 (체크박스가 True로 변한 경우)
                        if changes.get("신고") is True:
                            save_report(target_row["업체명"], target_row["도로명주소"])
                            report_count += 1

                        # 2. 기존 정보 업데이트 (상태나 비고가 수정된 경우)
                        # 신고만 한 경우에도 기본 업데이트 로직이 돌아가도록 구성
                        update_restaurant_status(
                            name=target_row["업체명"],
                            address=target_row["도로명주소"],
                            access=changes.get("large_vehicle_access", target_row.get("large_vehicle_access")),
                            status=changes.get("contract_status", target_row["contract_status"]),
                            remarks=changes.get("remarks", target_row.get("remarks", ""))
                        )
                        update_count += 1

                msg = f"✅ {update_count}건 업데이트 완료"
                if report_count > 0:
                    msg += f" (🚩 {report_count}건 신고 접수)"

                st.session_state.save_msg = msg
                st.rerun()