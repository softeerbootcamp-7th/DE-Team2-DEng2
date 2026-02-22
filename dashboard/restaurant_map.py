import pandas as pd
import pydeck as pdk
import streamlit as st
from st_aggrid import AgGrid, JsCode, GridOptionsBuilder, GridUpdateMode
from core.query import update_restaurant, load_contract_restaurants


def score_to_grade(x):
    if pd.isna(x):
        return None
    if x >= 0.8:
        return "A"
    elif x >= 0.6:
        return "B"
    elif x >= 0.4:
        return "C"
    elif x >= 0.2:
        return "D"
    else:
        return "E"


def parking_to_grade(x):
    if pd.isna(x):
        return None
    mapping = {
        5: "A",
        4: "B",
        3: "C",
        2: "D",
        1: "E",
    }
    return mapping.get(int(x), None)

def render_restaurant_grid(display_df):


    GRID_COLUMNS = [
        "업체명",
        "총점",
        "수익성",
        "영업_적합도",
        "주차_적합도",
        "contract_status",
        "remarks",
    ]

    grid_df = display_df[GRID_COLUMNS].copy()
    grid_df["_idx"] = display_df.index  # 원본 index 보관
    grid_df.insert(0, "순위", range(1, len(grid_df) + 1))
    grid_df["총점"] = grid_df["총점"].round().astype("Int64")

    grid_df["영업_적합도"] = grid_df["영업_적합도"].apply(score_to_grade)
    grid_df["수익성"] = grid_df["수익성"].apply(score_to_grade)
    grid_df["주차_적합도"] = grid_df["주차_적합도"].apply(parking_to_grade)

    grid_df = grid_df.rename(columns={
        "영업_적합도": "영업 적합도",
        "주차_적합도": "주차 적합도",
        "contract_status": "진행 상태",
        "remarks": "비고",
    })

    gb = GridOptionsBuilder.from_dataframe(grid_df)

    gb.configure_column("_idx", hide=True)  # 👈 화면에 안 보이게

    gb.configure_default_column(editable=False, resizable=True)
    gb.configure_selection(selection_mode="single", use_checkbox=False)

    gb.configure_column("순위", pinned="left", minWidth=60, flex=1, filter=False)
    gb.configure_column("업체명", pinned="left", minWidth=120, flex=3, tooltipField="업체명")
    gb.configure_column("총점", pinned="left", minWidth=80, flex=1.5, filter=False)

    gb.configure_column("수익성", pinned="left", minWidth=80, flex=1.5)
    gb.configure_column("영업 적합도", pinned="left", minWidth=80, flex=1.5)
    gb.configure_column("주차 적합도", pinned="left", minWidth=80, flex=1.5)

    gb.configure_column(
        "진행 상태",
        pinned="left",
        minWidth=90,
        flex=1.5,
        filter="agSetColumnFilter",
        cellStyle=JsCode("""
            function(params) {

                const styles = {
                    "후보": {
                        bg: "#2a2a2a",
                        color: "#cfcfcf"
                    },
                    "접촉": {
                        bg: "#1f2a38",
                        color: "#90caf9"
                    },
                    "관심": {
                        bg: "#332b1a",
                        color: "#ffd54f"
                    },
                    "협의": {
                        bg: "#3a2a1a",
                        color: "#ffb74d"
                    },
                    "성공": {
                        bg: "#1f3326",
                        color: "#81c784"
                    },
                    "실패": {
                        bg: "#3a1f1f",
                        color: "#ef9a9a"
                    }
                };

                const s = styles[params.value];
                if (!s) return {};

                return {
                    display: "inline-flex",
                    alignItems: "center",
                    justifyContent: "center",
                    padding: "2px 2px",
                    borderRadius: "999px",
                    backgroundColor: s.bg,
                    color: s.color,
                    fontWeight: "600",
                    fontSize: "12px",
                    lineHeight: "1",
                    boxShadow: "inset 0 0 0 1px rgba(255,255,255,0.04)"
                };
            }
        """)
    )

    gb.configure_column("비고", wrapText=False, autoHeight=False, tooltipField="비고")

    gb.configure_grid_options(
        domLayout="normal",
        rowHeight=57,
        enableBrowserTooltips=True,
    )

    custom_css = {
        ".ag-root-wrapper": {
            "font-size": "clamp(14px, 1.1vw, 18px)",
        },
        ".ag-header-cell": {
            "display": "flex",
            "justify-content": "center",
            "align-items": "center",
            "text-align": "center",
        },
        ".ag-header-cell-label": {
            "font-size": "clamp(14px, 1.1vw, 20px) !important",
            "font-weight": "800 !important",
            "justify-content": "center",
            "width": "100%",
            "text-align": "center",
        },
        ".ag-cell": {
            "display": "flex",
            "justify-content": "center",
            "align-items": "center",
            "text-align": "center",
            "font-size": "clamp(16px, 1.4vw, 24px) !important",
            "font-weight": "500",
            "overflow": "hidden",
            "text-overflow": "ellipsis",
            "white-space": "nowrap",
        },
        ".ag-row": {
            "height": "48px !important",
        },
    }

    # 그리드 렌더링
    grid = AgGrid(
        grid_df,
        gridOptions=gb.build(),
        update_mode=GridUpdateMode.SELECTION_CHANGED,
        key="restaurant_picker", # 키 고정 필수!
        theme="streamlit",
        height=502,
        custom_css=custom_css,
        allow_unsafe_jscode=True
    )

    selected = grid.get("selected_rows")

    # 선택된 행이 있을 때만 세션 업데이트
    if isinstance(selected, pd.DataFrame) and not selected.empty:
        row = selected.iloc[0]
        # AgGrid의 인덱스 대신 '상호명'으로 원본 데이터 재조회 (더 안전함)
        target_name = row["업체명"]
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
        df["access_display"] = df["주차_적합도"].apply(lambda x: "미입력" if pd.isna(x) else f"{int(x)}/5")
        df["remarks_display"] = df["remarks"].fillna("미입력")

        color_map = {
            "후보": [180, 180, 180, 200],        # 회색
            "접촉": [33, 150, 243, 200],        # 파랑
            "관심": [255, 193, 7, 220],         # 노랑
            "협의": [255, 140, 0, 220],         # 주황
            "성공": [76, 175, 80, 230],         # 초록
            "실패": [244, 67, 54, 230],         # 빨강
        }

        df["color"] = df["contract_status"].apply(lambda x: color_map.get(x, color_map["후보"]))

        ICON_URL = "https://img.icons8.com/ios-filled/50/ffffff/marker.png"
        df["icon_data"] = [{"url": ICON_URL, "width": 128, "height": 128, "anchorY": 128, "mask": True} for _ in range(len(df))]

        # 툴팁 생성
        df["tooltip_text"] = df.apply(lambda x: f"""
            <div style="font-family: 'Malgun Gothic', sans-serif; width: 220px; line-height: 1.6;">
                <b style="font-size:16px; color:#00d4ff;">🏠 {x['업체명']}</b><br/>
                <small style="color:#bbb;">{x['도로명주소']}</small>
                <hr style="margin:8px 0; border-color:#555;">
                <div style="font-size:13px;">
                    <b>🅿️ 유휴부지 면적:</b> {int(x['유휴부지_면적']):,}㎡<br/>
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


    # 세션에서 데이터 가져오기 (원본 변형 방지를 위해 복사)
    editing_df = st.session_state["editing_data"].copy()

    # 이미 등급 문자열로 변환된 경우(rerun) 중복 변환 방지
    if editing_df["수익성"].dtype != object:
        editing_df["수익성"] = editing_df["수익성"].apply(score_to_grade)
    if editing_df["영업_적합도"].dtype != object:
        editing_df["영업_적합도"] = editing_df["영업_적합도"].apply(score_to_grade)
    if editing_df["주차_적합도"].dtype != object:
        editing_df["주차_적합도"] = editing_df["주차_적합도"].apply(parking_to_grade)
    editing_df["총점"] = editing_df["총점"].round().astype("Int64")

    target_name = editing_df["업체명"].iloc[0]
    target_idx = st.session_state.get("editing_idx")

    st.subheader(f"📝 식당 정보 수정: {target_name}")

    if "save_msg" in st.session_state:
        st.success(st.session_state.save_msg)
        del st.session_state.save_msg

    grade_to_score = {"A": 5, "B": 4, "C": 3, "D": 2, "E": 1}

    # 2. 폼을 사용하여 1개의 행만 편집
    with st.form("single_update_form"):

        # 선택된 5개 컬럼만 에디터에 노출
        edited_df = st.data_editor(
            editing_df,
            column_config={
                "업체명": st.column_config.Column("업체명", disabled=True),
                "수익성": st.column_config.Column("수익성", disabled=True),
                "영업_적합도": st.column_config.Column("영업 적합도", disabled=True),
                "주차_적합도": st.column_config.SelectboxColumn(
                    "주차 적합도",
                    options=["A", "B", "C", "D", "E"],
                    help="A(매우좋음) ~ E(매우나쁨)"
                ),
                "contract_status": st.column_config.SelectboxColumn(
                    "진행 상태",
                    options=["후보", "접촉", "관심", "협의", "성공", "실패"],
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

                # 주차 등급을 다시 숫자로 변환
                selected_grade = final_row["주차_적합도"]
                numeric_parking = grade_to_score.get(selected_grade, None)

                # DB 업데이트 함수 호출
                update_restaurant(
                    name=final_row["업체명"],
                    # 원본 주소는 session_state나 원본 df에서 참조 (안전을 위해 editing_idx 활용 가능)
                    address=full_df.loc[target_idx, "도로명주소"],
                    access=numeric_parking,
                    status=final_row["contract_status"],
                    remarks=None if (pd.isna(final_row["remarks"]) or str(final_row["remarks"]).strip() == "") else final_row["remarks"]
                )

                # 2. ✅ 중요: 리런 후에도 지도가 이 식당을 보게 좌표 고정
                new_lat = full_df.loc[target_idx, "latitude"]
                new_lon = full_df.loc[target_idx, "longitude"]
                st.session_state["picked"] = (new_lat, new_lon)

                st.session_state.save_msg = f"✅ '{target_name}' 정보 업데이트 완료!"
                st.rerun()

def render_contract_management(target_sigungu):
    """계약 진행 중인 식당들을 리스트 형태로 보고 바로 수정하는 에디터"""

    st.markdown("### 🤝 계약 및 진행 현황 관리")
    st.caption("후보군을 제외한 '접촉, 관심, 협의, 성공, 실패' 상태의 식당들입니다.")

    # 1. 데이터 로드
    contract_df = load_contract_restaurants(target_sigungu)

    if contract_df.empty:
        st.info("해당 지역에 진행 중인 계약 건이 없습니다.")
        return

    # 표시용 데이터 정리 (원본 백업용 인덱스 유지)
    display_df = contract_df.copy()

    # 등급 변환 (수정 시 편의를 위해)
    display_df["주차_적합도"] = display_df["주차_적합도"].apply(parking_to_grade)
    
    # 2. 데이터 에디터 배치
    # 사용자가 수정한 내용은 'edited_df'에 담깁니다.
    edited_df = st.data_editor(
        display_df,
        column_config={
            "업체명": st.column_config.Column("업체명", width=100, disabled=True),
            "도로명주소": st.column_config.Column("주소", width=200, disabled=True),
            "총점": st.column_config.NumberColumn("점수", format="%d", width=5, disabled=True),
            "주차_적합도": st.column_config.SelectboxColumn(
                "주차 적합도",
                options=["A", "B", "C", "D", "E"],
                width=5,
                required=True
            ),
            "contract_status": st.column_config.SelectboxColumn(
                "진행 상태",
                options=["후보", "접촉", "관심", "협의", "성공", "실패"],
                width=5,
                required=True
            ),
            "remarks": st.column_config.TextColumn("비고 (특이사항)", width="large")
        },
        column_order=["업체명", "도로명주소", "contract_status", "총점", "주차_적합도", "remarks"],
        hide_index=True,
        width="stretch",
        key="contract_batch_editor"
    )

    # 3. 저장 버튼 로직
    col1, col2 = st.columns([8, 2])
    with col2:
        save_btn = st.button("💾 변경사항 일괄 저장", use_container_width=True, type="primary")

    if save_btn:
        grade_to_score = {"A": 5, "B": 4, "C": 3, "D": 2, "E": 1}
        updated_count = 0
        
        with st.spinner("DB 반영 중..."):
            # 기존 데이터(display_df)와 수정된 데이터(edited_df)를 비교하여 변경된 것만 처리
            # (또는 간단하게 전체 루프를 돌며 업데이트)
            for idx in range(len(edited_df)):
                row = edited_df.iloc[idx]
                orig_row = display_df.iloc[idx]
                
                # 실제로 값이 변경되었는지 체크 (성능 최적화)
                if (row["contract_status"] != orig_row["contract_status"] or 
                    row["주차_적합도"] != orig_row["주차_적합도"] or 
                    row["remarks"] != orig_row["remarks"]):
                    
                    update_restaurant(
                        name=row["업체명"],
                        address=row["도로명주소"],
                        access=grade_to_score.get(row["주차_적합도"], 3),
                        status=row["contract_status"],
                        remarks=None if (pd.isna(row["remarks"]) or str(row["remarks"]).strip() == "") else row["remarks"]
                    )
                    updated_count += 1
            
            if updated_count > 0:
                st.success(f"{updated_count}건의 정보가 업데이트되었습니다!")
                st.rerun()
            else:
                st.warning("변경사항이 없습니다.")