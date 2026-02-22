import streamlit as st
import geopandas as gpd
import pydeck as pdk
import os
from dotenv import load_dotenv
import pandas as pd

# 내부 모듈 임포트
from core.query import load_chajoo_data, load_parking_data
from st_aggrid import AgGrid, GridOptionsBuilder, GridUpdateMode


# 1. 환경 설정 및 페이지 설정
load_dotenv()
MAPBOX_API_KEY = os.getenv("MAPBOX_API_KEY")

# ------------------------------------------------------------------------------
# 데이터 전처리 함수 (app.py에서 호출하여 각 함수에 전달)
# ------------------------------------------------------------------------------
def prepare_chajoo_data(gdf):
    df_chajoo, _, _ = load_chajoo_data()
    if df_chajoo.empty:
        return None, None

    # 병합 및 최적화
    df_chajoo["shp_cd"] = df_chajoo["shp_cd"].astype(str)
    merged = gdf.merge(
        df_chajoo.drop(columns=['sido']), 
        left_on="SIGUNGU_CD", 
        right_on="shp_cd", 
        how="inner"
    )
    merged["geometry"] = merged["geometry"].simplify(tolerance=0.02, preserve_topology=True)

    # 공영 주차장 데이터
    df_parking = load_parking_data().dropna(subset=["lat", "lon"])

    return merged, df_parking

# ------------------------------------------------------------------------------
# 1. 그리드 렌더링 함수
# ------------------------------------------------------------------------------
def render_chajoo_grid(merged_df):

    if merged_df is None or merged_df.empty:
        st.info("데이터가 없습니다.")
        return

    # 1. 데이터 가공 (AgGrid용)
    grid_df = merged_df[["sido", "SIGUNGU_NM", "score"]].copy()
    grid_df = grid_df.sort_values(by="score", ascending=False).head(10)
    # 🥇 순위 컬럼 추가 (1부터 시작)
    grid_df.insert(0, "순위", range(1, len(grid_df) + 1))

    grid_df["_idx"] = grid_df.index  # 원본 index 보관


    grid_df = grid_df.rename(columns={
        "sido": "시도",
        "SIGUNGU_NM": "시군구",
        "score": "전략적 중요도"
    })

    # 2. AgGrid 설정
    gb = GridOptionsBuilder.from_dataframe(grid_df)
    gb.configure_column("_idx", hide=True)
    gb.configure_default_column(editable=False, resizable=True)
    gb.configure_selection(selection_mode="single", use_checkbox=False)


    gb.configure_column("순위", minWidth=60, flex=1, pinned="left", cellStyle={'text-align': 'center'}, filter=False)
    gb.configure_column("시도", minWidth=80, flex=1.5, pinned="left")
    gb.configure_column("시군구", minWidth=100, flex=2, pinned="left")
    gb.configure_column(
        "전략적 중요도",
        minWidth=100,
        flex=2,
        type=["numericColumn", "numberColumnFilter"],
        valueFormatter="Math.floor(value * 100) / 100",
        filter=False
    )

    gb.configure_grid_options(domLayout="normal", rowHeight=57)

    # 🎨 글꼴 크기 대폭 확대 및 볼드체 강조
    custom_css = {
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
            "justify-content": "center",
            "align-items": "center",
            "font-size": "clamp(14px, 1.2vw, 22px) !important",
            "font-weight": "500",
            "display": "flex",
            "align-items": "center"
        },
        ".ag-row-selected": {
            "background-color": "#2c3e50 !important",
            "border": "2px solid #00d4ff !important"
        }
    }

    # 3. AgGrid 렌더링
    grid = AgGrid(
        grid_df,
        gridOptions=gb.build(),
        update_mode=GridUpdateMode.SELECTION_CHANGED,
        theme="streamlit",
        key="chajoo_grid_picker",
        custom_css=custom_css,
        height=600
    )

    # 4. 클릭 이벤트 처리 (지도 이동 로직)
    selected = grid.get("selected_rows")

    if selected is not None:
        if isinstance(selected, pd.DataFrame) and not selected.empty:
            row = selected.iloc[0]
            idx = row["_idx"]

            st.session_state["target_sigungu"] = f"{row["시도"]} {row["시군구"]}"


            # 폴리곤의 중심점(Centroid) 계산하여 좌표 추출
            target_geom = merged_df.loc[idx, "geometry"]
            centroid = target_geom.centroid
            new_view = (centroid.y, centroid.x) # (lat, lon)

            # 세션 상태 업데이트 (지도가 이 좌표를 바라보게 설정)
            if st.session_state.get("chajoo_view") != new_view:
                st.session_state["chajoo_view"] = new_view

# ------------------------------------------------------------------------------
# 2. 지도 렌더링 함수
# ------------------------------------------------------------------------------
def render_chajoo_map(merged_df, df_parking, mapbox_api_key):

    # 색상 스케일 계산 (함수 내부에서 처리)
    def get_colors(val, max_v):
        ratio = val / max_v if max_v > 0 else 0
        return [int(255 * ratio), 60, int(255 * (1 - ratio)), 160]

    max_val = merged_df["value"].max() if not merged_df.empty else 0
    merged_df["fill_color"] = merged_df["value"].apply(lambda x: get_colors(x, max_val))

    # 시군구용 공통 툴팁 컬럼 생성
    merged_df["tooltip_html"] = merged_df.apply(
        lambda r: f"<b>{r['SIGUNGU_NM']}</b>: {int(r['value'])}명", axis=1
    )


    icon_settings = {
        "url": "https://img.icons8.com/ios-filled/50/ffffff/marker.png",
        "width": 128, "height": 128, "anchorY": 128, "mask": True,
    }
    df_parking["icon_data"] = [icon_settings] * len(df_parking)
    df_parking["color"] = [[204, 255, 0]] * len(df_parking) # 연두색 강조

    # 아이콘용 툴팁 HTML 데이터 생성
    df_parking["tooltip_html"] = df_parking.apply(
        lambda r: f"""
        <div style="font-family:'Malgun Gothic',sans-serif; padding:10px;">
          <b style="font-size:15px; color:#00d4ff;">🅿️ {r.get('name','')}</b><br/>
          <small style="color:#bbb;">{r.get('address','')}</small>
        </div>
        """, axis=1
    )

    # 레이어 구성
    layers = [
        # 1. 시군구 배경 레이어
        pdk.Layer(
            "GeoJsonLayer",
            merged_df,
            pickable=True,
            filled=True,
            stroked=True,
            get_fill_color="fill_color",
            get_line_color=[255, 255, 255, 40],
            line_width_min_pixels=1,
        ),
        # 2. 공영차고지 아이콘 레이어 (수정 버전)
        pdk.Layer(
            "IconLayer",
            df_parking,
            pickable=True,
            get_position="[lon, lat]",
            get_icon="icon_data",
            get_size=4,
            size_scale=5,
            get_color="color",
        ),
    ]

    view_pos = st.session_state.get("chajoo_view", (36.3, 127.8))
    zoom_level = 10 if st.session_state.get("chajoo_view") else 6.2

    use_satellite = st.session_state.get("chajoo_map_satellite", False)
    st.pydeck_chart(pdk.Deck(
            layers=layers,
            initial_view_state=pdk.ViewState(
                latitude=view_pos[0],
                longitude=view_pos[1],
                zoom=zoom_level,
                pitch=0
            ),
            map_style = "mapbox://styles/mapbox/satellite-streets-v12" if use_satellite else "mapbox://styles/mapbox/dark-v11",
            api_keys={"mapbox": mapbox_api_key},
            tooltip={
                "html": "{tooltip_html}",  # 양쪽 레이어에 공통으로 존재하는 컬럼명
                "style": {
                    "backgroundColor": "rgba(33, 33, 33, 0.9)",
                    "color": "white",
                    "fontSize": "13px",
                    "borderRadius": "5px"
                }
            }
        ),
        height=600,
        width="stretch")