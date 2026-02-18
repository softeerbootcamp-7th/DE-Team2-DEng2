import streamlit as st
import pandas as pd
import pydeck as pdk
from core.query import load_chajoo_data, load_parking_data

# ------------------------------------------------------------------------------
# 1. 헬퍼 함수: 색상 스케일 및 데이터 가공
# ------------------------------------------------------------------------------
def get_color_scale(value, max_val):
    """수치에 따른 RGBA 색상을 반환합니다."""
    if pd.isna(value) or max_val == 0:
        return [180, 180, 180, 120]

    # 값에 비례하여 Red 농도 조절 (R: 수치 비례, G: 고정, B: 역비례)
    ratio = value / max_val
    return [int(255 * ratio), 60, int(255 * (1 - ratio)), 160]

# ------------------------------------------------------------------------------
# 2. 메인 렌더링 함수
# ------------------------------------------------------------------------------
def render_chajoo_map(gdf, mapbox_api_key=None):
    """차주 분포 히트맵과 공영차고지 아이콘 레이어를 렌더링합니다."""

    # [데이터 로드]
    df_chajoo, _, _ = load_chajoo_data()
    df_parking = load_parking_data().dropna(subset=["lat", "lon"])

    if df_chajoo.empty:
        st.warning("표시할 차주 데이터가 없습니다.")
        return

    # --- [Step 1: GeoData 결합 및 최적화] ---
    merged = gdf.merge(df_chajoo, left_on="SIGUNGU_CD", right_on="shp_cd", how="inner")

    # CRS 및 기하학 단순화 (성능 최적화)
    if merged.crs is None or merged.crs.to_epsg() != 4326:
        merged = merged.to_crs(epsg=4326)
    merged["geometry"] = merged["geometry"]

    # 색상 적용
    max_val = merged["value"].max()
    merged["fill_color"] = merged["value"].apply(lambda x: get_color_scale(x, max_val))

    # GeoJson용 툴팁 HTML 데이터 생성
    merged["tooltip_html"] = merged.apply(
        lambda r: f"""
        <div style="font-family:'Malgun Gothic',sans-serif; padding:10px;">
          <b style="font-size:15px;">📍 {r.get('sido', '')} {r.get('SIGUNGU_NM','')}</b><br/>
          <b>화물차주 수:</b>
          <span style="color:#ffcc00;">{int(r['value']) if pd.notna(r['value']) else 0}명</span>
        </div>
        """, axis=1
    )

    # --- [Step 2: 주차장 아이콘 설정] ---
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

    # --- [Step 3: 레이어 구성] ---
    layers = [
        # 시군구별 차주 분포 레이어 (Heatmap 스타일)
        pdk.Layer(
            "GeoJsonLayer",
            merged,
            pickable=True,
            filled=True,
            stroked=True,
            get_fill_color="fill_color",
            get_line_color=[255, 255, 255, 40],
            line_width_min_pixels=1,
        ),
        # 공영차고지 아이콘 레이어
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

    # --- [Step 4: 지도 출력] ---
    st.pydeck_chart(pdk.Deck(
        layers=layers,
        initial_view_state=pdk.ViewState(
            latitude=36.3,
            longitude=127.8,
            zoom=6,
            pitch=0
        ),
        map_style="mapbox://styles/mapbox/dark-v11",
        api_keys={"mapbox": mapbox_api_key},
        tooltip={
            "html": "{tooltip_html}",
            "style": {
                "backgroundColor": "rgba(33, 33, 33, 0.95)",
                "color": "white",
                "border": "1px solid #00d4ff",
                "borderRadius": "8px",
            },
        }
    ), width='stretch')

    with st.expander("🗺️ 전국 수요 및 인프라 분석 가이드", expanded=True):
        legend_infrastructure = """
        <div style="background-color: #1e1e1e; padding: 20px 20px 48px 20px;; border-radius: 10px; border: 1px solid #444; color: white; font-family: sans-serif;">
            <table style="width: 100%; border-collapse: collapse;">
                <tr>
                    <td style="vertical-align: top; width: 50%; padding-right: 15px;">
                        <h5 margin: 0 0 10px 0;">📈 수요: 차주 밀집도</h5>
                        <div style="margin-bottom: 6px; font-size: 14px; display: flex; align-items: center;">
                            <span style="display:inline-block; width:12px; height:12px; background:#ff3c00; border-radius:2px; margin-right:8px;"></span>매우 높음
                        </div>
                        <div style="margin-bottom: 6px; font-size: 14px; display: flex; align-items: center;">
                            <span style="display:inline-block; width:12px; height:12px; background:#b43cff; border-radius:2px; margin-right:8px;"></span>중간 분포
                        </div>
                        <div style="margin-bottom: 6px; font-size: 14px; display: flex; align-items: center;">
                            <span style="display:inline-block; width:12px; height:12px; background:#b4b4b4; border-radius:2px; margin-right:8px;"></span>거주자 적음
                        </div>
                    </td>
                    <td style="vertical-align: top; width: 50%; border-left: 1px solid #333; padding-left: 20px;">
                        <h5 margin: 0 0 10px 0;">🅿️ 공급: 기존 인프라</h5>
                        <div style="margin-bottom: 12px; display: flex; align-items: center;">
                            <span style="display:inline-block; width:12px; height:12px; background:rgb(204, 255, 0); border-radius:50%; margin-right:10px;"></span>
                            <b font-size: 14px;">트럭헬퍼 주차장</b>
                        </div>
                        <div style="font-size: 12px; color: #ccc; line-height: 1.5;">
                            붉은 수요 구역에 연두색 점이 없는 곳이<br>
                            <span style="color:#ff6666;"><b>서비스 확장 우선순위</b></span> 지역입니다.
                        </div>
                    </td>
                </tr>
            </table>
        </div>
        """
        st.markdown(legend_infrastructure, unsafe_allow_html=True)
        st.write("")
