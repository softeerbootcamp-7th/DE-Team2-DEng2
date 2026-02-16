import streamlit as st
import geopandas as gpd
import pandas as pd
import pydeck as pdk
import numpy as np
from core.db import get_engine

@st.cache_data
def load_values(year_month: str):
    return pd.read_sql(
        """
        SELECT
            "SHP_CD" AS shp_cd,
            SUM(cargo_sales_count) AS value
        FROM chajoo_dist
        WHERE year_month = %s
        GROUP BY "SHP_CD"
        """,
        get_engine(),
        params=(year_month,)
    )


def render_chajoo_map(gdf, show_table=False):

    year_month = "2026-01"
    use_log = False

    df = load_values(year_month)

    merged = gdf.merge(
        df,
        left_on="SIGUNGU_CD",
        right_on="shp_cd",
        how="inner"
    )

    merged = merged[
        ["SIGUNGU_CD", "SIGUNGU_NM", "value", "geometry"]
    ]

    if merged.crs is None or merged.crs.to_epsg() != 4326:
        merged = merged.to_crs(epsg=4326)

    merged["geometry"] = merged["geometry"].simplify(
        tolerance=0.01,
        preserve_topology=True
    )

    values = merged["value"]
    if use_log:
        values = np.log1p(values)

    max_val = values.max()

    def color_scale(x):
        if max_val == 0 or pd.isna(x):
            return [180, 180, 180, 120]
        r = x / max_val
        return [
            int(255 * r),
            60,
            int(255 * (1 - r)),
            200
        ]

    merged["fill_color"] = values.apply(color_scale)

    geojson = {
        "type": "FeatureCollection",
        "features": [
            {
                "type": "Feature",
                "properties": {
                    "name": str(row["SIGUNGU_NM"]),
                    "value": int(row["value"]),
                    "fill_color": row["fill_color"],
                },
                "geometry": row["geometry"].__geo_interface__,
            }
            for _, row in merged.iterrows()
        ]
    }


    layer = pdk.Layer(
        "GeoJsonLayer",
        geojson,
        pickable=True,
        auto_highlight=True,
        filled=True,
        stroked=True,
        get_fill_color="properties.fill_color",
        get_line_color=[80, 80, 80],
        line_width_min_pixels=1,
    )

    deck = pdk.Deck(
        layers=[layer],
        initial_view_state=pdk.ViewState(
            latitude=36.5,
            longitude=127.8,
            zoom=6,
        ),
        tooltip={
            "html": "<b>{name}</b><br/>화물차주 수: {value}",
            "style": {"color": "white"}
        }
    )

    st.pydeck_chart(deck, use_container_width=True)

    if show_table:
        with st.expander("📋 데이터 테이블"):
            st.dataframe(
                merged.drop(columns=["geometry"]),
                use_container_width=True
            )
