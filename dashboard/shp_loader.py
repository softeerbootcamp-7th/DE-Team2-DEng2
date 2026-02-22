# chajoo_map.py
import streamlit as st
import geopandas as gpd
from core.settings import SHP_PATH

@st.cache_resource
def load_shp():
    return gpd.read_parquet(SHP_PATH)