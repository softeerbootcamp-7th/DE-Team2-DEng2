import os
import sys
from pathlib import Path
from urllib.parse import quote_plus

import pandas as pd
import streamlit as st
from sqlalchemy.orm import Session


PROJECT_ROOT = Path(__file__).resolve().parents[1]
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))

from api.db.session import create_engine_for_mode
from api.queries.silver_stage_1_queries import (
    get_dashboard_points,
    get_latest_snapshot,
    get_region_overview,
    get_sigungu_overview,
    search_stores,
)


def _default_local_host() -> str:
    host = os.getenv("LOCAL_PG_HOST", "localhost")
    # host 머신에서 실행할 때 docker service name이 아닌 localhost 사용
    return "localhost" if host == "postgres-local" else host


def _build_local_db_url(host: str, port: str, db: str, user: str, password: str) -> str:
    return (
        f"postgresql+psycopg2://{quote_plus(user)}:{quote_plus(password)}"
        f"@{host}:{port}/{db}"
    )


def _build_rds_db_url(
    host: str,
    port: str,
    db: str,
    user: str,
    password: str,
    sslmode: str,
) -> str:
    return (
        f"postgresql+psycopg2://{quote_plus(user)}:{quote_plus(password)}"
        f"@{host}:{port}/{db}?sslmode={sslmode}"
    )


@st.cache_resource(show_spinner=False)
def _get_engine(mode: str, db_url: str, rds_sslmode: str):
    return create_engine_for_mode(mode=mode, db_url=db_url, rds_sslmode=rds_sslmode)


def _to_df(rows: list[dict]) -> pd.DataFrame:
    if not rows:
        return pd.DataFrame()
    return pd.DataFrame(rows)


def main() -> None:
    st.set_page_config(page_title="Silver Stage 1 Dashboard", layout="wide")
    st.title("Silver Stage 1 Dashboard")
    st.caption("local postgres 또는 RDS postgres에서 데이터를 조회합니다.")

    st.sidebar.header("DB Settings")
    db_mode = st.sidebar.selectbox("DB Mode", ["local", "aws"], index=0)
    custom_db_url = st.sidebar.text_input(
        "Custom DB URL (optional)",
        value="",
        help="입력하면 아래 host/user 설정 대신 이 URL을 사용합니다.",
    ).strip()
    rds_sslmode = st.sidebar.selectbox("RDS sslmode", ["require", "prefer", "disable"], index=0)

    if db_mode == "local":
        local_host = st.sidebar.text_input("Host", value=_default_local_host())
        local_port = st.sidebar.text_input("Port", value=os.getenv("LOCAL_PG_HOST_PORT", "5432"))
        local_db = st.sidebar.text_input("Database", value=os.getenv("LOCAL_PG_DB", "deng2"))
        local_user = st.sidebar.text_input("User", value=os.getenv("LOCAL_PG_USER", "deng2"))
        local_password = st.sidebar.text_input(
            "Password",
            value=os.getenv("LOCAL_PG_PASSWORD", "deng2"),
            type="password",
        )
        db_url = custom_db_url or _build_local_db_url(
            host=local_host,
            port=local_port,
            db=local_db,
            user=local_user,
            password=local_password,
        )
    else:
        rds_host = st.sidebar.text_input("RDS Host", value=os.getenv("RDS_HOST", ""))
        rds_port = st.sidebar.text_input("RDS Port", value=os.getenv("RDS_PORT", "5432"))
        rds_db = st.sidebar.text_input("RDS Database", value=os.getenv("RDS_DB", ""))
        rds_user = st.sidebar.text_input("RDS User", value=os.getenv("RDS_USER", ""))
        rds_password = st.sidebar.text_input(
            "RDS Password",
            value=os.getenv("RDS_PASSWORD", ""),
            type="password",
        )
        db_url = custom_db_url or _build_rds_db_url(
            host=rds_host,
            port=rds_port,
            db=rds_db,
            user=rds_user,
            password=rds_password,
            sslmode=rds_sslmode,
        )

    if not db_url:
        st.warning("DB URL 또는 연결 정보를 입력하세요.")
        return

    try:
        engine = _get_engine(mode=db_mode, db_url=db_url, rds_sslmode=rds_sslmode)
        with Session(engine) as session:
            latest = get_latest_snapshot(session)
            if not latest:
                st.info("조회 가능한 스냅샷 데이터가 없습니다.")
                return

            source_year = latest["source_year"]
            source_month = latest["source_month"]
            st.subheader(f"Latest Snapshot: {source_year}-{source_month:02d}")

            region_rows = get_region_overview(
                session,
                source_year=source_year,
                source_month=source_month,
                limit=300,
            )
            region_df = _to_df(region_rows)
            if region_df.empty:
                st.info("해당 스냅샷에 지역 데이터가 없습니다.")
                return

            selected_region = st.selectbox("Region", options=region_df["region"].tolist())
            selected_region_row = region_df[region_df["region"] == selected_region].iloc[0]

            col1, col2, col3 = st.columns(3)
            col1.metric("Rows", int(selected_region_row["row_count"]))
            col2.metric("Lots", int(selected_region_row["lot_count"]))
            col3.metric("Store Rows", int(selected_region_row["store_row_count"]))

            st.markdown("### Region Overview")
            st.dataframe(region_df, use_container_width=True, hide_index=True)

            sigungu_rows = get_sigungu_overview(
                session,
                region=selected_region,
                source_year=source_year,
                source_month=source_month,
                limit=300,
            )
            sigungu_df = _to_df(sigungu_rows)
            st.markdown(f"### Sigungu Overview ({selected_region})")
            st.dataframe(sigungu_df, use_container_width=True, hide_index=True)

            st.markdown("### Store Search")
            keyword = st.text_input("Keyword", value="치킨")
            if keyword:
                store_rows = search_stores(
                    session,
                    keyword=keyword,
                    region=selected_region,
                    source_year=source_year,
                    source_month=source_month,
                    limit=200,
                )
                store_df = _to_df(store_rows)
                st.dataframe(store_df, use_container_width=True, hide_index=True)

            st.markdown("### Map")
            map_limit = st.slider("Map row limit", min_value=100, max_value=5000, step=100, value=1000)
            map_rows = get_dashboard_points(
                session,
                region=selected_region,
                source_year=source_year,
                source_month=source_month,
                limit=map_limit,
            )
            map_df = _to_df(map_rows)
            if map_df.empty:
                st.info("지도에 표시할 좌표 데이터가 없습니다.")
            else:
                st.map(map_df.rename(columns={"latitude": "lat", "longitude": "lon"})[["lat", "lon"]])
                with st.expander("Map Source Data"):
                    st.dataframe(map_df, use_container_width=True, hide_index=True)

    except Exception as exc:
        st.error(f"DB 조회 중 오류가 발생했습니다: {exc}")


if __name__ == "__main__":
    main()
