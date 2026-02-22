"""init schema

Revision ID: a79030e302fb
Revises:
Create Date: 2026-02-22 21:43:40.628058

"""
from typing import Sequence, Union

from alembic import op
import sqlalchemy as sa


# revision identifiers, used by Alembic.
revision: str = 'a79030e302fb'
down_revision: Union[str, Sequence[str], None] = None
branch_labels: Union[str, Sequence[str], None] = None
depends_on: Union[str, Sequence[str], None] = None


def upgrade() -> None:
    """schema.sql 기반 초기 스키마 생성."""

    # -- chajoo_dist --
    op.execute("""
        CREATE TABLE public.chajoo_dist (
            id integer NOT NULL,
            sido text NOT NULL,
            sigungu text NOT NULL,
            "SHP_CD" text,
            cargo_count bigint,
            "전략적_중요도" double precision,
            year text NOT NULL,
            month text NOT NULL
        )
    """)
    op.execute("""
        CREATE SEQUENCE public.chajoo_dist_id_seq
            AS integer START WITH 1 INCREMENT BY 1
            NO MINVALUE NO MAXVALUE CACHE 1
    """)
    op.execute("""
        ALTER SEQUENCE public.chajoo_dist_id_seq
            OWNED BY public.chajoo_dist.id
    """)
    op.execute("""
        ALTER TABLE ONLY public.chajoo_dist
            ALTER COLUMN id SET DEFAULT nextval('public.chajoo_dist_id_seq'::regclass)
    """)
    op.execute("""
        ALTER TABLE ONLY public.chajoo_dist
            ADD CONSTRAINT chajoo_dist_pkey PRIMARY KEY (id)
    """)

    # -- restaurant --
    op.execute("""
        CREATE TABLE public.restaurant (
            sigungu text,
            "총점" double precision,
            "영업_적합도" double precision,
            "수익성" double precision,
            "업체명" text NOT NULL,
            "도로명주소" text NOT NULL,
            "유휴부지_면적" double precision,
            longitude double precision,
            latitude double precision,
            year integer NOT NULL,
            month integer NOT NULL,
            week integer NOT NULL,
            region text,
            "주차_적합도" integer,
            contract_status text DEFAULT '후보'::text NOT NULL,
            remarks text
        )
    """)
    op.execute("""
        ALTER TABLE ONLY public.restaurant
            ADD CONSTRAINT pk_restaurant
            PRIMARY KEY ("업체명", "도로명주소", year, month, week)
    """)

    # -- truckhelper_parking_area --
    op.execute("""
        CREATE TABLE public.truckhelper_parking_area (
            "공영차고지명" text NOT NULL,
            "주소" text NOT NULL,
            lat double precision,
            lon double precision
        )
    """)
    op.execute("""
        ALTER TABLE ONLY public.truckhelper_parking_area
            ADD CONSTRAINT truckhelper_parking_area_pkey
            PRIMARY KEY ("공영차고지명", "주소")
    """)

    # -- user_view_history --
    op.execute("""
        CREATE TABLE public.user_view_history (
            id integer DEFAULT 1 NOT NULL,
            sigungu text NOT NULL,
            updated_at timestamp without time zone DEFAULT now() NOT NULL,
            CONSTRAINT check_single_row CHECK ((id = 1))
        )
    """)
    op.execute("""
        ALTER TABLE ONLY public.user_view_history
            ADD CONSTRAINT user_view_history_pkey PRIMARY KEY (id)
    """)


def downgrade() -> None:
    """테이블 삭제 (역순)."""
    op.execute("DROP TABLE IF EXISTS public.user_view_history")
    op.execute("DROP TABLE IF EXISTS public.truckhelper_parking_area")
    op.execute("DROP TABLE IF EXISTS public.restaurant")
    op.execute("DROP SEQUENCE IF EXISTS public.chajoo_dist_id_seq")
    op.execute("DROP TABLE IF EXISTS public.chajoo_dist")
