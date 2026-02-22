--
-- PostgreSQL database dump
--

\restrict 7K6KdobuYOIV1fkg7Tvkc1G21gVllDPue67IfAbctGsopcHIK86TfjBk71O303V

-- Dumped from database version 16.12 (Debian 16.12-1.pgdg13+1)
-- Dumped by pg_dump version 16.12 (Debian 16.12-1.pgdg13+1)

SET statement_timeout = 0;
SET lock_timeout = 0;
SET idle_in_transaction_session_timeout = 0;
SET client_encoding = 'UTF8';
SET standard_conforming_strings = on;
SELECT pg_catalog.set_config('search_path', '', false);
SET check_function_bodies = false;
SET xmloption = content;
SET client_min_messages = warning;
SET row_security = off;

SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: alembic_version; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.alembic_version (
    version_num character varying(32) NOT NULL
);


--
-- Name: chajoo_dist; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.chajoo_dist (
    id integer NOT NULL,
    sido text NOT NULL,
    sigungu text NOT NULL,
    "SHP_CD" text,
    cargo_count bigint,
    "전략적_중요도" double precision,
    year text NOT NULL,
    month text NOT NULL
);


--
-- Name: chajoo_dist_id_seq; Type: SEQUENCE; Schema: public; Owner: -
--

CREATE SEQUENCE public.chajoo_dist_id_seq
    AS integer
    START WITH 1
    INCREMENT BY 1
    NO MINVALUE
    NO MAXVALUE
    CACHE 1;


--
-- Name: chajoo_dist_id_seq; Type: SEQUENCE OWNED BY; Schema: public; Owner: -
--

ALTER SEQUENCE public.chajoo_dist_id_seq OWNED BY public.chajoo_dist.id;


--
-- Name: restaurant; Type: TABLE; Schema: public; Owner: -
--

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
);


--
-- Name: truckhelper_parking_area; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.truckhelper_parking_area (
    "공영차고지명" text NOT NULL,
    "주소" text NOT NULL,
    lat double precision,
    lon double precision
);


--
-- Name: user_view_history; Type: TABLE; Schema: public; Owner: -
--

CREATE TABLE public.user_view_history (
    id integer DEFAULT 1 NOT NULL,
    sigungu text NOT NULL,
    updated_at timestamp without time zone DEFAULT now() NOT NULL,
    CONSTRAINT check_single_row CHECK ((id = 1))
);


--
-- Name: chajoo_dist id; Type: DEFAULT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.chajoo_dist ALTER COLUMN id SET DEFAULT nextval('public.chajoo_dist_id_seq'::regclass);


--
-- Name: alembic_version alembic_version_pkc; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.alembic_version
    ADD CONSTRAINT alembic_version_pkc PRIMARY KEY (version_num);


--
-- Name: chajoo_dist chajoo_dist_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.chajoo_dist
    ADD CONSTRAINT chajoo_dist_pkey PRIMARY KEY (id);


--
-- Name: restaurant pk_restaurant; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.restaurant
    ADD CONSTRAINT pk_restaurant PRIMARY KEY ("업체명", "도로명주소", year, month, week);


--
-- Name: truckhelper_parking_area truckhelper_parking_area_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.truckhelper_parking_area
    ADD CONSTRAINT truckhelper_parking_area_pkey PRIMARY KEY ("공영차고지명", "주소");


--
-- Name: user_view_history user_view_history_pkey; Type: CONSTRAINT; Schema: public; Owner: -
--

ALTER TABLE ONLY public.user_view_history
    ADD CONSTRAINT user_view_history_pkey PRIMARY KEY (id);


--
-- PostgreSQL database dump complete
--

\unrestrict 7K6KdobuYOIV1fkg7Tvkc1G21gVllDPue67IfAbctGsopcHIK86TfjBk71O303V
