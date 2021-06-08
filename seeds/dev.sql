--
-- PostgreSQL database dump
--

-- Dumped from database version 12.4 (Debian 12.4-1.pgdg100+1)
-- Dumped by pg_dump version 13.2

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

--
-- Name: pgcrypto; Type: EXTENSION; Schema: -; Owner: -
--

CREATE EXTENSION IF NOT EXISTS pgcrypto WITH SCHEMA public;


--
-- Name: EXTENSION pgcrypto; Type: COMMENT; Schema: -; Owner: 
--

COMMENT ON EXTENSION pgcrypto IS 'cryptographic functions';


SET default_tablespace = '';

SET default_table_access_method = heap;

--
-- Name: _sqlx_migrations; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public._sqlx_migrations (
    version bigint NOT NULL,
    description text NOT NULL,
    installed_on timestamp with time zone DEFAULT now() NOT NULL,
    success boolean NOT NULL,
    checksum bytea NOT NULL,
    execution_time bigint NOT NULL
);


ALTER TABLE public._sqlx_migrations OWNER TO postgres;

--
-- Name: schemas; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.schemas (
    id uuid DEFAULT public.gen_random_uuid() NOT NULL,
    name text NOT NULL,
    share_id uuid NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);


ALTER TABLE public.schemas OWNER TO postgres;

--
-- Name: shares; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.shares (
    id uuid DEFAULT public.gen_random_uuid() NOT NULL,
    name text NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);


ALTER TABLE public.shares OWNER TO postgres;

--
-- Name: tables; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.tables (
    id uuid DEFAULT public.gen_random_uuid() NOT NULL,
    name text NOT NULL,
    schema_id uuid NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL,
    location text NOT NULL
);


ALTER TABLE public.tables OWNER TO postgres;

--
-- Name: tokens; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.tokens (
    id uuid DEFAULT public.gen_random_uuid() NOT NULL,
    token text NOT NULL,
    expires_at timestamp with time zone DEFAULT (now() + '30 days'::interval) NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);


ALTER TABLE public.tokens OWNER TO postgres;

--
-- Name: tokens_for_tables; Type: TABLE; Schema: public; Owner: postgres
--

CREATE TABLE public.tokens_for_tables (
    id uuid DEFAULT public.gen_random_uuid() NOT NULL,
    token_id uuid NOT NULL,
    table_id uuid NOT NULL,
    created_at timestamp with time zone DEFAULT now() NOT NULL
);


ALTER TABLE public.tokens_for_tables OWNER TO postgres;

--
-- Data for Name: _sqlx_migrations; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public._sqlx_migrations (version, description, installed_on, success, checksum, execution_time) FROM stdin;
20210530213127	simple tokens	2021-05-30 21:51:02.339722+00	t	\\x169f48de73792104d974bb0793b6cd4ca6dce759e75000a883d5cbc387b291d2838c619eda7362cd4d312d329acf8427	50215984
20210530215400	location for tables	2021-05-30 21:55:21.35334+00	t	\\xed47e0f38faaab6a4f4dc2f3854f0b55aa4d30d2ffe7dbc603e45bc343a2447ca6e76dbe5d4faed0c58d1836812ac2e3	2154908
\.


--
-- Data for Name: schemas; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.schemas (id, name, share_id, created_at) FROM stdin;
970be392-5de7-479b-a6a0-b027368bcdf8	samples	fcb12100-2590-496d-9578-d86e2d3ca831	2021-05-30 21:52:44.279493+00
\.


--
-- Data for Name: shares; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.shares (id, name, created_at) FROM stdin;
fcb12100-2590-496d-9578-d86e2d3ca831	rtyler	2021-05-30 21:52:12.81593+00
\.


--
-- Data for Name: tables; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.tables (id, name, schema_id, created_at, location) FROM stdin;
64947f1f-ef50-4280-bf6b-160c075cde43	covid19-nyt	970be392-5de7-479b-a6a0-b027368bcdf8	2021-05-30 21:55:50.992624+00	s3://delta-riverbank/COVID-19_NYT
\.


--
-- Data for Name: tokens; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.tokens (id, token, expires_at, created_at) FROM stdin;
\.


--
-- Data for Name: tokens_for_tables; Type: TABLE DATA; Schema: public; Owner: postgres
--

COPY public.tokens_for_tables (id, token_id, table_id, created_at) FROM stdin;
\.


--
-- Name: _sqlx_migrations _sqlx_migrations_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public._sqlx_migrations
    ADD CONSTRAINT _sqlx_migrations_pkey PRIMARY KEY (version);


--
-- Name: schemas schemas_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.schemas
    ADD CONSTRAINT schemas_pkey PRIMARY KEY (id);


--
-- Name: shares shares_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.shares
    ADD CONSTRAINT shares_pkey PRIMARY KEY (id);


--
-- Name: tables tables_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.tables
    ADD CONSTRAINT tables_pkey PRIMARY KEY (id);


--
-- Name: tokens_for_tables tokens_for_tables_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.tokens_for_tables
    ADD CONSTRAINT tokens_for_tables_pkey PRIMARY KEY (id);


--
-- Name: tokens tokens_pkey; Type: CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.tokens
    ADD CONSTRAINT tokens_pkey PRIMARY KEY (id);


--
-- Name: tables fk_schema; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.tables
    ADD CONSTRAINT fk_schema FOREIGN KEY (schema_id) REFERENCES public.schemas(id);


--
-- Name: schemas fk_share; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.schemas
    ADD CONSTRAINT fk_share FOREIGN KEY (share_id) REFERENCES public.shares(id);


--
-- Name: tokens_for_tables fk_table; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.tokens_for_tables
    ADD CONSTRAINT fk_table FOREIGN KEY (table_id) REFERENCES public.tables(id);


--
-- Name: tokens_for_tables fk_token; Type: FK CONSTRAINT; Schema: public; Owner: postgres
--

ALTER TABLE ONLY public.tokens_for_tables
    ADD CONSTRAINT fk_token FOREIGN KEY (token_id) REFERENCES public.tokens(id);


--
-- PostgreSQL database dump complete
--

