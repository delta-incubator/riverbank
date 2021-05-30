-- The simple tokens table

CREATE EXTENSION IF NOT EXISTS pgcrypto;

CREATE TABLE shares (
    id UUID PRIMARY KEY,
    name TEXT NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL
);

CREATE TABLE schemas (
    id UUID PRIMARY KEY,
    name TEXT NOT NULL,
    share_id UUID NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL,
    CONSTRAINT fk_share FOREIGN KEY(share_id) REFERENCES shares(id)
);

CREATE TABLE tables (
    id UUID PRIMARY KEY,
    name TEXT NOT NULL,
    schema_id UUID NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL,
    CONSTRAINT fk_schema FOREIGN KEY(schema_id) REFERENCES schemas(id)
);

CREATE TABLE tokens (
    id UUID PRIMARY KEY,
    token TEXT NOT NULL,
    expires_at TIMESTAMP WITH TIME ZONE DEFAULT (NOW() + interval '30 days') NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL
);
CREATE TABLE tokens_for_tables (
    id UUID PRIMARY KEY,
    token_id UUID NOT NULL,
    table_id UUID NOT NULL,
    created_at TIMESTAMP WITH TIME ZONE DEFAULT NOW() NOT NULL,
    CONSTRAINT fk_token FOREIGN KEY(token_id) REFERENCES tokens(id),
    CONSTRAINT fk_table FOREIGN KEY(table_id) REFERENCES tables(id)
);
