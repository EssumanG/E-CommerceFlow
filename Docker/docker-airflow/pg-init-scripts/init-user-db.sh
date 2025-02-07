#!/bin/bash
set -e

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
	CREATE USER test WITH PASSWORD 'postgres';
	CREATE DATABASE ecommerce;
	GRANT ALL PRIVILEGES ON DATABASE ecommerce TO test;

    \c ecommerce
    CREATE EXTENSION IF NOT EXISTS "uuid-ossp";
EOSQL