#!/bin/bash
set -e

# This script will be executed as the POSTGRES_USER (airflow in this case)
# which has superuser privileges and can create databases and roles.

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    -- Create the analytics database if it doesn't already exist
    SELECT 'CREATE DATABASE ${POSTGRES_ANALYTICS_DB}'
    WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = '${POSTGRES_ANALYTICS_DB}')\gexec

    -- Create the analytics user if it doesn't already exist
    DO
    \$do\$
    BEGIN
       IF NOT EXISTS (
          SELECT FROM pg_catalog.pg_roles
          WHERE  rolname = '${POSTGRES_ANALYTICS_USER}') THEN

          CREATE USER ${POSTGRES_ANALYTICS_USER} WITH PASSWORD '${POSTGRES_ANALYTICS_PASSWORD}';
       END IF;
    END
    \$do\$;

    -- Grant all privileges on the analytics database to the analytics user
    GRANT ALL PRIVILEGES ON DATABASE ${POSTGRES_ANALYTICS_DB} TO ${POSTGRES_ANALYTICS_USER};

    -- Optional: If you want the analytics user to be able to create schemas in their database
    GRANT CREATE ON DATABASE ${POSTGRES_ANALYTICS_DB} TO ${POSTGRES_ANALYTICS_USER};
EOSQL

psql -v ON_ERROR_STOP=1 \
     --username "$POSTGRES_USER" \
     --dbname   "$POSTGRES_ANALYTICS_DB" <<-EOSQL
    /* Allow analytics_user to create objects in the existing public schema */
    GRANT USAGE, CREATE ON SCHEMA public TO ${POSTGRES_ANALYTICS_USER};

    /* Optional but recommended: make future tables in public writable */
    ALTER DEFAULT PRIVILEGES IN SCHEMA public
           GRANT SELECT, INSERT, UPDATE, DELETE
           ON TABLES TO ${POSTGRES_ANALYTICS_USER};
EOSQL

echo "Analytics database '${POSTGRES_ANALYTICS_DB}' and user '${POSTGRES_ANALYTICS_USER}' initialization complete."
