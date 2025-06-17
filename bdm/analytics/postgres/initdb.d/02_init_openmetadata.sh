#!/bin/bash
set -e

# This script will be executed as the POSTGRES_USER (postgres in this case)
# which has superuser privileges and can create databases and roles.

psql -v ON_ERROR_STOP=1 --username "$POSTGRES_USER" --dbname "$POSTGRES_DB" <<-EOSQL
    -- Create the openmetadata database if it doesn't already exist
    SELECT 'CREATE DATABASE openmetadata_db'
    WHERE NOT EXISTS (SELECT FROM pg_database WHERE datname = 'openmetadata_db')\gexec

    -- Create the openmetadata user if it doesn't already exist
    DO
    \$do\$
    BEGIN
       IF NOT EXISTS (
          SELECT FROM pg_catalog.pg_roles
          WHERE  rolname = 'openmetadata_user') THEN

           CREATE USER openmetadata_user WITH PASSWORD 'openmetadata_password';
       END IF;
    END
    \$do\$;

    -- Grant all privileges on the openmetadata database to the openmetadata user
    GRANT ALL PRIVILEGES ON DATABASE openmetadata_db TO openmetadata_user;

    -- Optional: If you want the openmetadata user to be able to create schemas in their database
    GRANT CREATE ON DATABASE openmetadata_db TO openmetadata_user;
EOSQL

psql -v ON_ERROR_STOP=1 \
     --username "$POSTGRES_USER" \
     --dbname   "openmetadata_db" <<-EOSQL
    /* Allow openmetadata_user to create objects in the existing public schema */
    GRANT USAGE, CREATE ON SCHEMA public TO openmetadata_user;

    /* Optional but recommended: make future tables in public writable */
    ALTER DEFAULT PRIVILEGES IN SCHEMA public
           GRANT SELECT, INSERT, UPDATE, DELETE
           ON TABLES TO openmetadata_user;
EOSQL

echo "OpenMetadata database 'openmetadata_db' and user 'openmetadata_user' initialization complete."
