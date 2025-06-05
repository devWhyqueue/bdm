# Iceberg Catalog and Table Initialization for Trusted Zone (Automated)

This directory contains scripts to initialize an Apache Iceberg catalog and tables (`reddit_posts`, `finnhub_articles`) within the MinIO `trusted-zone`.
The initialization is **automatically triggered** when you start the Docker Compose environment.

## Prerequisites

- Docker and Docker Compose installed.

## Overview of Automated Docker Setup

-   **MinIO**: The `minio` service in `docker-compose.yml` provides S3-compatible storage. The `createbuckets` service automatically creates the `trusted-zone` bucket upon startup.
-   **Spark Configuration**: Spark settings for Iceberg and MinIO (S3A) are defined in `architecture/spark-defaults.conf`. This file is mounted into the Spark containers.
-   **Automated Initialization**:
    *   A service named `iceberg-init-runner` is defined in `docker-compose.yml`.
    *   This service automatically executes the `run_iceberg_init.sh` script after the `minio` and `createbuckets` services are ready.
    *   The script creates the Iceberg catalog and tables if they don't already exist and runs verification steps.
-   **Ad-hoc Spark Access**: The `spark-iceberg` service is also available if you need to run ad-hoc `spark-sql` queries or Spark applications separately.

## Scripts

The following scripts are executed automatically by the `iceberg-init-runner` service:

1.  **`01_create_catalog.sql`**:
    *   **Purpose**: Creates an Iceberg catalog named `catalog` IF IT DOES NOT ALREADY EXIST.
    *   **Details**: Points to `s3a://trusted-zone/iceberg_catalog/` in MinIO.

2.  **`02_create_reddit_posts_table.sql`**:
    *   **Purpose**: Creates the `catalog.reddit_posts` Iceberg table IF IT DOES NOT ALREADY EXIST.
    *   **Schema**: Defines columns for Reddit post data.
    *   **Partitioning**: Partitioned by `days(scraped_at)`.

3.  **`03_create_finnhub_articles_table.sql`**:
    *   **Purpose**: Creates the `catalog.finnhub_articles` Iceberg table IF IT DOES NOT ALREADY EXIST.
    *   **Schema**: Defines columns for Finnhub article data.
    *   **Partitioning**: Partitioned by `days(scraped_at)`.

4.  **`04_verify_tables.sql`**:
    *   **Purpose**: Contains SQL statements to verify the setup (DESCRIBE tables, INSERT sample data, SELECT sample data). These operations help confirm that the tables are writable and readable. The sample data insertion is idempotent in the sense that re-running it might insert duplicate sample data if not designed with unique keys for the samples, but the table creation itself is idempotent.

5.  **`run_iceberg_init.sh`**:
    *   **Purpose**: Orchestrates the execution of the above SQL scripts using `spark-sql` within the `iceberg-init-runner` container.
    *   **Idempotency**: The SQL scripts use `IF NOT EXISTS` for catalog and table creation, making the initialization process safe to re-run.
    *   **Verification**: Includes execution of `04_verify_tables.sql`. Success implies basic functionality.

## Execution and Verification

1.  **Start Docker Compose services**:
    From the repository root:
    ```bash
    docker-compose up -d
    ```
    (Or `docker-compose up` if you want to see logs in the foreground).
    The `iceberg-init-runner` service will automatically start and execute `run_iceberg_init.sh`.

2.  **Check Initialization Logs**:
    To see the output of the initialization script and check for success or errors:
    ```bash
    docker-compose logs iceberg-init-runner
    ```
    Look for "Iceberg Initialization and Verification Completed Successfully" at the end of the logs. If the script encounters an error (due to `set -e`), the service will likely show as exited with a non-zero code.

3.  **Manual MinIO Verification (Optional)**:
    If you want to inspect the MinIO bucket directly after the initialization:
    ```bash
    # List contents of the Iceberg catalog's base path
    docker-compose exec minio mc ls myminio/trusted-zone/iceberg_catalog/

    # List contents of the 'catalog' database directory
    docker-compose exec minio mc ls myminio/trusted-zone/iceberg_catalog/catalog/

    # Verify reddit_posts metadata
    docker-compose exec minio mc ls myminio/trusted-zone/iceberg_catalog/catalog/reddit_posts/metadata/

    # Verify finnhub_articles metadata
    docker-compose exec minio mc ls myminio/trusted-zone/iceberg_catalog/catalog/finnhub_articles/metadata/
    ```
    You can also check the MinIO UI (usually at `http://localhost:9001`) for the `trusted-zone` bucket and its contents.

## Acceptance Criteria Met

*   Iceberg catalog `catalog` is automatically created at `s3a://trusted-zone/iceberg_catalog/` on `docker-compose up`.
*   Tables `catalog.reddit_posts` and `catalog.finnhub_articles` are automatically created with specified schemas and partitioning.
*   The process is idempotent due to `IF NOT EXISTS` in DDLs.
*   Verification (including test inserts/selects) is part of the automated script.
*   Iceberg metadata is stored in MinIO as expected.
```
