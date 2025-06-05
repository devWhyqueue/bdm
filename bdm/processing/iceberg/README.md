# Iceberg Catalog and Table Initialization for Trusted Zone (Automated)

This directory contains scripts to initialize Apache Iceberg tables (`reddit_posts`, `finnhub_articles`) within the
MinIO `trusted-zone`.
The initialization is **automatically triggered** when you start the Docker Compose environment.

## Prerequisites

- Docker and Docker Compose installed.

## Overview of Automated Docker Setup

-   **MinIO**: The `minio` service in `docker-compose.yml` provides S3-compatible storage. The `createbuckets` service automatically creates the `trusted-zone` bucket upon startup.
- **Spark Configuration**: Core Spark settings for Iceberg and MinIO (S3A) are defined in
  `architecture/spark-defaults.conf` (e.g., `spark.sql.catalog.iceberg=org.apache.iceberg.spark.SparkCatalog`,
  `spark.sql.defaultCatalog=iceberg`). This file is mounted into the Spark containers. The warehouse path for the
  `iceberg` catalog (`s3a://trusted-zone/iceberg_catalog/`) is specified in the `run_iceberg_init.sh` script when
  invoking `spark-sql`.
-   **Automated Initialization**:
    *   A service named `iceberg-init-runner` is defined in `docker-compose.yml`.
    *   This service automatically executes the `run_iceberg_init.sh` script after the `minio` and `createbuckets` services are ready.
    * The script executes SQL files to create tables (e.g., `catalog.reddit_posts`, `catalog.finnhub_articles`) if they
      don't already exist, and runs verification steps. These tables are created within the `iceberg` catalog (e.g., as
      `iceberg.catalog.reddit_posts` if the SQL uses `CREATE TABLE catalog.reddit_posts`).
-   **Ad-hoc Spark Access**: The `spark-iceberg` service is also available if you need to run ad-hoc `spark-sql` queries or Spark applications separately.

## Scripts

The following scripts are executed automatically by the `iceberg-init-runner` service:

1. **`01_create_reddit_posts_table.sql`**:
    * **Purpose**: Creates the `catalog.reddit_posts` Iceberg table (effectively `iceberg.catalog.reddit_posts`) IF IT
      DOES NOT ALREADY EXIST.
    *   **Schema**: Defines columns for Reddit post data.
    *   **Partitioning**: Partitioned by `days(scraped_at)`.

2. **`02_create_finnhub_articles_table.sql`**:
    * **Purpose**: Creates the `catalog.finnhub_articles` Iceberg table (effectively `iceberg.catalog.finnhub_articles`)
      IF IT DOES NOT ALREADY EXIST.
    *   **Schema**: Defines columns for Finnhub article data.
    *   **Partitioning**: Partitioned by `days(scraped_at)`.

3. **`03_verify_tables.sql`**:
    * **Purpose**: Contains SQL statements to verify the setup (DESCRIBE tables, INSERT sample data, SELECT sample data
      for `catalog.reddit_posts` and `catalog.finnhub_articles`). These operations help confirm that the tables are
      writable and readable. The sample data insertion is idempotent in the sense that re-running it might insert
      duplicate sample data if not designed with unique keys for the samples, but the table creation itself is
      idempotent.

4. **`run_iceberg_init.sh`**:
    * **Purpose**: Orchestrates the execution of the SQL scripts for table creation and verification using `spark-sql`
      within the `iceberg-init-runner` container. It also sets the warehouse path for the `iceberg` catalog.
    * **Idempotency**: The SQL scripts use `IF NOT EXISTS` for table creation, making the initialization process safe to
      re-run.
    * **Verification**: Includes execution of `03_verify_tables.sql`. Success implies basic functionality.

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

    You can also check the MinIO UI (usually at `http://localhost:9001`) for the `trusted-zone` bucket and its contents.
