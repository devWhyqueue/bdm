-- Create an Iceberg catalog named 'catalog' pointing to MinIO, if it does not already exist:
CREATE CATALOG IF NOT EXISTS catalog
  USING iceberg
  WITH (
    'type'='hadoop',
    'warehouse'='s3a://trusted-zone/iceberg_catalog/'
  );
