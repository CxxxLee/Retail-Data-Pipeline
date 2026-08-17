-- ============================================================
-- SNOWFLAKE ENVIRONMENT SETUP
-- ============================================================

USE ROLE ACCOUNTADMIN;


-- ============================================================
-- SECTION 1: CREATE WAREHOUSE, DATABASE, AND SCHEMA
-- ============================================================

CREATE WAREHOUSE IF NOT EXISTS PROJECT_WH
    WAREHOUSE_SIZE = 'XSMALL'
    AUTO_SUSPEND = 60
    AUTO_RESUME = TRUE;

CREATE DATABASE IF NOT EXISTS RETAIL_PROJECT_DB;

CREATE SCHEMA IF NOT EXISTS RETAIL_PROJECT_DB.CURATED;


-- ============================================================
-- SECTION 2: CREATE EXTERNAL VOLUME
-- ============================================================

CREATE OR REPLACE EXTERNAL VOLUME RETAIL_ICEBERG_VOLUME
    STORAGE_LOCATIONS =
    (
        (
            NAME = 'retail_iceberg_location'
            STORAGE_PROVIDER = 'S3'
            STORAGE_BASE_URL =
                's3://rev1-249954438267-us-east-1-an/iceberg/'
            STORAGE_AWS_ROLE_ARN =
                'arn:aws:iam::249954438267:role/SnowFlakeIceberg'
        )
    )
    ALLOW_WRITES = FALSE;


-- Display external-volume properties.
DESC EXTERNAL VOLUME RETAIL_ICEBERG_VOLUME;


-- Verify Snowflake can access the S3 location.
SELECT SYSTEM$VERIFY_EXTERNAL_VOLUME(
    'RETAIL_ICEBERG_VOLUME'
);


-- ============================================================
-- SECTION 3: CREATE AWS GLUE CATALOG INTEGRATION
-- ============================================================

CREATE OR REPLACE CATALOG INTEGRATION RETAIL_GLUE_CATALOG
    CATALOG_SOURCE = GLUE
    CATALOG_NAMESPACE = 'iceberg_catalog_db'
    TABLE_FORMAT = ICEBERG
    GLUE_AWS_ROLE_ARN =
        'arn:aws:iam::249954438267:role/SnowFlakeGlueRole'
    GLUE_CATALOG_ID = '249954438267'
    GLUE_REGION = 'us-east-1'
    ENABLED = TRUE;


-- Display Glue integration properties.
DESC CATALOG INTEGRATION RETAIL_GLUE_CATALOG;