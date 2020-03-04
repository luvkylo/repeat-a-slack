# Depolyment instruction

## Mediatailor
1. Create a table in PROD redshift using the following schema:
```
CREATE TABLE IF NOT EXISTS cwl_mediatailor_ad_decision_server_interactions
(
    request_time                                TIMESTAMP,
    aws_account_id                              VARCHAR(65535),
    customer_id                                 VARCHAR(65535),
    event_description                           VARCHAR(65535),
    event_timestamp                             TIMESTAMP,
    event_type                                  VARCHAR(65535),
    origin_id                                   VARCHAR(65535),
    request_id                                  VARCHAR(65535),
    session_id                                  VARCHAR(65535),
    session_type                                VARCHAR(65535),
    beacon_info_beacon_http_response_code       VARCHAR,
    beacon_info_beacon_uri                      VARCHAR(65535),
    beacon_info_headers_0_name                  VARCHAR(65535),
    beacon_info_headers_0_value                 VARCHAR(65535),
    beacon_info_headers_1_name                  VARCHAR(65535),
    beacon_info_headers_1_value                 VARCHAR(65535),
    beacon_info_tracking_event                  VARCHAR(65535),
    message1                                    VARCHAR(65535),
    message2                                    VARCHAR(65535),
    message3                                    VARCHAR(65535),
    message4                                    VARCHAR(65535) 
);
```

2. Assign credential to Jenkin's job 
    - Database Name
    - Username
    - Password
    - Host
    - Port

3. Run COPY command to move past data from S3 to PROD redshift
```
COPY cwl_mediatailor_ad_decision_server_interactions from 's3://prd-freq-report-data-fr/mediaTailor-redshift-data-storage/mediaTailorData-${month}${day}${year}-${hour}${minute}.json' iam_role 'arn:aws:iam::077497804067:role/RedshiftS3Role' json 'auto' timeformat 'auto' REGION AS 'eu-central-1';
```

## TV Augmentation
1. Create a table in PROD redshift using the following schema:
```
CREATE TABLE IF NOT EXISTS cwl_metadata_api_fr
(
  request_time TIMESTAMP,
  wrf_keys VARCHAR(65535),
  external_identifier_source VARCHAR,
  external_identifier VARCHAR(65535),
  languages VARCHAR,
  http_code INTEGER,
  message VARCHAR(65535),
  message2 VARCHAR(65535),
  message3 VARCHAR(65535),
  message4 VARCHAR(65535),
  http_type VARCHAR,
  http_uri VARCHAR,
  ip_address VARCHAR
);
```

2. Assign credential to Jenkin's job 
    - Database Name
    - Username
    - Password
    - Host
    - Port

3. Run COPY command to move past data from S3 to PROD redshift
```
COPY cwl_metadata_api_fr from 's3://prd-freq-report-data-fr/prd-freq-metadata-api-data/mediaTailorData-${month}${day}${year}.json' iam_role 'arn:aws:iam::077497804067:role/RedshiftS3Role' json 'auto' timeformat 'auto' REGION AS 'eu-central-1';
```