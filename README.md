# CMS Analyze Tools Data Collection
## Summary
    Extract data from Fastly, Cloudwatch, Mediatailor and through data manipulation, store them in Redshift and visualize them in AWS QuickSight.

## Mediatailor
-----------------------------------------------------------------------
### Data Flow
1. Mediatailor events are logged into Cloudwatch
2. Kinesis Firehouse log events from Cloudwatch to S3 every 15 minutes to the following location:
3. The log is then downloaded to Jenkins for data manipulation is re-upload to the following two locations as 
4. The data in JSON file is then copied to a Redshift Database using the COPY query
5. The data in CSV file is being read by QuickSight SPICE, and is load into the dataset (scheduled refresh hourly)
Confluence link:
https://docs.frequency.com/display/CO/MediaTailor+Cloudwatch+Data+to+Quicksight
------------------------------
### Github Reporsitory for QA
```
https://github.com/frequency/frequency-test-poc/tree/mediatailor_to_redshift
```
The scripts are run in 15 minutes interval

-------------------------------
### Quicksight Row Level Security
Amazon doc: https://docs.aws.amazon.com/quicksight/latest/user/restrict-access-to-a-data-set-using-row-level-security.html

Permission CSV location: 
```
prd-freq-report-data-fr/permission.csv
```

Quicksight dataset name:
```
mediatailor_row_level_permission
```
---------------------------------
### Schema
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
-------------------------------
### S3 to Redshift query command
```
COPY cwl_mediatailor_ad_decision_server_interactions from 's3://prd-freq-report-data-fr/mediaTailor-redshift-data-storage/mediaTailorData-${month}${day}${year}-${hour}${minute}.json' iam_role 'arn:aws:iam::077497804067:role/RedshiftS3Role' json 'auto' timeformat 'auto' REGION AS 'eu-central-1';
```

## TV Augmentation
-------------------------------
### Data Flow
1. LGI pushes raw JSON file into our s3 bucket daily
2. The script download all files and clean the data
3. The script then split the data into their first level objects (i.e. events, channels, contents, credits, genres, pictures, products, series, titles)
4. The script then upload them to another s3 bucket
5. The script will then run a COPY command to ingest those data into Redshift
Confluence link:
https://docs.frequency.com/display/CO/TV_Aug+Metadata+%28LGI%29+From+S3+to+Redshift
------------------------------
### Github Reporsitory for QA
```
https://github.com/frequency/frequency-data-analyze/tree/LGI_metadata_dev
```
The scripts are run in 24 hours interval

## Fastly