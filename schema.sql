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

CREATE TABLE IF NOT EXIST tv_aug_kpi_results (
    start_time              DATE,
    end_time                DATE,
    query_date              DATE,
    type                    VARCHAR,
    crid                    VARCHAR,
    adult                   BOOLEAN,
    title_name              VARCHAR,
    description             VARCHAR(65535),
    episode_number          INTEGER,
    season_number           INTEGER,
    series_name             VARCHAR,
    region                  VARCHAR,
    is_on_demand            BOOLEAN,
    hits                    BIGINT,
    api_request_number      BIGINT,
    video_results           VARCHAR(65535),
    video_response_code     INTEGER
);

CREATE TABLE IF NOT EXIST tv_aug_content_provider_results (
    start_time              DATE,
    end_time                DATE,
    query_date              DATE,
    region                  VARCHAR,
    total_hits              BIGINT,
    content_provider        VARCHAR,
    crid_counts             BIGINT
);

CREATE TABLE IF NOT EXIST cwl_mediatailor_fillrate (
    query_date              TIMESTAMP,
    origin_id               VARCHAR,
    filled_duration_sum     BIGINT,
    origin_avail_duration_sum        BIGINT,
    num_ads_sum             BIGINT
);