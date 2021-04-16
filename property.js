const property = {
  aws: {
    fromBucketName: process.env.S3_FROM_BUCKET_NAME,
    toBucketName: process.env.S3_TO_BUCKET_NAME,
    prefix: process.env.S3_BUCKET_PREFIX,
    jsonPutKeyFolder: process.env.S3_JSON_PUT_KEY_FOLDER,
    csvPutKeyFolder: process.env.S3_CSV_PUT_KEY_FOLDER,
    tvaugFromBucketName: process.env.S3_TVAUG_FROM_BUCKET_NAME,
    tvaugPrefix: process.env.S3_TVAUG_BUCKET_PREFIX,
    tvaugJsonPutKeyFolder: process.env.S3_TVAUG_JSON_PUT_KEY_FOLDER,
    tvaugWeeklySnapshotFolder: process.env.S3_TVAUG_WEEKLY_SNAPSHOT_FOLDER,
    fillRateFileLog: process.env.FILL_RATE_FILE_LOG,
  },
  redshift: {
    user: process.env.REDSHIFT_USER,
    database: process.env.REDSHIFT_DATABASE,
    password: process.env.REDSHIFT_PASSWORD,
    port: process.env.REDSHIFT_PORT,
    host: process.env.REDSHIFT_HOST,
  },
  tvaug: {
    be: {
      device: process.env.TVAUG_API_BE_DEVICE_ID,
      token: process.env.TVAUG_API_BE_TOKEN,
    },
    ch: {
      device: process.env.TVAUG_API_CH_DEVICE_ID,
      token: process.env.TVAUG_API_CH_TOKEN,
    },
    uk: {
      device: process.env.TVAUG_API_UK_DEVICE_ID,
      token: process.env.TVAUG_API_UK_TOKEN,
    },
    ie: {
      device: process.env.TVAUG_API_IE_DEVICE_ID,
      token: process.env.TVAUG_API_IE_TOKEN,
    },
  },
};

module.exports = property;
