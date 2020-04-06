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
  },
  redshift: {
    user: process.env.REDSHIFT_USER,
    database: process.env.REDSHIFT_DATABASE,
    password: process.env.REDSHIFT_PASSWORD,
    port: process.env.REDSHIFT_PORT,
    host: process.env.REDSHIFT_HOST,
  },
};

module.exports = property;
