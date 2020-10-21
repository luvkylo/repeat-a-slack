/* eslint-disable max-len */
/* eslint-disable no-await-in-loop */
require('dotenv').config();

const AWS = require('aws-sdk');
const zlib = require('zlib');
const Redshift = require('node-redshift');
const fs = require('fs');
const path = require('path');
const cliProgress = require('cli-progress');
const geoip = require('geoip-lite');
const { s3multipartUpload } = require('./s3_multipart_uploader');
// const property = require('./property_local');
const property = require('./property');

// Config AWS connection
AWS.config.update({});

// Create S3 connection
const s3 = new AWS.S3();

AWS.config.update({ region: 'eu-central-1' });

const client = property.redshift;

// Create Redshift connection
const redshiftClient2 = new Redshift(client, { rawConnection: true });

let completed = '';

s3.getObject({
  Bucket: property.aws.toBucketName,
  Key: 'prd_completedRecord.txt',
}, (err, data) => {
  if (err) throw err;
  else completed = data.Body.toString();
});

// Get date for file name
let date = new Date();

date = new Date(Date.UTC(date.getUTCFullYear(), date.getUTCMonth(),
  date.getUTCDate(), date.getUTCHours(), date.getUTCMinutes()));

const lastMonth = new Date();
lastMonth.setUTCDate(1);
lastMonth.setUTCHours(-1);
const lastDay = lastMonth.getUTCDate();

let month = date.getUTCMonth() + 1;
const year = date.getUTCFullYear();
let hour = date.getUTCHours();
let day = date.getUTCDate();
month = day === 0 ? month - 1 : month;
month = month < 10 ? `0${month}` : month;
day = day === 0 ? lastDay : day;
day = day < 10 ? `0${day}` : day;
let minute = date.getUTCMinutes();
hour = hour < 10 ? `0${hour}` : hour;
minute = minute < 10 ? `0${minute}` : minute;

console.log(`Processing file on this day: ${date.toISOString()}`);

// Initialize variables for mutiparts upload
const fileName = `mediaTailorData-${month}${day}${year}-${hour}${minute}.json`;
const filePath = path.join(__dirname, 'JSON', fileName);
let arr = [];
let lastModified = '';
let lookBack = false;

// Create and open stream to write to temporary JSON file
const stream = fs.createWriteStream(filePath, { flags: 'w' });

// Params for calls
const bucketParams = {
  Bucket: property.aws.fromBucketName,
  Delimiter: '',
  Prefix: `${property.aws.prefix}Firehouse/${year}/${month}/${day}/`,
};

const getParams = {
  Bucket: property.aws.fromBucketName,
};

console.log(`Looking into the bucket prefix: ${bucketParams.Prefix}`);

console.log('Getting all objects in S3 bucket...');

function getRecord() {
  return new Promise((resolve) => {
    s3.getObject({
      Bucket: property.aws.toBucketName,
      Key: 'prd_completedRecord.txt',
    }, (err, data) => {
      if (err) { throw new Error(err); } else resolve(data.Body.toString());
    });
  });
}

function parse(key) {
  return new Promise((resolve) => {
    getParams.Key = key;

    // Get the file using key
    s3.getObject(getParams, (getErr, getData) => {
      if (getErr) throw new Error(getErr);
      else {
      // Uncompress the data returned
        console.log(key);
        const progress = new cliProgress.SingleBar({}, cliProgress.Presets.legacy);
        const buffer = new Buffer.alloc(getData.ContentLength, getData.Body, 'base64');
        zlib.unzip(buffer, (zipErr, zipData) => {
          if (zipErr) throw new Error(zipErr);
          else {
            // Split each data into array when new line
            let logData = zipData.toString().split('{"messageType"');

            // Create variables for json
            const objArr = ['awsAccountId', 'customerId', 'eventDescription', 'eventType', 'originId', 'requestId', 'sessionId', 'sessionType', 'beaconInfo'];

            logData = logData.filter((el) => el !== '');
            let { length } = logData;
            progress.start(length);

            // For every row of data
            logData.forEach((logsItem) => {
              const logs = `{"messageType"${logsItem}`;
              let logObj = JSON.parse(logs);

              if (logObj.logEvents.length > 1) {
                length += logObj.logEvents.length - 1;
                progress.setTotal(length);
              }

              logObj.logEvents.forEach((logItem) => {
                const log = logItem;
                const regex = /\u0000/g;
                const matchArr = log.message.match(regex);
                if (matchArr) {
                  log.message = log.message.replace(/\u0000/, `(NULL*${matchArr.length})`);
                  log.message = log.message.replace(regex, '');
                }

                const epochTime = new Date(0);
                epochTime.setUTCMilliseconds(log.timestamp);
                const stampYear = epochTime.getUTCFullYear();
                let stampMonth = epochTime.getUTCMonth() + 1;
                stampMonth = stampMonth < 10 ? `0${stampMonth}` : stampMonth;
                let stampDay = epochTime.getUTCDate();
                stampDay = stampDay < 10 ? `0${stampDay}` : stampDay;
                let stampHour = epochTime.getUTCHours();
                stampHour = stampHour < 10 ? `0${stampHour}` : stampHour;
                let stampMinute = epochTime.getUTCMinutes();
                stampMinute = stampMinute < 10 ? `0${stampMinute}` : stampMinute;
                let stampSec = epochTime.getUTCSeconds();
                stampSec = stampSec < 10 ? `0${stampSec}` : stampSec;
                let stampMiliSec = epochTime.getUTCMilliseconds();
                stampMiliSec = stampMiliSec < 100 ? `0${stampMiliSec}` : stampMiliSec;
                stampMiliSec = stampMiliSec < 10 ? `0${stampMiliSec}` : stampMiliSec;
                const time = `${stampYear}-${stampMonth}-${stampDay}T${stampHour}:${stampMinute}:${stampSec}.${stampMiliSec}Z`;

                const { message } = log;
                const jsonObj = JSON.parse(message);

                objArr.forEach((ele) => {
                  if (!jsonObj[ele]) {
                    jsonObj[ele] = '';
                  }
                });

                let beacon_info_headers_0_name = '';
                let beacon_info_headers_0_value = '';
                let beacon_info_headers_1_name = '';
                let beacon_info_headers_1_value = '';
                let city = '';
                let country_iso = '';
                let region = '';
                if (jsonObj.beaconInfo && jsonObj.beaconInfo.headers) {
                  jsonObj.beaconInfo.headers.forEach((header) => {
                    if (header.name) {
                      if (header.name === 'User-Agent') {
                        beacon_info_headers_0_name = `"${header.name.trim().replace(/\"/g, "'")}"`;
                        beacon_info_headers_0_value = `"${header.value.trim().replace(/\"/g, "'")}"`;
                      } else if (header.name === 'X-Forwarded-For') {
                        beacon_info_headers_1_name = `"${header.name.trim().replace(/\"/g, "'")}"`;
                        beacon_info_headers_1_value = `"${header.value.trim().replace(/\"/g, "'")}"`;
                        const ip = beacon_info_headers_1_value.replace(/\"/g, '');
                        const geoInfo = geoip.lookup(ip);
                        if (geoInfo) {
                          city = geoInfo.city;
                          country_iso = geoInfo.country;
                          region = geoInfo.region;
                        }
                      }
                    }
                  });
                }

                // Create an object to be transform into JSON
                logObj = {
                  request_time: time,
                  aws_account_id: jsonObj.awsAccountId,
                  customer_id: jsonObj.customerId,
                  event_description: jsonObj.eventDescription,
                  event_timestamp: jsonObj.eventTimestamp,
                  event_type: jsonObj.eventType,
                  origin_id: jsonObj.originId,
                  request_id: jsonObj.requestId,
                  session_id: jsonObj.sessionId,
                  session_type: jsonObj.sessionType,
                  beacon_info_beacon_http_response_code: ((jsonObj.beaconInfo && typeof jsonObj.beaconInfo.beaconHttpResponseCode !== 'undefined') ? jsonObj.beaconInfo.beaconHttpResponseCode : ''),
                  beacon_info_beacon_uri: ((jsonObj.beaconInfo && typeof jsonObj.beaconInfo.beaconUri !== 'undefined') ? jsonObj.beaconInfo.beaconUri : ''),
                  beacon_info_headers_0_name,
                  beacon_info_headers_0_value,
                  beacon_info_headers_1_name,
                  beacon_info_headers_1_value,
                  beacon_info_tracking_event: ((jsonObj.beaconInfo && typeof jsonObj.beaconInfo.trackingEvent !== 'undefined') ? jsonObj.beaconInfo.trackingEvent : ''),
                  city,
                  country_iso,
                  region,
                };

                // Transform object into JSON string
                const temp = JSON.stringify(logObj).replace(/\n|\r/g, '');

                // Write the JSON string into the temporary JSON file
                stream.write(`${temp}\n`);
                progress.increment();
              });
            });
            // When finish all data parsing for this file,
            // return a resolve signal for promise
            progress.stop();
            resolve('done');
          }
        });
      }
    });
  });
}

// Get a list of current compressed logs in the S3 bucket
function listAllKeys() {
  try {
    s3.listObjectsV2(bucketParams, async (listErr, listData) => {
      if (listErr) throw new Error(listErr);
      else {
        const contents = listData.Contents;
        arr = arr.concat(contents);

        if (listData.IsTruncated) {
          bucketParams.ContinuationToken = listData.NextContinuationToken;
          try {
            listAllKeys();
          } catch (error) {
            throw new Error(error);
          }
        } else {
        // Sort the logs by last modified date
          console.log('Sorting the objects by dates...');
          arr.sort((a, b) => ((b.LastModified > a.LastModified) ? 1
            : ((a.LastModified > b.LastModified) ? -1 : 0)));

          // Check if the lastest log is added
          console.log('Checking if the latest log is added...');

          // If it is beginning of the day, look at past day's last file
          // to make sure we have complete data
          if (hour === '00' && !lookBack) {
            console.log('Beginning of the day: ');
            let pastDay = day - 1;
            let pastMonth = pastDay === 0 ? month - 1 : month;
            pastMonth = +pastMonth;
            const pastYear = pastMonth === 0 ? year - 1 : year;
            pastMonth = pastMonth === 0 ? 12 : pastMonth;
            pastMonth = pastMonth < 10 ? `0${pastMonth}` : pastMonth;
            pastDay = pastDay === 0 ? lastDay : pastDay;
            pastDay = pastDay < 10 ? `0${pastDay}` : pastDay;

            // Update bucket params and call this function again
            bucketParams.Prefix = `${property.aws.prefix}Firehouse/${pastYear}/${pastMonth}/${pastDay}/`;
            try {
              lookBack = true;
              listAllKeys();
            } catch (error) {
              throw new Error(error);
            }
          } else {
            lastModified = arr[0].LastModified;

            try {
            // Get the record from S3
              completed = await getRecord();

              // Temp variable for counting
              let c = 0;

              // If we have already run this file, stop the script
              if (arr[0].LastModified.toISOString() === completed) {
                console.log('Recent file does not seems to be present. Detail as follow:');
                console.log(`The latest log is: ${arr[0].LastModified.toISOString()}`);
                throw new Error('Current file already parsed');
              } else {
                // Look through all S3 item keys
                for (let i = 0; i < arr.length; i += 1) {
                  // If current file already been parsed, get the previous one
                  // (remember all file has been sorted)
                  if (arr[i].LastModified.toISOString() === completed) {
                    c = i - 1;
                    break;
                  }
                  c = i;
                }
                const currentGz = [];

                console.log('Appending all recent file key to array for looping...');

                for (let co = c; co >= 0; co -= 1) {
                  currentGz.push(arr[co].Key);
                }

                console.log(currentGz);

                const itemLastModified = new Date(arr[c].LastModified);
                console.log(`Item last modified: ${itemLastModified.toISOString()}`);
                console.log(`Processing files on this day: ${date.toISOString()}`);

                arr = [];

                // Create a promise array to hold all promises
                for (let h = 0; h < currentGz.length; h += 1) {
                  await parse(currentGz[h]);
                }

                // When all file has been called and all data has been parsed and
                // Close the writing stream to file so all data is settled
                stream
                  .on('error', (err) => {
                    console.log(err);
                    throw new Error(err);
                  })
                  .end(() => {
                    console.log('Uploading to s3');

                    s3multipartUpload(fileName,
                      property.aws.toBucketName, property.aws.jsonPutKeyFolder, (cb) => {
                        console.log(cb);

                        // Remove the temporary file
                        try {
                          fs.unlinkSync(filePath);
                          console.log('File removed');
                        } catch (fileErr) {
                          throw new Error(fileErr);
                        }

                        // Run Redshift query
                        console.log('Running Redshift query...');
                        // const copyCmd = `COPY cwl_mediatailor_ad_decision_server_interactions from \'s3://${property.aws.toBucketName}/${property.aws.jsonPutKeyFolder}mediaTailorData-${month}${day}${year}-${hour}${minute}.json\' credentials \'aws_access_key_id=${property.aws.aws_access_key_id};aws_secret_access_key=${property.aws.aws_secret_access_key}\' json \'auto\' timeformat \'auto\' REGION AS \'eu-central-1\';`;
                        const copyCmd = `COPY cwl_mediatailor_ad_decision_server_interactions from \'s3://${property.aws.toBucketName}/${property.aws.jsonPutKeyFolder}/mediaTailorData-${month}${day}${year}-${hour}${minute}.json\' iam_role \'arn:aws:iam::077497804067:role/RedshiftS3Role\' json \'auto\' timeformat \'auto\' REGION AS \'eu-central-1\';`;
                        redshiftClient2.connect((connectErr) => {
                          if (connectErr) throw new Error(connectErr);
                          else {
                            console.log('Connected to Redshift!');
                            redshiftClient2.query(copyCmd, (queryErr, migrateData) => {
                              if (queryErr) throw new Error(queryErr);
                              else {
                                console.log(migrateData);
                                console.log('Migrated to Redshift');

                                // log the latest file date in S3 to prevent duplicate run
                                s3.putObject({
                                  Bucket: property.aws.toBucketName,
                                  Body: lastModified.toISOString(),
                                  Key: 'prd_completedRecord.txt',
                                }, (uploadErr, uploadData) => {
                                  if (uploadErr) throw new Error(uploadErr);
                                  else {
                                    console.log('Record Saved!');
                                    console.log(uploadData);

                                    date = new Date();

                                    date = new Date(Date.UTC(date.getUTCFullYear(), date.getUTCMonth(),
                                      date.getUTCDate(), date.getUTCHours() - 1));

                                    let aggregatedCompleted = '';

                                    s3.getObject({
                                      Bucket: property.aws.toBucketName,
                                      Key: 'prd_aggregatedCompletedRecord.txt',
                                    }, (err, data) => {
                                      if (err) throw err;
                                      else aggregatedCompleted = data.Body.toString();


                                      let lastCompleteDate = new Date(Date.parse(aggregatedCompleted));

                                      if (minute < 30 || date < lastCompleteDate) {
                                        redshiftClient2.close();
                                      } else {
                                        if (date > lastCompleteDate) {
                                          date = new Date(Date.UTC(lastCompleteDate.getUTCFullYear(), lastCompleteDate.getUTCMonth(),
                                            lastCompleteDate.getUTCDate(), lastCompleteDate.getUTCHours()));
                                        }

                                        let month1 = date.getUTCMonth() + 1;
                                        const year1 = date.getUTCFullYear();
                                        let hour1 = date.getUTCHours();
                                        let day1 = date.getUTCDate();
                                        month1 = month1 < 10 ? `0${month1}` : month1;
                                        day1 = day1 < 10 ? `0${day1}` : day1;
                                        hour1 = hour1 < 10 ? `0${hour1}` : hour1;

                                        lastCompleteDate = new Date(Date.UTC(year, month - 1, day, hour));

                                        const timeRange = `${year}-${month}-${day} ${hour}:00:00`;
                                        const timeRange1 = `${year1}-${month1}-${day1} ${hour1}:00:00`;
                                        const timeRangeDay = `${year1}-${month1}-${day1} 00:00:00`;
                                        const timeRangeMonth = `${year1}-${month1}-01 00:00:00`;

                                        const aggregatedCmd = {
                                          insert: {
                                            cwl_mediatailor_impression_beacon_cmd: `INSERT INTO cwl_mediatailor_impression_beacon ( SELECT DATE_TRUNC(\'minutes\', request_time) as timestamps, beacon_info_tracking_event, CASE WHEN CONCAT(CONCAT(REVERSE(SPLIT_PART(REVERSE(SPLIT_PART(SPLIT_PART(beacon_info_beacon_uri, \'/\', 3), \'?\', 1)), \'.\', 2)), \'.\') ,REVERSE(SPLIT_PART(REVERSE(SPLIT_PART(SPLIT_PART(beacon_info_beacon_uri, \'/\', 3), \'?\', 1)), \'.\', 1)))=\'.\' THEN NULL ELSE CONCAT(CONCAT(REVERSE(SPLIT_PART(REVERSE(SPLIT_PART(SPLIT_PART(beacon_info_beacon_uri, \'/\', 3), \'?\', 1)), \'.\', 2)), \'.\') ,REVERSE(SPLIT_PART(REVERSE(SPLIT_PART(SPLIT_PART(beacon_info_beacon_uri, \'/\', 3), \'?\', 1)), \'.\', 1))) END as uri_source, COUNT(beacon_info_tracking_event) as beacon_tracking_event_count, origin_id, event_type, city, country_iso, region FROM cwl_mediatailor_ad_decision_server_interactions WHERE beacon_info_tracking_event=\'impression\' and request_time>=\'${timeRange1}\' and request_time<\'${timeRange}\' GROUP BY timestamps, origin_id, uri_source, event_type, beacon_info_tracking_event, city, country_iso, region ORDER BY timestamps asc);`,
                                            cwl_mediatailor_minute_unique_users_sessions_cmd: `INSERT INTO cwl_mediatailor_minute_unique_users_sessions (SELECT DATE_TRUNC(\'minutes\', request_time) as timestamps, COUNT( DISTINCT session_id) as unique_session, COUNT( DISTINCT beacon_info_headers_1_value) as unique_user, origin_id, city, country_iso, region FROM cwl_mediatailor_ad_decision_server_interactions WHERE request_time>=\'${timeRange1}\' and request_time<\'${timeRange}\' GROUP BY timestamps, origin_id, city, country_iso, region ORDER BY timestamps asc);`,
                                            cwl_mediatailor_hourly_unique_users_sessions_cmd: `INSERT INTO cwl_mediatailor_hourly_unique_users_sessions (SELECT DATE_TRUNC(\'hours\', request_time) as timestamps, COUNT( DISTINCT session_id) as unique_session, COUNT( DISTINCT beacon_info_headers_1_value) as unique_user, origin_id, city, country_iso, region FROM cwl_mediatailor_ad_decision_server_interactions WHERE request_time>=\'${timeRange1}\' and request_time<\'${timeRange}\' GROUP BY timestamps, origin_id, city, country_iso, region ORDER BY timestamps asc);`,
                                            cwl_mediatailor_daily_unique_users_sessions_cmd: `INSERT INTO cwl_mediatailor_daily_unique_users_sessions ( SELECT DATE_TRUNC(\'days\', request_time) as timestamps, COUNT( DISTINCT session_id) as unique_session, COUNT( DISTINCT beacon_info_headers_1_value) as unique_user, origin_id, city, country_iso, region FROM cwl_mediatailor_ad_decision_server_interactions WHERE request_time>=\'${timeRangeDay}\' and request_time<\'${timeRange}\' GROUP BY timestamps, origin_id, city, country_iso, region ORDER BY timestamps asc);`,
                                            cwl_mediatailor_monthly_unique_users_sessions_cmd: `INSERT INTO cwl_mediatailor_monthly_unique_users_sessions (SELECT DATE_TRUNC(\'months\', request_time) as timestamps, COUNT( DISTINCT session_id) as unique_session, COUNT( DISTINCT beacon_info_headers_1_value) as unique_user, origin_id, city, country_iso, region FROM cwl_mediatailor_ad_decision_server_interactions WHERE request_time>=\'${timeRangeMonth}\' and request_time<\'${timeRange}\' GROUP BY timestamps, origin_id, city, country_iso, region ORDER BY timestamps asc);`,
                                            cwl_mediatailor_ad_event_type_cmd: `INSERT INTO cwl_mediatailor_ad_event_type (SELECT DATE_TRUNC(\'minutes\', request_time) as timestamps, origin_id, event_type, COUNT(event_type) as request_count, city, country_iso, region FROM cwl_mediatailor_ad_decision_server_interactions WHERE request_time>=\'${timeRange1}\' and request_time<\'${timeRange}\' GROUP BY timestamps, origin_id, event_type, city, country_iso, region ORDER BY timestamps asc);`,
                                            cwl_mediatailor_error_cmd: `INSERT INTO cwl_mediatailor_error (SELECT DATE_TRUNC(\'minutes\', request_time) as timestamps, count(request_id) as request_count, origin_id, event_type, city, country_iso, region FROM cwl_mediatailor_ad_decision_server_interactions WHERE request_time>=\'${timeRange1}\' and request_time<\'${timeRange}\' GROUP BY timestamps, origin_id, event_type, city, country_iso, region ORDER BY timestamps asc);`,
                                          },
                                          delete: {
                                            cwl_mediatailor_daily_unique_users_sessions_delete_cmd: `DELETE FROM cwl_mediatailor_daily_unique_users_sessions WHERE timestamps>=\'${timeRangeDay}\' and timestamps<\'${timeRange}\';`,
                                            cwl_mediatailor_monthly_unique_users_sessions_delete_cmd: `DELETE FROM cwl_mediatailor_monthly_unique_users_sessions WHERE timestamps>=\'${timeRangeMonth}\' and timestamps<\'${timeRange}\';`,
                                            cwl_mediatailor_ad_decision_server_interactions_delete_cmd: `DELETE FROM cwl_mediatailor_ad_decision_server_interactions WHERE request_time<\'${timeRangeMonth}\';`,
                                          },
                                        };

                                        const insertPromises = [];
                                        const deletePromises = [];

                                        Object.keys(aggregatedCmd.delete).forEach((cmd) => {
                                          deletePromises.push(new Promise((res) => {
                                            redshiftClient2.query(aggregatedCmd.delete[cmd], (qErr, deleteData) => {
                                              if (qErr) throw new Error(qErr);
                                              else {
                                                console.log(deleteData);
                                                res(`Deleted: ${cmd}`);
                                              }
                                            });
                                          }));
                                        });

                                        Object.keys(aggregatedCmd.insert).forEach((cmd) => {
                                          insertPromises.push(new Promise((res) => {
                                            redshiftClient2.query(aggregatedCmd.insert[cmd], (qErr, insertData) => {
                                              if (qErr) throw new Error(qErr);
                                              else {
                                                console.log(insertData);
                                                res(`Inserted: ${cmd}`);
                                              }
                                            });
                                          }));
                                        });

                                        Promise.all(deletePromises)
                                          .then(() => {
                                            Promise.all(insertPromises)
                                              .then(() => {
                                                console.log('All insert completed');

                                                s3.putObject({
                                                  Bucket: property.aws.toBucketName,
                                                  Body: lastCompleteDate.toISOString(),
                                                  Key: 'prd_aggregatedCompletedRecord.txt',
                                                }, (Err, Data) => {
                                                  if (Err) throw new Error(Err);
                                                  else {
                                                    console.log('Aggregated Record Saved!');
                                                    console.log(Data);
                                                  }
                                                });

                                                redshiftClient2.close();
                                              });
                                          });
                                      }
                                    });
                                  }
                                });
                              }
                            });
                          }
                        });
                      });
                  });
              }
            } catch (error) {
              throw new Error(error);
            }
          }
        }
      }
    });
  } catch (error) {
    throw new Error(error);
  }
}

try {
  listAllKeys();
} catch (error) {
  throw new Error(error);
}
