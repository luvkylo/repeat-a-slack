require('dotenv').config();

const AWS = require('aws-sdk');
// let property = require("./property_local");
const zlib = require('zlib');
const Redshift = require('node-redshift');
const fs = require('fs');
const path = require('path');
const splitFile = require('split-file');
const cliProgress = require('cli-progress');
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
  Key: 'completedRecord.txt',
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
const fileName = 'temp.json';
const filePath = path.join(__dirname, 'JSON', fileName);
const fileKey = fileName;
const startTime = new Date();
let partNum = 0;
const partSize = 1024 * 1024 * 50;
// 50 represent 50 MB, the AWS recommand file size to be used in multipart upload
const maxUploadTries = 3;
const multipartMap = {
  Parts: [],
};
let arr = [];
let nameArr = [];
const partObj = {};
let finalPartDone = false;
let lastModified = '';

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

const multiPartParams = {
  Bucket: property.aws.toBucketName,
  Key: `${property.aws.jsonPutKeyFolder}mediaTailorData-${month}${day}${year}-${hour}${minute}.json`,
  ContentType: 'application/json',
};

console.log(`Looking into the bucket prefix: ${bucketParams.Prefix}`);

// Function for completeing multipart upload
function completeMultipartUpload(doneParams) {
  try {
    // S3 call
    s3.completeMultipartUpload(doneParams, (err, data) => {
      if (err) {
        console.log('An error occurred while completing the multipart upload');
        throw new Error(err);
      } else {
      // When upload are complete
        const delta = (new Date() - startTime) / 1000;
        console.log('Completed upload in', delta, 'seconds');
        console.log('Final upload data:', data);
        console.log('Removing temporary file...');

        // Remove the temporary file
        try {
          fs.unlinkSync(filePath);
          console.log('File removed');
        } catch (fileErr) {
          throw new Error(fileErr);
        }

        nameArr.forEach((namePath) => {
          try {
            fs.unlinkSync(namePath);
            console.log(`File removed: ${namePath}`);
          } catch (fileErr) {
            throw new Error(fileErr);
          }
        });

        // Run Redshift query
        console.log('Running Redshift query...');
        // let copyCmd = `COPY cwl_mediatailor_ad_decision_server_interactions from \'s3://${property.aws.toBucketName}/${property.aws.jsonPutKeyFolder}mediaTailorData-${month}${day}${year}-${hour}${minute}.json\' credentials \'aws_access_key_id=${property.aws.aws_access_key_id};aws_secret_access_key=${property.aws.aws_secret_access_key}\' json \'auto\' timeformat \'auto\' REGION AS \'eu-central-1\';`;
        const copyCmd = `COPY cwl_mediatailor_ad_decision_server_interactions from \'s3://${property.aws.toBucketName}/${property.aws.jsonPutKeyFolder}mediaTailorData-${month}${day}${year}-${hour}${minute}.json\' iam_role \'arn:aws:iam::077497804067:role/RedshiftS3Role\' json \'auto\' timeformat \'auto\' REGION AS \'eu-central-1\';`;
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
                  Key: 'completedRecord.txt',
                }, (uploadErr, uploadData) => {
                  if (uploadErr) throw new Error(uploadErr);
                  else {
                    console.log('Record Saved!');
                    console.log(uploadData);
                  }
                });
                redshiftClient2.close();
              }
            });
          }
        });
      }
    });
  } catch (error) {
    throw new Error(error);
  }
}

// Function to upload parts
function uploadPart(multipart, partParams, tryNum = 1) {
  try {
    // S3 call
    s3.uploadPart(partParams, (multiErr, mData) => {
    // If encounter error, retry for 3 times
      if (multiErr) {
        console.log('multiErr, upload part error:', multiErr);
        if (tryNum < maxUploadTries) {
          console.log('Retrying upload of part: #', partParams.PartNumber);
          try {
            uploadPart(multipart, partParams, tryNum + 1);
          } catch (error) {
            throw new Error(error);
          }
        } else {
          throw new Error(`Failed uploading part: #${partParams.PartNumber}`);
        }
        throw new Error(`Retrying part #${partParams.PartNumber} already.`);
      }

      // Append to the multipartMap for final assembly in completeMultipartUpload function
      multipartMap.Parts[partParams.PartNumber - 1] = {
        ETag: mData.ETag,
        PartNumber: Number(partParams.PartNumber),
      };
      console.log('Completed part', partParams.PartNumber);
      console.log('mData', mData);

      delete partObj[partParams.PartNumber];
      if (Object.entries(partObj).length !== 0 && !finalPartDone) return;
      // complete only when all parts uploaded

      if (!finalPartDone) {
        finalPartDone = true;
        const doneParams = {
          Bucket: property.aws.toBucketName,
          Key: `${property.aws.jsonPutKeyFolder}mediaTailorData-${month}${day}${year}-${hour}${minute}.json`,
          MultipartUpload: multipartMap,
          UploadId: multipart.UploadId,
        };

        // When all parts are uploaded
        console.log('Completing upload...');
        try {
          completeMultipartUpload(doneParams);
        } catch (error) {
          throw new Error(error);
        }
      }
    });
  } catch (error) {
    throw new Error(error);
  }
}

console.log('Getting all objects in S3 bucket...');

function getRecord() {
  return new Promise((resolve, reject) => {
    s3.getObject({
      Bucket: property.aws.toBucketName,
      Key: 'completedRecord.txt',
    }, (err, data) => {
      if (err) reject(new Error(err));
      else resolve(data.Body.toString());
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
          if (arr.length === 0) {
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
                }
                const currentGz = [];

                console.log('Appending all recent file key to array for looping...');
                currentGz.push(arr[c].Key);

                const itemLastModified = new Date(arr[c].LastModified);
                console.log(`Item last modified: ${itemLastModified.toISOString()}`);
                console.log(`Processing files on this day: ${date.toISOString()}`);

                arr = [];

                const progress = new cliProgress.SingleBar({}, cliProgress.Presets.legacy);

                // Create a promise array to hold all promises
                const promises = [];
                currentGz.forEach((key) => {
                // Add promise to the array
                  promises.push(new Promise((resolve, reject) => {
                    getParams.Key = key;

                    // Get the file using key
                    s3.getObject(getParams, (getErr, getData) => {
                      if (getErr) reject(new Error(getErr));
                      else {
                      // Uncompress the data returned
                        const buffer = new Buffer.alloc(getData.ContentLength, getData.Body, 'base64');
                        zlib.unzip(buffer, (zipErr, zipData) => {
                          if (zipErr) reject(new Error(zipErr));
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

                                let message1; let message2; let message3; let message4;
                                message1 = '';
                                message2 = '';
                                message3 = '';
                                message4 = '';

                                let { message } = log;
                                const jsonObj = JSON.parse(message);

                                if (Buffer.byteLength(message) > 65535) {
                                  const tempBuffer = Buffer.from(message);
                                  message1 = tempBuffer.slice(0, 65535).toString();
                                  message2 = tempBuffer.slice(65536).toString();
                                } else {
                                  message1 = message;
                                }

                                if (Buffer.byteLength(message2) > 65535) {
                                  const tempBuffer = Buffer.from(message2);
                                  message2 = tempBuffer.slice(0, 65535).toString();
                                  message3 = tempBuffer.slice(65536).toString();
                                }

                                if (Buffer.byteLength(message3) > 65535) {
                                  const tempBuffer = Buffer.from(message3);
                                  message3 = tempBuffer.slice(0, 65535).toString();
                                  message4 = tempBuffer.slice(65536).toString();
                                }

                                if (Buffer.byteLength(message4) > 65535) {
                                  const tempBuffer = Buffer.from(message4);
                                  message4 = tempBuffer.slice(0, 65535).toString();
                                  message = tempBuffer.slice(65536).toString();
                                  console.log(message4);
                                  console.log(message);
                                }

                                objArr.forEach((ele) => {
                                  if (!jsonObj[ele]) {
                                    jsonObj[ele] = '';
                                  }
                                });

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
                                  beacon_info_headers_0_name: ((jsonObj.beaconInfo && jsonObj.beaconInfo.headers && typeof jsonObj.beaconInfo.headers[0].name !== 'undefined') ? jsonObj.beaconInfo.headers[0].name : ''),
                                  beacon_info_headers_0_value: ((jsonObj.beaconInfo && jsonObj.beaconInfo.headers && typeof jsonObj.beaconInfo.headers[0].value !== 'undefined') ? jsonObj.beaconInfo.headers[0].value : ''),
                                  beacon_info_headers_1_name: ((jsonObj.beaconInfo && jsonObj.beaconInfo.headers && typeof jsonObj.beaconInfo.headers[1].name !== 'undefined') ? jsonObj.beaconInfo.headers[1].name : ''),
                                  beacon_info_headers_1_value: ((jsonObj.beaconInfo && jsonObj.beaconInfo.headers && typeof jsonObj.beaconInfo.headers[1].value !== 'undefined') ? jsonObj.beaconInfo.headers[1].value : ''),
                                  beacon_info_tracking_event: ((jsonObj.beaconInfo && typeof jsonObj.beaconInfo.trackingEvent !== 'undefined') ? jsonObj.beaconInfo.trackingEvent : ''),
                                  message1,
                                  message2,
                                  message3,
                                  message4,
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
                            resolve('done');
                          }
                        });
                      }
                    });
                  }));
                });

                // When all file has been called and all data has been parsed and
                // resolve signal for all promises are returned
                Promise.all(promises)
                  .then(() => {
                    progress.stop();
                    // Close the writing stream to file so all data is settled
                    stream.end(() => {
                    // split the JSON file incase it reaches the Buffer limit at 2147483647 bytes
                      splitFile.splitFileBySize(`${__dirname}/JSON/temp.json`, 2147483647)
                        .then((names) => {
                        // Once all data is settled, Read the JSON file and
                        // prepare its data to be split into parts for mulipart uploads
                          console.log('All data is written to temporary JSON file(s)!');

                          const buffers = {};
                          let last = '';
                          const first = names[0];
                          let i = 1;

                          // Once split to different JSON file
                          names.forEach((name) => {
                          // Read each buffer and append to an object with structure:
                          // {
                          //     "Part1 __dirname": {
                          //         buffer: < Buffer>,
                          //         next: "Part2 __dirname",
                          //         num: "Part num, e.g. 1"
                          //     },
                          //     "Part2 __dirname": {
                          //         buffer: < Buffer>,
                          //         next: (if there is another part) "Part3 __dirname",
                          //         num: "Part num, e.g. 1"
                          //     },
                          // }
                            buffers[name] = {
                              num: i,
                            };
                            i += 1;
                            if (last !== '') {
                              const buffObj = buffers[last];
                              buffObj.next = name;
                              buffers[last] = buffObj;
                            }
                            last = name;
                          });
                          nameArr = names;

                          // Get the total length of buffer
                          let length = 0;

                          Object.keys(buffers).forEach((key) => {
                            const stat = fs.statSync(key);
                            const { size } = stat;
                            length += size;
                          });

                          console.log(`Total buffer bytes: ${length}`);

                          // Calculate the number of parts needed to upload
                          // the whole file with each parts in 100 MB size
                          console.log('Creating multipart upload for:', fileKey);

                          // S3 call to get a multipart upload ID
                          s3.createMultipartUpload(multiPartParams, (mpErr, multipart) => {
                            if (mpErr) { throw new Error('Error!', mpErr); } else {
                              console.log('Got upload ID', multipart.UploadId);

                              let bufferObj = buffers[first];
                              bufferObj.buffer = fs.readFileSync(first);

                              // Grab each partSize chunk and upload it as a part
                              for (let rangeStart = 0; rangeStart < length;
                                rangeStart += partSize) {
                                partNum += 1;
                                const end = Math.min(rangeStart + partSize, length);
                                let start = rangeStart;
                                const { buffer } = bufferObj;
                                let body = '';

                                // Collect bytes from different buffer and append to the body param
                                // If first buffer
                                if (bufferObj.num === 1) {
                                  // If part length exceed buffer current
                                  // buffer length, get the next buffer
                                  if ((bufferObj.num * 2147483647) > rangeStart
                                && (bufferObj.num * 2147483647) < end) {
                                    buffers[bufferObj.next].buffer = fs
                                      .readFileSync(bufferObj.next);

                                    const part1 = buffer.slice(start, 2147483647);
                                    const part2 = buffers[bufferObj.next]
                                      .buffer.slice(0, end - 2147483647);

                                    // Concat the two part and change the body param
                                    body = Buffer.concat([part1, part2]);

                                    console.log(`Part ${bufferObj.num} Byte start: ${start}`);
                                    console.log(`Part ${bufferObj.num} Byte end: ${2147483647}`);
                                    console.log(`Part ${buffers[bufferObj.next].num} Byte start: ${0}`);
                                    console.log(`Part ${buffers[bufferObj.next].num} Byte end: ${end - 2147483647}`);
                                    console.log(`Total byte: ${end}`);

                                    // Move to next buffer object
                                    bufferObj = buffers[bufferObj.next];

                                    // Else proceed as normal in the same buffer
                                  } else {
                                    body = buffer.slice(start, end);
                                    console.log(`Part ${bufferObj.num} Byte start: ${start}`);
                                    console.log(`Part ${bufferObj.num} Byte end: ${end}`);
                                    console.log(`Total byte: ${end}`);
                                  }

                                  // Else change the start and end byte
                                  // to accomadate moving to a new buffer
                                } else {
                                  start = rangeStart - (2147483647 * (bufferObj.num - 1));
                                  const rangeEnd = end - (2147483647 * (bufferObj.num - 1));

                                  // If part length exceed buffer current buffer length,
                                  // get the next buffer
                                  if ((bufferObj.num * 2147483647) > rangeStart
                                && (bufferObj.num * 2147483647) < end) {
                                    buffers[bufferObj.next].buffer = fs
                                      .readFileSync(bufferObj.next);

                                    const part1 = buffer.slice(start, 2147483647);
                                    const part2 = buffers[bufferObj.next].buffer
                                      .slice(0, rangeEnd - 2147483647);

                                    // Concat the two part and change the body param
                                    body = Buffer.concat([part1, part2]);

                                    console.log(`Part ${bufferObj.num} Byte start: ${start}`);
                                    console.log(`Part ${bufferObj.num} Byte end: ${2147483647}`);
                                    console.log(`Part ${buffers[bufferObj.next].num} Byte start: ${0}`);
                                    console.log(`Part ${buffers[bufferObj.next].num} Byte end: ${rangeEnd - 2147483647}`);

                                    console.log(`Total byte: ${end}`);

                                    // Move to next buffer object
                                    bufferObj = buffers[bufferObj.next];

                                    // Else proceed as normal in the same buffer
                                  } else {
                                    body = buffer.slice(start, rangeEnd);
                                    console.log(`Part ${bufferObj.num} Byte start: ${start}`);
                                    console.log(`Part ${bufferObj.num} Byte end: ${rangeEnd}`);

                                    console.log(`Total byte: ${end}`);
                                  }
                                }

                                const partParams = {
                                  Body: body,
                                  Bucket: property.aws.toBucketName,
                                  Key: `${property.aws.jsonPutKeyFolder}mediaTailorData-${month}${day}${year}-${hour}${minute}.json`,
                                  PartNumber: String(partNum),
                                  UploadId: multipart.UploadId,
                                };
                                // Send a single part
                                console.log('Uploading part: #', partParams.PartNumber, ', Range start:', rangeStart);
                                partObj[partParams.PartNumber] = '';
                                try {
                                  uploadPart(multipart, partParams);
                                } catch (error) {
                                  throw new Error(error);
                                }
                              }
                            }
                          });
                        })
                        .catch((err) => {
                          throw new Error(err);
                        });
                    })
                      .catch((endErr) => {
                        throw new Error(endErr);
                      });
                  })
                  .catch((promErr) => {
                    throw new Error(promErr);
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
