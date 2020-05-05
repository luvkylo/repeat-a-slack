require('dotenv').config();

const AWS = require('aws-sdk');
// let property = require("./property_local");
const zlib = require('zlib');
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

// Get date for file name
let date = new Date();

date = new Date(Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), date.getUTCDate(),
  date.getUTCHours(), date.getUTCMinutes()));

const lastMonth = new Date();
lastMonth.setUTCDate(1);
lastMonth.setUTCHours(-1);
const lastDay = lastMonth.getUTCDate();

let month = date.getUTCMonth() + 1;
let year = date.getUTCFullYear();
let hour = date.getUTCHours();
let day = hour >= 12 ? date.getUTCDate() : date.getUTCDate() - 1;
month = day === 0 ? month - 1 : month;
year = month === 0 ? year - 1 : year;
month = month === 0 ? 12 : month;
month = month < 10 ? `0${month}` : month;
day = day === 0 ? lastDay : day;
day = day < 10 ? `0${day}` : day;

console.log(`Processing file on this day: ${date.toISOString()}`);

// Initialize variables for mutiparts upload
const fileName = 'temp.csv';
const filePath = path.join(__dirname, 'JSON', fileName);
const fileKey = fileName;
const startTime = new Date();
let partNum = 0;
const partSize = 1024 * 1024 * 50;
// 50 represent 50 MB, the AWS recommends file size to be used in multipart upload
const maxUploadTries = 3;
const multipartMap = {
  Parts: [],
};
let arr = [];
let nameArr = [];
const partObj = {};
let finalPartDone = false;

// Create and open stream to write to temporary CSV file
const stream = fs.createWriteStream(filePath, { flags: 'w' });
stream.write('request_time,aws_account_id,customer_id,event_description,event_timestamp,event_type,origin_id,request_id,session_id,session_type,beacon_info_beacon_http_response_code,beacon_info_beacon_uri,beacon_info_headers_0_name,beacon_info_headers_0_value,beacon_info_headers_1_name,beacon_info_headers_1_value,beacon_info_tracking_event,vast_ad_ids,ad_systems,ad_titles,creative_ids,creative_ad_ids\n');

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
  ContentType: 'application/csv',
};

console.log(`Looking into the bucket prefix: ${bucketParams.Prefix}`);

// Function for completing multipart upload
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
          Key: `${property.aws.csvPutKeyFolder}mediaTailorData-${month}${day}${year}-${hour}.csv`,
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

// Get a list of current compressed logs in the S3 bucket
function listAllKeys() {
  try {
    s3.listObjectsV2(bucketParams, (listErr, listData) => {
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
          const lastModified = new Date(arr[0].LastModified);
          lastModified.setSeconds(0);

          const now = new Date();
          const current = new Date(Date.UTC(now.getUTCFullYear(),
            now.getUTCMonth(), now.getUTCDate()));

          let currentGz = [];

          // Append all recent file keys to the array for looping
          console.log('Appending all recent file key to array for looping...');
          arr.forEach((item) => {
            const itemKeyArr = item.Key.split('/');
            if (hour >= 12) {
              if (itemKeyArr[5] <= 12) {
                currentGz.push(item.Key);
              }
            } else if (itemKeyArr[5] > 12) {
              currentGz.push(item.Key);
            }
          });

          hour = hour >= 12 ? '12' : '00';

          multiPartParams.Key = `${property.aws.csvPutKeyFolder}mediaTailorData-${month}${day}${year}-${hour}.csv`;
          console.log(`${property.aws.csvPutKeyFolder}mediaTailorData-${month}${day}${year}-${hour}.csv`);

          arr = [];

          // If lastest log is not added, array will be empty
          if (currentGz.length === 0) {
            console.log('Recent file does not seems to be present. Detail as follow:');
            console.log(`The latest log is: ${lastModified}`);
            console.log(`The day is missing: ${current}`);

          // Else, for each file key in array
          } else {
            console.log(`Found ${currentGz.length} files!`);
            console.log(`Processing files on this day: ${date.toISOString()}`);

            const progress = new cliProgress.SingleBar({}, cliProgress.Presets.legacy);
            progress.start(currentGz.length);

            // Create a promise array to hold all promises
            const promises = [];
            currentGz = currentGz.sort();
            currentGz.forEach((key) => {
            // Add promise to the array
              promises.push(new Promise((resolve) => {
                getParams.Key = key;

                // Get the file using key
                s3.getObject(getParams, (getErr, getData) => {
                  if (getErr) { throw new Error(getErr); } else {
                  // Uncompress the data returned
                    const buffer = new Buffer.alloc(getData.ContentLength, getData.Body, 'base64');
                    zlib.unzip(buffer, (zipErr, zipData) => {
                      if (zipErr) { throw new Error(zipErr); } else {
                      // Split each data into array when new line
                        let logData = zipData.toString().split('{"messageType"');

                        // Create variables for CSV
                        const objArr = ['awsAccountId', 'customerId', 'eventDescription', 'eventType', 'originId', 'requestId', 'sessionId', 'sessionType', 'beaconInfo'];

                        logData = logData.filter((el) => el !== '');

                        // For every row of data
                        logData.forEach((logsItem) => {
                          const logs = `{"messageType"${logsItem}`;
                          const logObj = JSON.parse(logs);

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

                            // Create an object to be transform into CSV
                            const request_time = time;
                            const aws_account_id = jsonObj.awsAccountId.trim();
                            const customer_id = jsonObj.customerId.trim();
                            const event_description = `"${jsonObj.eventDescription.trim().replace(/\"/g, "'")}"`;
                            const event_timestamp = (jsonObj.eventTimestamp.length === 20) ? `${jsonObj.eventTimestamp.slice(0, 19)}.000${jsonObj.eventTimestamp.slice(19)}` : jsonObj.eventTimestamp;
                            const event_type = jsonObj.eventType.trim();
                            const origin_id = jsonObj.originId.trim();
                            const request_id = jsonObj.requestId.trim();
                            const session_id = jsonObj.sessionId.trim();
                            const session_type = jsonObj.sessionType.trim();
                            const beacon_info_beacon_http_response_code = ((jsonObj.beaconInfo && typeof jsonObj.beaconInfo.beaconHttpResponseCode !== 'undefined') ? jsonObj.beaconInfo.beaconHttpResponseCode : '');
                            const beacon_info_beacon_uri = ((jsonObj.beaconInfo && typeof jsonObj.beaconInfo.beaconUri !== 'undefined') ? `"${jsonObj.beaconInfo.beaconUri.trim().replace(/\"/g, "'")}"` : '');
                            let beacon_info_headers_0_name = '';
                            let beacon_info_headers_0_value = '';
                            let beacon_info_headers_1_name = '';
                            let beacon_info_headers_1_value = '';
                            if (jsonObj.beaconInfo && jsonObj.beaconInfo.headers) {
                              jsonObj.beaconInfo.headers.forEach((header) => {
                                if (header.name) {
                                  if (header.name === 'User-Agent') {
                                    beacon_info_headers_0_name = `"${header.name.trim().replace(/\"/g, "'")}"`;
                                    beacon_info_headers_0_value = `"${header.value.trim().replace(/\"/g, "'")}"`;
                                  } else if (header.name === 'X-Forwarded-For') {
                                    beacon_info_headers_1_name = `"${header.name.trim().replace(/\"/g, "'")}"`;
                                    beacon_info_headers_1_value = `"${header.value.trim().replace(/\"/g, "'")}"`;
                                  }
                                }
                              });
                            }

                            const beacon_info_tracking_event = ((jsonObj.beaconInfo && typeof jsonObj.beaconInfo.trackingEvent !== 'undefined') ? `"${jsonObj.beaconInfo.trackingEvent.trim().replace(/\"/g, "'")}"` : '');

                            let vast_ad_ids = [];
                            let ad_systems = [];
                            let ad_titles = [];
                            let creative_ids = [];
                            let creative_ad_ids = [];

                            if (jsonObj.vastResponse
                              && Array.isArray(jsonObj.vastResponse.vastAds)) {
                              jsonObj.vastResponse.vastAds.forEach((ad) => {
                                vast_ad_ids.push(ad.vastAdId);
                                ad_systems.push(ad.adSystem);
                                ad_titles.push(ad.adTitle);
                                creative_ids.push(ad.creativeId);
                                creative_ad_ids.push(ad.creativeAdId);
                              });

                              vast_ad_ids = `"[${vast_ad_ids.join()}]"`;
                              ad_systems = `"[${ad_systems.join()}]"`;
                              ad_titles = `"[${ad_titles.join()}]"`;
                              creative_ids = `"[${creative_ids.join()}]"`;
                              creative_ad_ids = `"[${creative_ad_ids.join()}]"`;
                            } else if (jsonObj.vastAd) {
                              vast_ad_ids = jsonObj.vastAd.vastAdId;
                              ad_systems = jsonObj.vastAd.adSystem;
                              ad_titles = jsonObj.vastAd.adTitle;
                              creative_ids = jsonObj.vastAd.creativeId;
                              creative_ad_ids = jsonObj.vastAd.creativeAdId;
                            } else {
                              vast_ad_ids = '';
                              ad_systems = '';
                              ad_titles = '';
                              creative_ids = '';
                              creative_ad_ids = '';
                            }

                            // Transform object into CSV string
                            const temp = `${request_time},${aws_account_id},${customer_id},${event_description},${event_timestamp},${event_type},${origin_id},${request_id},${session_id},${session_type},${beacon_info_beacon_http_response_code},${beacon_info_beacon_uri},${beacon_info_headers_0_name},${beacon_info_headers_0_value},${beacon_info_headers_1_name},${beacon_info_headers_1_value},${beacon_info_tracking_event},${vast_ad_ids},${ad_systems},${ad_titles},${creative_ids},${creative_ad_ids}`;

                            // Write the CSV string into the temporary CSV file
                            stream.write(`${temp}\n`);
                          });
                        });
                        progress.increment();
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
                // split the CSV file incase it reaches the Buffer limit at 2147483647 bytes
                  splitFile.splitFileBySize(`${__dirname}/JSON/temp.csv`, 2147483647)
                    .then((names) => {
                    // Once all data is settled, Read the CSV file and prepare its
                    // data to be split into parts for mulipart uploads
                      console.log('All data is written to temporary CSV file(s)!');

                      const buffers = {};
                      let last = '';
                      const first = names[0];
                      let i = 1;

                      // Once split to different CSV file
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

                      // Calculate the number of parts needed to upload the whole
                      // file with each parts in 100 MB size
                      console.log('Creating multipart upload for:', fileKey);

                      // S3 call to get a multipart upload ID
                      s3.createMultipartUpload(multiPartParams, (mpErr, multipart) => {
                        if (mpErr) { throw new Error('Error!', mpErr); } else {
                          console.log('Got upload ID', multipart.UploadId);

                          let bufferObj = buffers[first];
                          bufferObj.buffer = fs.readFileSync(first);

                          // Grab each partSize chunk and upload it as a part
                          for (let rangeStart = 0; rangeStart < length; rangeStart += partSize) {
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
                                buffers[bufferObj.next].buffer = fs.readFileSync(bufferObj.next);

                                const part1 = buffer.slice(start, 2147483647);
                                const part2 = buffers[bufferObj.next]
                                  .buffer.slice(0, end - 2147483647);

                                // Concat the two part and change the body param
                                body = Buffer.concat([part1, part2]);

                                console.log(`Part ${bufferObj.num}Byte start: ${start}`);
                                console.log(`Part ${bufferObj.num}Byte end: ${2147483647}`);
                                console.log(`Part ${buffers[bufferObj.next].num}Byte start: ${0}`);
                                console.log(`Part ${buffers[bufferObj.next].num}Byte end: ${end - 2147483647}`);
                                console.log(`Total byte: ${end}`);

                                // Move to next buffer object
                                bufferObj = buffers[bufferObj.next];

                                // Else proceed as normal in the same buffer
                              } else {
                                body = buffer.slice(start, end);
                                console.log(`Part ${bufferObj.num}Byte start: ${start}`);
                                console.log(`Part ${bufferObj.num}Byte end: ${end}`);
                                console.log(`Total byte: ${end}`);
                              }

                              // Else change the start and end byte
                              // to accomadate moving to a new buffer
                            } else {
                              start = rangeStart - (2147483647 * (bufferObj.num - 1));
                              const rangeEnd = end - (2147483647 * (bufferObj.num - 1));

                              // If part length exceed buffer current
                              // buffer length, get the next buffer
                              if ((bufferObj.num * 2147483647) > rangeStart
                          && (bufferObj.num * 2147483647) < end) {
                                buffers[bufferObj.next].buffer = fs.readFileSync(bufferObj.next);

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
                              Key: `${property.aws.csvPutKeyFolder}mediaTailorData-${month}${day}${year}-${hour}.csv`,
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
                      throw new Error('Error: ', err);
                    });
                });
              })
              .catch((promErr) => {
                throw new Error(promErr);
              });
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
