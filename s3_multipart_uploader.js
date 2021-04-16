/* eslint-disable no-param-reassign */
/* eslint-disable no-async-promise-executor */
/* eslint-disable no-loop-func */
require('dotenv').config();

const AWS = require('aws-sdk');
const fs = require('fs');
const splitFile = require('split-file');
// const property = require('./property');

// Config AWS connection
AWS.config.update({});

// Create S3 connection
const s3 = new AWS.S3();

AWS.config.update({ region: 'eu-central-1' });

module.exports = {
  s3multipartUpload: (fileName, uploadBucket, uploadPrefix, cb) => {
  // Initialize variables for mutiparts upload
    console.log(fileName);
    uploadPrefix = uploadPrefix.replace(/\//g, '');
    const fileKey = fileName;
    const startTime = new Date();
    let partNum = 0;
    const partSize = 1024 * 1024 * 50;
    // 50 represent 50 MB, the AWS recommends file size to be used in multipart upload
    const maxUploadTries = 5;
    let multipartMap = {
      Parts: [],
    };
    const nameArr = [];
    const partObj = {};
    let finalPartDone = false;
    let retry = {};

    const multiPartParams = {
      Bucket: uploadBucket,
      Key: `${uploadPrefix}/${fileName}`,
      ContentType: `application/${fileName.split('.')[1]}`,
    };

    console.log(multiPartParams);

    function completeMultipartUpload(doneParams) {
      // S3 call
      return new Promise((resolve) => {
        try {
        // when completed multipart upload
          s3.completeMultipartUpload(doneParams, (err, data) => {
            try {
              if (err) {
                console.log('An error occurred while completing the multipart upload');
                throw new Error(err);
              } else {
                // When upload are complete
                const delta = (new Date() - startTime) / 1000;
                console.log('Completed upload in', delta, 'seconds');
                console.log('Final upload data:', data);
                console.log('Removing temporary file...');

                nameArr.forEach((namePath) => {
                  try {
                    fs.unlinkSync(namePath);
                    console.log(`File removed: ${namePath}`);
                  } catch (fileErr) {
                    throw new Error(fileErr);
                  }
                });

                resolve(data);
              }
            } catch (error) {
              throw new Error(error);
            }
          });
        } catch (err) {
          throw new Error(err);
        }
      });
    }

    // Function to upload parts
    function uploadPart(multipart, partParams, tryNum = 1) {
      return new Promise((resolve) => {
        try {
        // S3 call
          s3.uploadPart(partParams, async (multiErr, mData) => {
            try {
              // If encounter error, retry for 5 times
              if (multiErr) {
                console.log('multiErr, upload part error:', multiErr);
                if (tryNum < maxUploadTries) {
                  console.log(partParams.PartNumber, retry[partParams.PartNumber], tryNum);
                  if (retry[partParams.PartNumber] === tryNum) {
                    console.log('Retrying upload of part: #', `${partParams.PartNumber} for the ${tryNum + 1} times`);
                    retry[partParams.PartNumber] += 1;
                    // call the function again to retry the upload
                    const res = await uploadPart(multipart, partParams, tryNum + 1)
                      .catch((e) => {
                        console.log(`Error: ${e}`);
                        throw new Error(e);
                      });
                    resolve(res);
                  } else {
                    // if already trying, warn user its already retrying
                    console.log('Already retrying upload of part: #', `${partParams.PartNumber} for the ${tryNum + 1} times`);
                  }
                } else {
                  // if retried 5 times and still failed, reject the part
                  throw new Error(`Failed uploading part: #${partParams.PartNumber}`);
                }
              } else {
                // Append to the multipartMap for final assembly in completeMultipartUpload function
                multipartMap.Parts[partParams.PartNumber - 1] = {
                  ETag: mData.ETag,
                  PartNumber: Number(partParams.PartNumber),
                };
                console.log('Completed part', partParams.PartNumber);
                console.log('mData', mData);

                // delete object flag
                delete partObj[partParams.PartNumber];
                // show what part is left to upload
                console.log(Object.keys(partObj).join(', '));
                if (Object.keys(partObj).length !== 0 && !finalPartDone) {
                  resolve('Still have parts to uploads');
                } else if (finalPartDone) {
                  console.log('pass through');
                } else {
                  finalPartDone = true;
                  const doneParams = {
                    Bucket: uploadBucket,
                    Key: `${uploadPrefix}/${fileName}`,
                    MultipartUpload: multipartMap,
                    UploadId: multipart.UploadId,
                  };

                  // When all parts are uploaded
                  console.log('Completing upload...');
                  try {
                    const completedMulti = await completeMultipartUpload(doneParams)
                      .catch((e) => {
                        throw new Error(e);
                      });
                    resolve(completedMulti.Location);
                  } catch (error) {
                    throw new Error(error);
                  }
                }
              }
            } catch (err) {
              throw new Error(err);
            }
          });
        } catch (err) {
          throw new Error(err);
        }
      });
    }

    splitFile.splitFileBySize(`${__dirname}/JSON/${fileName}`, 2147483647)
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
          nameArr.push(name);
        });

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
        multiPartParams.Key = `${uploadPrefix}/${fileName}`;
        s3.createMultipartUpload(multiPartParams, async (mpErr, multipart) => {
          if (mpErr) { throw new Error(mpErr); }
          console.log('Got upload ID', multipart.UploadId);

          let bufferObj = buffers[first];
          bufferObj.buffer = fs.readFileSync(first);

          let promises = [];

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

                // Move to next buffer object
                bufferObj = buffers[bufferObj.next];

                // Else proceed as normal in the same buffer
              } else {
                body = buffer.slice(start, end);
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

                // Move to next buffer object
                bufferObj = buffers[bufferObj.next];

                // Else proceed as normal in the same buffer
              } else {
                body = buffer.slice(start, rangeEnd);
              }
            }

            const partParams = {
              Body: body,
              Bucket: uploadBucket,
              Key: `${uploadPrefix}/${fileName}`,
              PartNumber: String(partNum),
              UploadId: multipart.UploadId,
            };

            // Send a single part
            console.log('Uploading part: #', partParams.PartNumber, ', Range start:', rangeStart);
            partObj[partParams.PartNumber] = '';
            // set retry flag for this particular part to 1
            retry[partParams.PartNumber] = 1;
            promises.push(new Promise(async (resolve) => {
              try {
                const msg = await uploadPart(multipart, partParams)
                  .catch((e) => { throw new Error(e); });
                console.log(msg);
                resolve(msg);
              } catch (error) {
                console.log(`Error: ${error}`);
                throw new Error(error);
              }
            }));
          }

          // once all parts are uploaded with no error
          Promise.all(promises)
            .then((resolves) => {
              // clear all upload flags
              partNum = 0;
              multipartMap = {
                Parts: [],
              };
              finalPartDone = false;
              promises = [];
              retry = {};
              // resolve the promise
              const location = resolves.filter((ele) => ele !== 'Still have parts to uploads');
              cb(`Done at location: ${location[0]}`);
            })
            .catch((promisesErr) => {
              // throw error
              throw new Error(promisesErr);
            });
        });
      })
      .catch((err) => {
        throw new Error(err);
      });
  },
};
