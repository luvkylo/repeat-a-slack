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
    const fileKey = fileName;
    const startTime = new Date();
    let partNum = 0;
    const partSize = 1024 * 1024 * 50;
    // 50 represent 50 MB, the AWS recommends file size to be used in multipart upload
    const maxUploadTries = 3;
    const multipartMap = {
      Parts: [],
    };
    let nameArr = [];
    const partObj = {};
    let finalPartDone = false;

    const multiPartParams = {
      Bucket: uploadBucket,
      Key: `${uploadPrefix}/${fileName}`,
      ContentType: 'application/csv',
    };

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

            nameArr.forEach((namePath) => {
              try {
                fs.unlinkSync(namePath);
                console.log(`File removed: ${namePath}`);
              } catch (fileErr) {
                throw new Error(fileErr);
              }
            });

            cb('upload complete');
          }
        });
      } catch (error) {
        throw new Error(error);
      }
    }

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
              Bucket: uploadBucket,
              Key: `${uploadPrefix}/${fileName}`,
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

        console.log(`Multipart Params: ${multiPartParams}`);

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
                Bucket: uploadBucket,
                Key: `${uploadPrefix}/${fileName}`,
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
        console.log(err);
        throw new Error('Error: ', err);
      });
  },
};
