require('dotenv').config();

const AWS = require('aws-sdk');
const Redshift = require('node-redshift');
const fs = require('fs');
const path = require('path');
const splitFile = require('split-file');
const cliProgress = require('cli-progress');
const readLine = require('readline');
const countLinesInFile = require('count-lines-in-file');
const changeCase = require('change-case');
const mergeFiles = require('merge-files');
// const property = require('./property_local');
const property = require('./property');
const tiObj = require('./LGI_field.js');

// Config AWS connection
AWS.config.update({});

// Create S3 connection
const s3 = new AWS.S3();

AWS.config.update({ region: 'eu-central-1' });

const client = property.redshift;

// Create Redshift connection
const redshiftClient2 = new Redshift(client, { rawConnection: true });

// Get date for file name
let date = new Date();

date = new Date(Date.UTC(date.getUTCFullYear(), date.getUTCMonth(),
  date.getUTCDate() - 1));

const month = date.getUTCMonth() + 1;
const year = date.getUTCFullYear();
const day = date.getUTCDate();

const strMonth = month < 10 ? `0${month}` : month;
const strDay = day < 10 ? `0${day}` : day;

let count = 0;
const ti = ['"events"', '"channels"', '"contents"', '"credits"', '"genres"', '"pictures"', '"products"', '"series"', '"titles"'];
const startTime = new Date();
let partNum = 0;
const partSize = 1024 * 1024 * 50;
// 50 represent 50 MB, the AWS recommends file size to be used in multipart upload
const maxUploadTries = 5;
let multipartMap = {
  Parts: [],
};
let arr = [];
const nameArr = [];
const partObj = {};
let finalPartDone = false;
const regionObj = {};
const currentGz = [];
let retry = {};
const seriesCheckArr = ['production_date', 'year_of_production_start', 'season_number', 'year_of_production_end', 'number_of_episodes'];
const titlesCheckArr = ['episode_number', 'minimum_age', 'duration_in_seconds', 'duration_in_seconds_ref', 'production_date', 'streaming_popularity_week', 'streaming_popularity_month', 'streaming_popularity_day'];
let errorFlag = false;

// const getParams = {
//   Bucket: property.aws.tvaugFromBucketName,
//   Key: `${property.aws.tvaugPrefix}2020_3_9_ch`,
// };

// Params for calls
const bucketParams = {
  Bucket: property.aws.tvaugFromBucketName,
  Delimiter: '',
  Prefix: `${property.aws.tvaugPrefix}${year}_${month}_${day}_`,
};

const getParams = {
  Bucket: property.aws.tvaugFromBucketName,
};

const multiPartParams = {
  Bucket: property.aws.toBucketName,
  ContentType: 'application/json',
};

console.log(`Looking into the bucket prefix: ${bucketParams.Prefix}`);

const errWrite = fs.createWriteStream(path.join(__dirname, 'JSON', 'error.json'), { flags: 'w' });

const isEmpty = (value) => typeof value === 'undefined' || value === null || value === false;
const isNumeric = (value) => !isEmpty(value) && !Number.isNaN(Number(value));

// Function for completing multipart upload
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
function uploadPart(multipart, partParams, file, tryNum = 1) {
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
                const res = await uploadPart(multipart, partParams, file, tryNum + 1)
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
            } else {
              finalPartDone = true;
              const doneParams = {
                Bucket: property.aws.toBucketName,
                Key: `${property.aws.tvaugJsonPutKeyFolder}${year}_${month}_${day}_${file}.json`,
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

console.log('Getting all objects in S3 bucket...');

// Function to write to files
function writeToFile(region, key) {
  return new Promise((resolve) => {
    try {
      getParams.Key = key;
      const writeObj = {};
      const fileStream = fs.createWriteStream(path.join(__dirname, 'JSON', `temp_${region}.json`));

      console.log(`Getting file: ${key}`);

      // create a read stream to the file because file might be too big to read in buffer
      const s3Stream = s3.getObject(getParams).createReadStream();

      // Listen for errors returned by the service
      s3Stream.on('error', (err) => {
        // NoSuchKey: The specified key does not exist
        throw new Error(err);
      });

      // stream write to file locally
      s3Stream.pipe(fileStream).on('error', (err) => {
        // capture any errors that occur when writing data to the file
        throw new Error(`File Stream:${err}`);
      }).on('close', () => {
        console.log(`Got the file ${key}, getting number of lines.`);

        let n = 0;

        // count the line in files to get an accurate cli progress bar
        countLinesInFile(path.join(__dirname, 'JSON', `temp_${region}.json`), (err, num) => {
          if (err) throw new Error(err);
          else {
            nameArr.push(path.join(__dirname, 'JSON', `temp_${region}.json`));
            console.log(`Got the number of lines: ${num}`);
            n = num;

            // create a read stream to local file
            const jsonStream = fs.createReadStream(path.join(__dirname, 'JSON', `temp_${region}.json`), 'utf-8');

            const readline = readLine.createInterface({
              input: jsonStream,
            });

            let objLine = '';
            const str = `temp_${region}.json [{bar}] {percentage}% | ETA: {eta}s | {value}/{total}`;
            const bar1 = new cliProgress.SingleBar({ format: str });
            bar1.start(n, 0);
            let passedZero = false;
            let countGate = 0;
            let prevLine = ',';
            let lineArr = [];
            let errorCount = 0;

            // read file line
            const rl = readline
              .on('line', async (line) => {
                if (!errorFlag) {
                  let tline = line.trim();

                  // if line has JSON level 1 field name
                  if (tline.includes(ti[countGate]) && prevLine === ',') {
                    // make a new file with that field name
                    if (count < ti.length - 1 && passedZero) {
                      count += 1;
                    }
                    if (count === 0) {
                      passedZero = true;
                      writeObj[ti[count]] = {};
                      writeObj[ti[count]].write = fs.createWriteStream(path.join(__dirname, 'JSON', `${ti[count].replace(/"/g, '')}_${region}.json`), { flags: 'w' });
                    } else {
                      writeObj[ti[count]] = {};
                      writeObj[ti[count - 1]].write.end();
                      writeObj[ti[count]].write = fs.createWriteStream(path.join(__dirname, 'JSON', `${ti[count].replace(/"/g, '')}_${region}.json`), { flags: 'w' });
                    }
                    objLine = null;
                    if (countGate < ti.length - 1) {
                      countGate += 1;
                    }
                  } else {
                    // else do error checking and transformation to make sure
                    // the data is clean enough to be parsed
                    const match = tline.match(/""[A-Za-z0-9àâçéèêëîïôûùüÿñæœ]|[A-Za-z0-9àâçéèêëîïôûùüÿñæœ]""/g);
                    if (match) {
                      match.forEach((m) => {
                        const reg = new RegExp(m, 'g');
                        tline = tline.replace(reg, m.replace(/\"\"/g, '"'));
                      });
                    }
                    const l = tline;
                    tline = tline.trim();

                    // append each line to string for object transformation
                    objLine += tline.replace('{{', '{').replace('yy', 'y').replace('II', 'I').replace('[[', '[')
                      .replace(']]', ']')
                      .replace('}}', '}')
                      .replace('::', ':')
                      .replace(/(\\)+"/g, '\\\"')
                      .replace('."",', '.",');
                    lineArr.push(l);

                    // if string gets too long, it signal an error has been occurred
                    // warn the user but will not stop program from continuing
                    if (objLine.length > 1000000 && tline.replace('{{', '{').replace(',,', ',') === ', {') {
                      console.log('Line got too big, something went wrong!');
                      objLine = tline.replace('{{', '{').replace('yy', 'y').replace('II', 'I').replace('[[', '[')
                        .replace(']]', ']')
                        .replace('}}', '}')
                        .replace('::', ':')
                        .replace(/(\\)+"/g, '\\\"')
                        .replace('."",', '.",');
                    }

                    // if string is an object
                    if (objLine.match(/}/g) && objLine.match(/{/g) && objLine.match(/{/g).length === objLine.match(/}/g).length) {
                      // more data cleaning
                      let jsonStr = objLine.trim().replace('null', '').replace('faalse', 'false').replace('::', ':');
                      jsonStr = jsonStr.replace(',,', ',').replace('": 00,', '": 0,');
                      jsonStr = jsonStr.replace('ffalse', 'false').replace('fallse', 'false').replace('falsse', 'false').replace('falsee', 'false');
                      jsonStr = jsonStr.replace('ttrue', 'true').replace('trrue', 'true').replace('truue', 'true').replace('truee', 'true')
                        .replace('dellasfalto\\', 'dellasfalto');
                      jsonStr = jsonStr.substring(jsonStr.indexOf('{'));
                      let parse = '';
                      let temp = '';

                      // try parsing the data
                      try {
                        parse = JSON.parse(jsonStr);
                        parse.region = region;

                        // data cleaning to make sure field name matches
                        // redshift column
                        Object.keys(parse).forEach((ele) => {
                          parse[ele] = JSON.stringify(parse[ele]);
                          const tempStr = parse[ele];
                          delete parse[ele];
                          parse[changeCase.snakeCase(ele)] = tempStr.replace(/(^")|("$)/g, '');
                          if (!tiObj[ti[count].replace(/"/g, '')].includes(changeCase.snakeCase(ele))) {
                            for (let pos = 1; pos <= ele.length; pos += 1) {
                              const tempEle = ele.slice(0, pos - 1) + ele.slice(pos);
                              if (tiObj[ti[count].replace(/"/g, '')].includes(changeCase.snakeCase(tempEle))) {
                                parse[changeCase.snakeCase(tempEle)] = parse[changeCase
                                  .snakeCase(ele)];
                                break;
                              }
                              // if (pos === ele.length) {
                              //   errWrite.write(`${ele}\n\n`);
                              //   console.log(`\n${ele}`);
                              // }
                            }
                            delete parse[changeCase.snakeCase(ele)];
                          }
                        });

                        // more data cleaning to make sure value is redshift compliant
                        tiObj[ti[count].replace(/"/g, '')].forEach((ele) => {
                          if (!parse[ele]) {
                            parse[ele] = '';
                          }

                          if ((ti[count] === '"genres"' && ele === 'term_id')
                      || (ti[count] === '"products"' && ele === 'list_price')
                      || (ti[count] === '"titles"' && ele === 'list_price')) {
                            parse[ele] = parse[ele].replace(/\"/g, '');
                            parse[ele] = isNumeric(parse[ele]) ? parse[ele] : '';
                          }
                        });

                        if ((ti[count] === '"series"')) {
                          seriesCheckArr.forEach((k) => {
                            if (parse[k] === '') {
                              parse[k] = -999;
                            } else {
                              parse[k] = parse[k].replace(/\"/g, '');
                            }
                          });
                        }

                        if ((ti[count] === '"titles"')) {
                          titlesCheckArr.forEach((k) => {
                            if (parse[k] === '') {
                              parse[k] = -999;
                            } else {
                              parse[k] = parse[k].replace(/\"/g, '');
                            }
                          });
                        }

                        parse.ingest_time = `${strMonth}/${strDay}/${year}`;

                        // write into JSON file for s3 upload
                        temp = `${JSON.stringify(parse, null, 4)}\n`;
                        writeObj[ti[count]].write.write(temp);
                      } catch (error) {
                        errorCount += 1;
                        // no error throwing because this is not a blocker
                        console.log('\n');
                        console.log(jsonStr);
                        console.log(error);
                        errWrite.write(`${l}\n\n`);
                        errWrite.write(`${objLine}\n\n`);
                        errWrite.write(`${jsonStr}\n\n`);
                        errWrite.write(`${lineArr.join('\n')}\n\n`);
                        // however if too much error occurred, throw an error
                        // and stop the process
                        if (errorCount === 300) {
                          errorFlag = true;
                          rl.close();
                          rl.removeAllListeners();
                          jsonStream.destroy();
                          jsonStream.on('close', () => {
                            console.log('Error: Too much error');
                            throw new Error('Too much error');
                          });
                        }
                      }
                      // reset all flags
                      parse = '';
                      temp = '';
                      objLine = '';
                      lineArr = [];
                    }
                  }

                  // variable for error logging
                  if (passedZero) {
                    prevLine = line.trim();
                  }
                }
                bar1.increment();
              })
              .on('close', () => {
              // when complete reading the file
                objLine = null;
                bar1.stop();
                console.log(`Completed file: ${key}`);
                // resolve the promise
                writeObj[ti[count]].write.end(() => {
                  count = 0;
                  resolve('done');
                });
              });
          }
        });
      });
    } catch (err) {
      throw new Error(err);
    }
  });
}

// Functions to upload file to S3
function uploadFile(file) {
  return new Promise((res) => {
    try {
      splitFile.splitFileBySize(`${__dirname}/JSON/${file.replace(/"/g, '')}.json`, 2147483647)
        .then((names) => {
        // Once all data is settled, Read the JSON file and
        // prepare its data to be split into parts for multipart uploads
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

          // Calculate the number of parts needed to upload
          // the whole file with each parts in 100 MB size
          console.log('Creating multipart upload for:', `${file.replace(/"/g, '')}.json`);

          // S3 call to get a multipart upload ID
          multiPartParams.Key = `${property.aws.tvaugJsonPutKeyFolder}${year}_${month}_${day}_${file.replace(/"/g, '')}.json`;
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
              // If part length exceed buffer current buffer length, get the next buffer
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

              // Else change the start and end byte to accommodate moving to a new buffer
              } else {
                start = rangeStart - (2147483647 * (bufferObj.num - 1));
                const rangeEnd = end - (2147483647 * (bufferObj.num - 1));

                // If part length exceed buffer current buffer length,
                // get the next buffer
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
                Bucket: property.aws.toBucketName,
                Key: `${property.aws.tvaugJsonPutKeyFolder}${year}_${month}_${day}_${file.replace(/"/g, '')}.json`,
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
                  const msg = await uploadPart(multipart, partParams, file.replace(/"/g, '')).catch((e) => { throw new Error(e); });
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
                res(`Done with ${file} at location: ${location[0]}`);
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
    } catch (err) {
      throw new Error(err);
    }
  });
}

// main function
async function listAllKeys() {
  try {
    // get listed objects from s3
    s3.listObjectsV2(bucketParams, async (listErr, listData) => {
      if (listErr) throw new Error(listErr);
      else {
        try {
          const contents = listData.Contents;
          arr = arr.concat(contents);

          // if there are more objects
          if (listData.IsTruncated) {
            bucketParams.ContinuationToken = listData.NextContinuationToken;
            await listAllKeys().catch((e) => {
              console.log(`Error: ${e}`);
              throw new Error(e);
            });
          } else if (arr.length === 0) {
          // if cant find any objects
            console.log('No file on this day');
            throw new Error(`No file on this day: ${year}/${month}/${day}`);
          } else {
          // if found all objects
            arr.forEach((ele) => {
              currentGz.push(ele.Key);
              // get the object region
              regionObj[ele.Key] = ele.Key.split('_').slice(-1).pop();
            });
            console.log(currentGz);

            arr = [];

            // Create a promise array to hold all promises
            for (const key of currentGz) {
              if (!errorFlag) {
                const region = regionObj[key];
                // Add promise to the array
                try {
                  await writeToFile(region, key).catch((e) => {
                    console.log(`Error: ${e}`);
                    throw new Error(e);
                  });
                } catch (error) {
                  console.log(`Error: ${error}`);
                  throw new Error(error);
                }
              }
            }

            // When all file has been called and all data has been parsed and
            // resolve signal for all promises are returned
            errWrite.end();
            // for every file/region
            for (const file of ti) {
              console.log(file);
              const inputFileArr = [];
              Object.keys(regionObj).forEach((key) => {
                inputFileArr.push(path.join(__dirname, 'JSON', `${file.replace(/"/g, '')}_${regionObj[key]}.json`));
              });
              const outputFile = `${__dirname}/JSON/${file.replace(/"/g, '')}.json`;
              console.log(inputFileArr);
              // merge first level object JSON with
              // different regions into one first level object JSON
              try {
                await mergeFiles(inputFileArr, outputFile).catch((e) => {
                  console.log(`Error: ${e}`);
                  throw new Error(e);
                });
                inputFileArr.forEach((tempPath) => {
                  try {
                  // remove all region files for this first level object
                    fs.unlinkSync(tempPath);
                    console.log(`removed ${tempPath}`);
                  } catch (fileErr) {
                    console.log(`Error: ${fileErr}`);
                    throw new Error(fileErr);
                  }
                });

                console.log('File merged');

                // upload the first level object JSON to s3
                nameArr.push(outputFile);
                const done = await uploadFile(file).catch((e) => {
                  console.log(`Error: ${e}`);
                  throw new Error(e);
                });
                console.log(done);
              } catch (error) {
                console.log(`Error: ${error}`);
                throw new Error(error);
              }
            }
            // Run Redshift query
            redshiftClient2.connect((connectErr) => {
              if (connectErr) throw new Error(connectErr);
              else {
                console.log('Connected to Redshift!');
                console.log('Running Redshift queries...');

                for (let x = 0; x < ti.length; x += 1) {
                  const table = ti[x].replace(/"/g, '');

                  // const copyCmd = `COPY tv_aug_${table}_metadata from \'s3://${property.aws.toBucketName}/${property.aws.tvaugJsonPutKeyFolder}${year}_${month}_${day}_${table}.json\' credentials \'aws_access_key_id=${property.aws.aws_access_key_id};aws_secret_access_key=${property.aws.aws_secret_access_key}\' json \'auto\' dateformat \'auto\' REGION AS \'eu-central-1\';`;
                  const copyCmd = `COPY tv_aug_${table}_metadata from \'s3://${property.aws.toBucketName}/${property.aws.tvaugJsonPutKeyFolder}${year}_${month}_${day}_${table}.json\' iam_role \'arn:aws:iam::077497804067:role/RedshiftS3Role\' json \'auto\' dateformat \'auto\' REGION AS \'eu-central-1\';`;

                  redshiftClient2.query(copyCmd, (queryErr, migrateData) => {
                    if (queryErr) throw new Error(queryErr);
                    else {
                      console.log(migrateData);
                      console.log(`Migrated to Redshift table tv_aug_${table}_metadata`);
                      if (x === (ti.length - 1)) {
                        redshiftClient2.close();
                        console.log('Removing temporary file...');

                        // Remove the temporary file
                        nameArr.forEach((namePath) => {
                          try {
                            fs.unlinkSync(namePath);
                            console.log(`File removed: ${namePath}`);
                          } catch (fileErr) {
                            console.log(`Error: ${fileErr}`);
                            throw new Error(fileErr);
                          }
                        });
                      }
                    }
                  });
                }
              }
            });
          }
        } catch (err) {
          console.log(`Error: ${err}`);
          throw new Error(err);
        }
      }
    });
  } catch (error) {
    console.log(`Error: ${error}`);
    throw new Error(error);
  }
}

listAllKeys().catch((err) => {
  console.log(`Error: ${err}`);
  throw new Error(err);
});
