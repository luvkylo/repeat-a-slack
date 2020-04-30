require('dotenv').config();

const AWS = require('aws-sdk');
const cliProgress = require('cli-progress');
const Redshift = require('node-redshift');
// const property = require('./property_local');
const property = require('./property');

AWS.config.update({ region: 'us-west-2' });

// let cloudwatchlogs = new AWS.CloudWatchLogs();
const cloudwatchlogs = new AWS.CloudWatchLogs();

const statusParams = {
  logGroupName: 'MediaTailor/AdDecisionServerInteractions',
  status: 'Running',
};

let total = 0;
let start = 0;
let stop = 0;

// the date below set the date for querying -> from 2 days ago to yesterday
// this script should runs in 2 days interval
// for every 2 days, it checks the data for the past two days
const date = new Date();
const firstDate = new Date(Date.UTC(date.getUTCFullYear(), date.getUTCMonth(),
  date.getUTCDate() - 1, date.getUTCHours(), date.getUTCMinutes()));
const firstMonth = firstDate.getUTCMonth() + 1;
const firstYear = firstDate.getUTCFullYear();
const firstDay = firstDate.getUTCDate();

const secondDate = new Date(Date.UTC(date.getUTCFullYear(), date.getUTCMonth(),
  date.getUTCDate() - 2, date.getUTCHours(), date.getUTCMinutes()));
const secondMonth = secondDate.getUTCMonth() + 1;
const secondYear = secondDate.getUTCFullYear();
const secondDay = secondDate.getUTCDate();

const client = property.redshift;

// Create Redshift connection
const redshiftClient2 = new Redshift(client, { rawConnection: true });

// Check query status
function statusFunc(queryId) {
  return new Promise((resolve, reject) => {
    setTimeout(() => {
      // check cloudwatch query status
      cloudwatchlogs.describeQueries(statusParams, async (err, statusData) => {
        if (err) {
          console.log(err);
          reject(new Error(err));
        } else {
          const arr = statusData.queries.filter((q) => (q.queryId === queryId));
          // if status is running, run the function again
          if (arr.length > 0) {
            try {
              const status = await statusFunc(queryId).catch((e) => {
                console.log(e);
                reject(e);
              });
              resolve(status);
            } catch (e) {
              console.log(e);
              reject(e);
            }
          // else, get the query result
          } else {
            const resultParams = {
              queryId,
            };

            cloudwatchlogs.getQueryResults(resultParams, (resultErr, resultData) => {
              if (resultErr) {
                console.log(resultErr, resultErr.stack); // an error occurred
                reject(new Error(resultErr));
              } else if (resultData.statistics.recordsMatched > 10000) {
                console.log(resultData.statistics.recordsMatched);
                resolve('Too much record');
              } else {
                total += resultData.results.length;
                console.log(resultData.results.length);
                resolve('done');
              }
            });
          }
        }
      });
    }, 500);
  });
}

// function that starts the query
function query(queryParams) {
  return new Promise((resolve, reject) => {
    cloudwatchlogs.startQuery(queryParams, async (err, data) => {
      if (err) {
        console.log(err);
        reject(new Error(err, err.stack));
      } else {
        // wait for query to complete
        const status = await statusFunc(data.queryId).catch((e) => {
          console.log(e);
          reject(e);
        });
        resolve(status);
      }
    });
  });
}

function cloudwatch() {
  return new Promise(async (resolve, reject) => {
    // set query intervals
    const startDate = Date.UTC(secondYear, secondMonth, secondDay, 0, 0, 0, 0);
    const endDate = Date.UTC(firstYear, firstMonth, firstDay, 23, 59, 59, 999);

    let progressDate = startDate;

    const queryParams = {
      startTime: startDate,
      queryString: 'fields @timestamp, @message | sort @timestamp desc',
      endTime: endDate,
      limit: 10000,
      logGroupName: 'MediaTailor/AdDecisionServerInteractions',
    };
    console.log(startDate, endDate);
    console.log(queryParams);

    // Create Cloudatch connection
    try {
      // cloudwatchlogs = new AWS.CloudWatchLogs();
      const str = '[{bar}] {percentage}% | ETA: {eta}s | {value}/{total}';
      const progress = new cliProgress.SingleBar({ format: str });
      progress.start((endDate - startDate + 1) / 60000, 0);

      // while start date is less than end date
      while (progressDate < endDate) {
      // start time increment is more than end date
        if ((progressDate + 60000) >= endDate) {
        // set end time equals to end date
          start = progressDate;
          stop = endDate;
        // else increment end time by (60 seconds - 1 millisecond)
        } else {
          start = progressDate;
          stop = progressDate + 59999;
        }

        queryParams.startTime = start;
        queryParams.endTime = stop;

        // start query
        try {
          let queryWait = await query(queryParams).catch((e) => {
            console.log(e);
            reject(e);
          });
          while (queryWait === 'Too much record') {
          // if too much log stream, cut query interval by half
            console.log(queryWait);
            stop = (start + ((start - stop) / 2)) - 1;
            queryParams.endTime = stop;
            queryWait = await query(queryParams).catch((e) => {
              console.log(e);
              reject(e);
            });
            progress.increment(((start - stop) / 2) / 60000);
            progressDate += ((start - stop) / 2);
          }

          // increase start time by 60 seconds
          progressDate += 60000;
          progress.increment();
        } catch (err) {
          console.log(err);
          reject(new Error(err));
        }
      }
      // stop progress bar
      progress.stop();
      resolve('Done');
    } catch (e) {
      console.log(e);
      reject(e);
    }
  });
}

async function main() {
  try {
    console.log('starting!');
    const cloudquery = await cloudwatch().catch((e) => {
      console.log(e);
      throw new Error(e);
    });
    console.log(`cloudwatch query: ${cloudquery}`);
    // query redshift records for number of records
    const selectCmd = `SELECT count(*) FROM cwl_mediatailor_ad_decision_server_interactions WHERE event_timestamp BETWEEN \'${secondYear}-${secondMonth}-${secondDay} 00:00:00\' AND \'${firstYear}-${firstMonth}-${firstDay} 23:59:59\';`;
    redshiftClient2.connect((connectErr) => {
      if (connectErr) {
        console.log(connectErr);
        throw new Error(connectErr);
      } else {
        console.log('Connected to Redshift!');
        redshiftClient2.query(selectCmd, (queryErr, migrateData) => {
          if (queryErr) {
            console.log(queryErr);
            throw new Error(queryErr);
          } else {
            console.log(migrateData.rows[0].count, total);

            // if number of records matches cloudwatch query record count
            if (+migrateData.rows[0].count <= total + 100
              && +migrateData.rows[0].count >= total - 100) {
            // record validated
              console.log('Record match!');
            } else {
            // else throw error
              throw new Error('Record does not match!');
            }
            redshiftClient2.close();
          }
        });
      }
    });
  } catch (err) {
    console.log(err);
    throw new Error(err);
  }
}

try {
  main();
} catch (error) {
  console.log(error);
  throw new Error(error);
}
