require('dotenv').config();

const AWS = require('aws-sdk');
const cliProgress = require('cli-progress');
const Redshift = require('node-redshift');
const property = require('./property_local');

AWS.config.update({ region: 'us-west-2' });

let cloudwatchlogs = new AWS.CloudWatchLogs();

const statusParams = {
  logGroupName: 'MediaTailor/AdDecisionServerInteractions',
  status: 'Running',
};

let total = 0;
let start = 0;
let stop = 0;

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
  return new Promise((resolve) => {
    setTimeout(() => {
      // check cloudwatch query status
      cloudwatchlogs.describeQueries(statusParams, async (err, statusData) => {
        if (err) console.log(err);
        else {
          const arr = statusData.queries.filter((q) => (q.queryId === queryId));
          // if status is running, run the function again
          if (arr.length > 0) {
            const status = await statusFunc(queryId);
            resolve(status);
          // else, get the query result
          } else {
            const resultParams = {
              queryId,
            };

            cloudwatchlogs.getQueryResults(resultParams, (resultErr, resultData) => {
              if (resultErr) console.log(resultErr, resultErr.stack); // an error occurred
              else if (resultData.statistics.recordsMatched > 10000) {
                console.log(resultData.statistics.recordsMatched);
                resolve('Too much record');
              } else {
                total += resultData.results.length;
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
  return new Promise((resolve) => {
    cloudwatchlogs.startQuery(queryParams, async (err, data) => {
      if (err) console.log(err, err.stack);
      else {
        // wait for query to complete
        const status = await statusFunc(data.queryId);
        resolve(status);
      }
    });
  });
}

function cloudwatch() {
  return new Promise(async (resolve) => {
    // set query intervals
    const startDate = Date.UTC(firstYear, firstMonth, firstDay, 0, 0, 0, 0);
    const endDate = Date.UTC(secondYear, secondMonth, secondDay, 23, 59, 59, 999);

    let progressDate = startDate;

    const queryParams = {
      startTime: startDate,
      queryString: 'fields @timestamp, @message | sort @timestamp desc',
      endTime: endDate,
      limit: 10000,
      logGroupName: 'MediaTailor/AdDecisionServerInteractions',
    };

    // Create Cloudatch connection
    cloudwatchlogs = new AWS.CloudWatchLogs();
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
      let queryWait = await query(queryParams);
      while (queryWait === 'Too much record') {
        // if too much log stream, cut query interval by half
        console.log(queryWait);
        stop = (start + ((start - stop) / 2)) - 1;
        queryParams.endTime = stop;
        queryWait = await query(queryParams);
        progress.increment(((start - stop) / 2) / 60000);
        progressDate += ((start - stop) / 2);
      }

      // increase start time by 60 seconds
      progressDate += 60000;
      progress.increment();
    }
    // stop progress bar
    progress.stop();
    resolve('Done');
  });
}

async function main() {
  await cloudwatch();

  // query redshift records for number of records
  const selectCmd = `SELECT count(*) FROM cwl_mediatailor_ad_decision_server_interactions WHERE event_timestamp BETWEEN \'${secondYear}-${secondMonth}-${secondDay} 00:00:00\' AND \'${firstYear}-${firstMonth}-${firstDay} 23:59:59\';`;
  redshiftClient2.connect((connectErr) => {
    if (connectErr) throw connectErr;
    else {
      console.log('Connected to Redshift!');
      redshiftClient2.query(selectCmd, (queryErr, migrateData) => {
        if (queryErr) throw queryErr;
        else {
          console.log(migrateData.rows[0].count);

          // if number of records matches cloudwatch query record count
          if (+migrateData.rows[0].count === total) {
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
}

main();
