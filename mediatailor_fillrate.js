require('dotenv').config();

const AWS = require('aws-sdk');
const Redshift = require('node-redshift');
// const property = require('./property_local');
const property = require('./property');

AWS.config.update({ region: 'us-west-2' });

// const sts = new AWS.STS();

const statusParams = {
  logGroupName: 'MediaTailor/AdDecisionServerInteractions',
  status: 'Running',
};

const date = new Date();
const firstDate = new Date(Date.UTC(date.getUTCFullYear(), date.getUTCMonth(),
  date.getUTCDate() - 1, date.getUTCHours(), date.getUTCMinutes()));
const firstMonth = firstDate.getUTCMonth();
const firstYear = firstDate.getUTCFullYear();
const firstDay = firstDate.getUTCDate();

const client = property.redshift;
let x = 0;

// Create Redshift connection
const redshiftClient2 = new Redshift(client, { rawConnection: true });

let insertKPICmd = 'INSERT INTO cwl_mediatailor_fillrate (query_date, origin_id, filled_duration_sum, origin_avail_duration_sum, num_ads_sum) VALUES ';

// Check query status
function statusFunc(queryId, cloudwatchlogs) {
  return new Promise((resolve) => {
    setTimeout(() => {
      // check cloudwatch query status
      cloudwatchlogs.describeQueries(statusParams, async (err, statusData) => {
        if (err) {
          throw new Error(err);
        } else {
          const arr = statusData.queries.filter((q) => (q.queryId === queryId));
          // if status is running, run the function again
          if (arr.length > 0) {
            try {
              const status = await statusFunc(queryId, cloudwatchlogs).catch((e) => {
                throw new Error(e);
              });
              resolve(status);
            } catch (e) {
              throw new Error(e);
            }
          // else, get the query result
          } else {
            const resultParams = {
              queryId,
            };

            cloudwatchlogs.getQueryResults(resultParams, (resultErr, resultData) => {
              if (resultErr) {
                throw new Error(resultErr);
              } else {
                resolve(resultData.results);
              }
            });
          }
        }
      });
    }, 500);
  });
}

// function that starts the query
function query(queryParams, cloudwatchlogs) {
  return new Promise((resolve) => {
    cloudwatchlogs.startQuery(queryParams, async (err, data) => {
      if (err) {
        throw new Error(err);
      } else {
        // wait for query to complete
        console.log('Started Query');
        console.log(data);
        try {
          const status = await statusFunc(data.queryId, cloudwatchlogs).catch((e) => {
            throw new Error(e);
          });
          resolve(status);
        } catch (e) {
          throw new Error(e);
        }
      }
    });
  });
}

function cloudwatch(cloudwatchlogs) {
  return new Promise(async (resolve) => {
    // set query intervals
    console.log(`Doing ${firstYear}-${firstMonth + 1}-${firstDay}`);
    const startDate = Date.UTC(firstYear, firstMonth, firstDay, 0, 0, 0, 0);
    const endDate = Date.UTC(firstYear, firstMonth, firstDay, 23, 59, 59, 999);

    const queryParams = {
      startTime: startDate,
      queryString: 'stats SUM(avail.filledDuration), SUM(avail.originAvailDuration), SUM(avail.numAds) by bin(1m), originId | filter eventType like /FILLED/',
      endTime: endDate,
      limit: 10000,
      logGroupName: 'MediaTailor/AdDecisionServerInteractions',
    };

    // Create Cloudatch connection
    try {
      const queryWait = await query(queryParams, cloudwatchlogs).catch((e) => {
        throw new Error(e);
      });
      resolve(queryWait);
    } catch (e) {
      throw new Error(e);
    }
  });
}

async function main() {
  // the comment below is used in the local enviroment to assume role
  // in order to read the data from cloudwatch

  //   sts.assumeRole({
  //     RoleArn: 'arn:aws:iam::881583556644:role/freq-assumes-cloudwatch-readonly-master-account',
  //     RoleSessionName: 'alvin@frequency.com',
  //     SerialNumber: 'arn:aws:iam::077497804067:mfa/alvin@frequency.com',
  //     TokenCode: '057317',
  //     DurationSeconds: 43200,
  //   }, async (err, data) => {
  //     if (err) {
  //       console.log('Cannot assume role');
  //       console.log(err, err.stack);
  //     } else {
  //       AWS.config.update({
  //         accessKeyId: data.Credentials.AccessKeyId,
  //         secretAccessKey: data.Credentials.SecretAccessKey,
  //         sessionToken: data.Credentials.SessionToken,
  //       });
  //       console.log('Assumed Role!');
  const cloudwatchlogs = new AWS.CloudWatchLogs();
  try {
    // get cloudwatch query result
    const result = await cloudwatch(cloudwatchlogs).catch((e) => {
      throw new Error(e);
    });
    console.log(result);

    if (result.length > 0) {
      // for each row in the result, extract the data
      result.forEach((row) => {
        let day = ''; let origin_id = ''; let filled_duration_sum = ''; let origin_avail_duration_sum = ''; let
          num_ads_sum = '';
        row.forEach((ele) => {
          if (ele.field === 'bin(1d)') {
            const [d, time] = ele.value.split(' ');
            // this is dilebrate because eslint has the array deconstructing warning
            day = time;
            day = d.split('-').join('');
          } else if (ele.field === 'originId') {
            origin_id = ele.value;
          } else if (ele.field === 'SUM(avail.filledDuration)') {
            filled_duration_sum = ele.value;
          } else if (ele.field === 'SUM(avail.originAvailDuration)') {
            origin_avail_duration_sum = ele.value;
          } else if (ele.field === 'SUM(avail.numAds)') {
            num_ads_sum = ele.value;
          }
        });
        // append it into the redshift query string
        const tempArr = `('${day}','${origin_id}',${filled_duration_sum},${origin_avail_duration_sum},${num_ads_sum})`;
        if (x < result.length - 1) { insertKPICmd += `${tempArr},`; } else { insertKPICmd += `${tempArr};`; }
        x += 1;
      });

      // after parsing through each row, connect to redshift to run the query
      redshiftClient2.connect((connectErr) => {
        if (connectErr) throw connectErr;
        else {
          console.log('Connected to Redshift!');
          // running the query
          redshiftClient2.query(insertKPICmd, (queryErr, queryData) => {
            if (queryErr) throw queryErr;
            else {
              console.log(queryData);
              console.log('Done!');
              redshiftClient2.close();
            }
          });
        }
      });
    } else {
      console.log('Cannot find any record');
    }
  } catch (e) {
    console.log(e);
    throw new Error(e);
  }
//     }
//   });
}

main();
