require('dotenv').config();

const Redshift = require('node-redshift');
const axios = require('axios');
const cliProgress = require('cli-progress');
const fs = require('fs');
const path = require('path');
const csv = require('fast-csv');
const { s3multipartUpload } = require('./s3_multipart_uploader');
// const property = require('./property');
const property = require('./property_local');

const client = property.redshift;

// Create Redshift connection
const redshiftClient2 = new Redshift(client, { rawConnection: true });

// Get date for file name
let date = new Date();

date = new Date(Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), date.getUTCDate()));

const endMonth = date.getUTCMonth() + 1;
const endYear = date.getUTCFullYear();
const endD = date.getUTCDate();

const endStrMonth = endMonth < 10 ? `0${endMonth}` : endMonth;
const endStrDay = endD < 10 ? `0${endD}` : endD;

date = new Date();
date = new Date(Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), date.getUTCDate() - 7));

const startMonth = date.getUTCMonth() + 1;
const startYear = date.getUTCFullYear();
const startD = date.getUTCDate();

const startStrMonth = startMonth < 10 ? `0${startMonth}` : startMonth;
const startStrDay = startD < 10 ? `0${startD}` : startD;

date = new Date();
date = new Date(Date.UTC(date.getUTCFullYear(), date.getUTCMonth(),
  date.getUTCDate()));

const qMonth = date.getUTCMonth() + 1;
const queryYear = date.getUTCFullYear();
const qDay = date.getUTCDate();

const queryMonth = qMonth < 10 ? `0${qMonth}` : qMonth;
const queryDay = qDay < 10 ? `0${qDay}` : qDay;

const region = process.env.REGION;

const queryKPICmd = `select
titles.title_id as crid,
titles.name as title_name,
case when (titles.series_id <> '') THEN 'series' ELSE 'other' END as type,
titles.region as region,
contents.discoverable_as_vod as is_on_demand
from (
  SELECT *
  FROM
    (SELECT title_id, name, episode_number, region, series_id,
            ROW_NUMBER() OVER (PARTITION BY title_id
                               ORDER BY name, episode_number, region) AS title_id_ranked
     FROM tv_aug_titles_metadata
     WHERE ingest_time>='${startStrMonth}-${startStrDay}-${startYear}' and ingest_time<'${endStrMonth}-${endStrDay}-${endYear}' AND region='${region}' AND is_adult=FALSE AND (title_id <> '') IS TRUE
     ORDER BY title_id, name, episode_number, region) AS ranked
  WHERE ranked.title_id_ranked = 1
) as titles
left join (
  SELECT *
  FROM
    (SELECT title_id, discoverable_as_vod,
            ROW_NUMBER() OVER (PARTITION BY title_id
                               ORDER BY discoverable_as_vod) AS title_id_ranked
     FROM tv_aug_contents_metadata
     ORDER BY title_id, discoverable_as_vod) AS ranked
  WHERE ranked.title_id_ranked = 1
) as contents on contents.title_id=titles.title_id
ORDER BY crid DESC;`;

const { device } = property.tvaug[region];
const { token } = property.tvaug[region];

const startDate = `${startStrMonth}/${startStrDay}/${startYear}`;
const endDate = `${endStrMonth}/${endStrDay}/${endYear}`;
const queryDate = `${queryMonth}/${queryDay}/${queryYear}`;

console.log(`Start Date: ${startDate}`);
console.log(`End Date: ${endDate}`);

const csvStream = csv.format({ headers: true });

const fileName = `tvaug_metadata_match_weekly_snapshot_${region}_${queryYear}${queryMonth}${queryDay}.csv`;

const writeStream = fs.createWriteStream(path.join(__dirname, 'JSON', fileName), { flags: 'w' });

csvStream.pipe(writeStream).on('end', () => console.log('done'));

function request(url, headers, obj, bar1, retry = 0) {
  return new Promise((res) => {
    axios.get(url, {
      headers,
      muteHttpExceptions: true,
      validateStatus(status) {
        return [200, 404, 500, 504].indexOf(status) !== -1;
      },
    })
      .then((response) => {
        if (response.status === 504) {
          console.log('Encountered 504');
          let re = retry;
          if (re < 5) {
            setTimeout(async () => {
              const newObj = await request(url, headers, obj, bar1, re += 1);
              console.log('Retry successful');
              res(newObj);
            }, 2000);
          } else {
            console.log('Retry failed');
            throw new Error('Encountered 504 but retry failed after 4 times');
          }
        } else if (response.status === 500) {
          console.log('Encountered 500');
          let re = retry;
          if (re < 5) {
            setTimeout(async () => {
              const newObj = await request(url, headers, obj, bar1, re += 1);
              console.log('Retry successful');
              res(newObj);
            }, 2000);
          } else {
            console.log('Retry failed');
            throw new Error('Encountered 500 but retry failed after 4 times');
          }
        } else if (response.data.message === 'no program exists with for the external identifier provided!') {
          const newObj = obj;
          newObj.number_matched = -1;
          res(newObj);
        } else if (response.data.videos) {
          const newObj = obj;
          newObj.number_matched = response.data.videos.length;
          res(newObj);
        } else {
          console.log('Error: ');
          console.log(response);
          const newObj = obj;
          newObj.number_matched = -1;
          res(newObj);
        }
      })
      .catch((err) => {
        bar1.stop();
        csvStream.end();
        redshiftClient2.close();
        console.log(err);
        throw new Error(err);
      });
  });
}

function query() {
  return new Promise((reso) => {
    // redshift query to get the all unique crids
    redshiftClient2.query(queryKPICmd, async (queryErr, queryData) => {
      if (queryErr) {
        redshiftClient2.close();
        csvStream.end();
        console.log(queryErr);
        return new Error(queryErr);
      }

      redshiftClient2.close();

      console.log(Buffer.byteLength(JSON.stringify(queryData.rows), 'utf8'));
      console.log(`Got region: ${region}`);

      const str = `${region} [{bar}] {percentage}% | ETA: {eta}s | {value}/{total}`;
      const bar1 = new cliProgress.SingleBar({ format: str });
      bar1.start(queryData.rows.length, 0);

      // for each crid, call the prd-lgi-api to get wikidata for that item
      for (let p = 0; p < queryData.rows.length; p += 1) {
        // if (p > 4824) {
        const row = queryData.rows[p];
        const url = `http://prd-lgi-api.frequency.com/api/2.0/programs/videos?video_image=256w144h,solid,rectangle&external_identifier_source=LGI&external_identifier=${row.crid}&page_size=43`;
        const headers = { 'X-Frequency-DeviceId': device, 'X-Frequency-Auth': token };
        const crid = row.crid ? row.crid.replace(/('|")/g, "\\'") : row.crid;
        let title_name = row.title_name ? row.title_name.replace(/('|")/g, "\\'") : row.title_name;
        title_name = title_name === 'null' ? '' : `${title_name}`;
        const temp = {
          query_date: queryDate,
          crid,
          title_name,
          type: row.type,
          region: row.region,
          is_on_demand: row.is_on_demand,
        };

        const newObj = await request(url, headers, temp, bar1).catch((e) => {
          console.log(e);
          throw new Error(e.err);
        });

        csvStream.write(newObj);
        bar1.increment();
        // } else {
        //   bar1.increment();
        // }
      }

      csvStream.end();
      bar1.stop();
      //   redshiftClient2.close();

      reso(`All data written to file: tvaug_metadata_match_weekly_snapshot_${region}_${queryYear}${queryMonth}${queryDay}.csv`);
      return 0;
    });
  });
}

try {
  redshiftClient2.connect(async (connectErr) => {
    if (connectErr) throw new Error(connectErr);
    else {
      console.log('Connected to Redshift!');
      console.log(`\nquerying region: ${region}`);
      try {
        const complete = await query().catch((e) => {
          console.log(e);
          throw new Error(e.err);
        });
        console.log(complete);

        s3multipartUpload(fileName, property.aws.toBucketName,
          property.aws.tvaugWeeklySnapshotFolder, (cb) => {
            console.log(cb);

            const filePath = path.join(__dirname, 'JSON', fileName);

            // Remove the temporary file
            try {
              fs.unlinkSync(filePath);
              console.log('File removed');
            } catch (fileErr) {
              throw new Error(fileErr);
            }

            // Run Redshift query
            console.log('Running Redshift query...');
            const copyCmd = `COPY tv_aug_weekly_match_result from \'s3://${property.aws.toBucketName}/${property.aws.tvaugWeeklySnapshotFolder}/${fileName}\' credentials \'aws_access_key_id=${property.aws.aws_access_key_id};aws_secret_access_key=${property.aws.aws_secret_access_key}\' CSV dateformat AS \'MM/DD/YYYY\' IGNOREHEADER 1 REGION AS \'eu-central-1\';`;
            // const copyCmd = `COPY tv_aug_weekly_match_result from \'s3://${property.aws.toBucketName}/${property.aws.tvaugWeeklySnapshotFolder}/${fileName}\' iam_role \'arn:aws:iam::077497804067:role/RedshiftS3Role\' CSV dateformat AS \'MM/DD/YYYY\' IGNOREHEADER 1 REGION AS \'eu-central-1\';`;
            redshiftClient2.connect((cErr) => {
              if (cErr) throw new Error(cErr);
              else {
                console.log('Connected to Redshift!');
                redshiftClient2.query(copyCmd, (queryErr, migrateData) => {
                  if (queryErr) throw new Error(queryErr);
                  else {
                    console.log(migrateData);
                    console.log('Migrated to Redshift');
                    redshiftClient2.close();
                  }
                });
              }
            });
          });
      } catch (e) {
        console.log(e);
        throw new Error(e);
      }
    }
  });
} catch (e) {
  throw new Error(e);
}
