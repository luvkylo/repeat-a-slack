require('dotenv').config();

const util = require('util');
const Redshift = require('node-redshift');
const axios = require('axios');
const axiosRetry = require('axios-retry');
const cliProgress = require('cli-progress');
const fs = require('fs');
const path = require('path');
const csv = require('fast-csv');
const { s3multipartUpload } = require('./s3_multipart_uploader');
const property = require('./property');
const Utils = require('./dateUtils');
// const property = require('./property_local');

const region = process.env.REGION;

const BUNDLE_SIZE = parseInt(process.env.BUNDLE_SIZE, 10);
const MAX_RETRIES = 5;
const RETRY_DELAY = 2000;
const NEXT_BUNDLE_DELAY = 500; // timeout before sending next bundle
const TEMP_MAX_REQUESTS = process.env.TEMP_MAX_REQUESTS
  ? process.env.TEMP_MAX_REQUESTS : null; // to reduce number of requests (for testing)

if (!BUNDLE_SIZE) {
  throw new Error('Provide BUNDLE_SIZE');
}

// setup retries
axiosRetry(axios, {
  retries: MAX_RETRIES,
  retryDelay: () => RETRY_DELAY,
});

const HEADERS = {
  'X-Frequency-DeviceId': property.tvaug[region].device,
  'X-Frequency-Auth': property.tvaug[region].token,
};

const getProgramsUrl = (crid) => (
  `http://prd-lgi-api.frequency.com/api/2.0/programs/videos?video_image=256w144h,solid,rectangle&external_identifier_source=LGI&external_identifier=${crid}&page_size=43`
);

const ProgressBar = new cliProgress.SingleBar({
  format: `${region} [{bar}] {percentage}% | ETA: {eta}s | {value}/{total}`,
});

// Create Redshift connection
let RedshiftClient = new Redshift(property.redshift, { rawConnection: true });

// Get current date
const date = new Date();

// Start Date
const startDateObject = Utils.getDateObject(
  new Date(Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), date.getUTCDate() - 7)),
);

// End Date
const endDateObject = Utils.getDateObject(
  new Date(Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), date.getUTCDate())),
);

// Query Date
const queryDateObject = Utils.getDateObject(
  new Date(Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), date.getUTCDate())),
);

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
     WHERE ingest_time>='${startDateObject.monthStr}-${startDateObject.dayStr}-${startDateObject.yearStr}' and ingest_time<'${endDateObject.monthStr}-${endDateObject.dayStr}-${endDateObject.yearStr}' AND region='${region}' AND is_adult=FALSE AND (title_id <> '') IS TRUE
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

const fileDate = `${queryDateObject.yearStr}${queryDateObject.monthStr}${queryDateObject.dayStr}`;
const fileName = `tvaug_metadata_match_weekly_snapshot_${region}_${fileDate}.csv`;

const writeStream = fs.createWriteStream(path.join(__dirname, 'JSON', fileName), { flags: 'w' });

util.log(`Writing to file location: ${path.join(__dirname, 'JSON', fileName)}`);

const csvStream = csv.format({ headers: true });
csvStream.pipe(writeStream).on('end', () => util.log('done'));

function closeEverything() {
  try {
    ProgressBar.stop();
    csvStream.end();
    RedshiftClient.close();
  } catch (ignore) {
    //
  }
}

function request(url, headers, obj) {
  return new Promise((resolve, reject) => {
    axios.get(url, {
      headers,
      muteHttpExceptions: true,
      validateStatus(status) {
        return [200, 404].indexOf(status) !== -1;
      },
    })
      .then((response) => {
        const { data } = response;
        if (data.message === 'no program exists with for the external identifier provided!') {
          resolve({
            ...obj,
            number_matched: -1,
          });
        } else if (data.videos) {
          resolve({
            ...obj,
            number_matched: data.videos.length,
          });
        } else {
          util.log('Other Error:', response);
          resolve({
            ...obj,
            number_matched: -1,
          });
        }
      })
      .catch((err) => {
        console.log(err);
        const matches = url.match(/external_identifier=([^\&]*)/);
        let crid = null;
        if (matches[1]) {
          [, crid] = matches;
        }

        reject(new Error(`Request failed: ${err.errno} ${err.syscall}: ${crid}`));
      });
  });
}

function parallel(objects) {
  return Promise.all(
    objects.map((object) => request(
      object.url,
      HEADERS,
      object.baseObject,
    )),
  );
}

function query() {
  return new Promise((resolve) => {
    // redshift query to get the all unique crids
    RedshiftClient.query(queryKPICmd, (queryErr, queryData) => {
      if (queryErr) {
        closeEverything();
        util.log(queryErr);
        return new Error(queryErr);
      }

      RedshiftClient.close();

      util.log(queryData.rows.length);
      util.log(`Got region: ${region}`);

      const promises = [];

      // for each crid, call the prd-lgi-api to get wikidata for that item
      for (let p = 0; p < queryData.rows.length; p += 1) {
        if (TEMP_MAX_REQUESTS && p > TEMP_MAX_REQUESTS) {
          break;
        }

        const row = queryData.rows[p];
        const title_name = row.title_name ? row.title_name.replace(/('|")/g, "\\'") : row.title_name;

        const baseObject = {
          query_date: queryDateObject.fullStr,
          crid: row.crid ? row.crid.replace(/('|")/g, "\\'") : row.crid,
          title_name: title_name === 'null' ? '' : `${title_name}`,
          type: row.type,
          region: row.region,
          is_on_demand: row.is_on_demand,
        };

        promises.push({
          url: getProgramsUrl(row.crid),
          baseObject,
        });
      }

      return resolve(promises);
    });
  });
}

function processRequests(objects, callback) {
  if (objects.length > 0) {
    const bundle = objects.splice(0, BUNDLE_SIZE);

    parallel(bundle)
      .then((data) => {
        data.forEach((newObj) => {
          csvStream.write(newObj);
          ProgressBar.increment();
        });
        return data;
      })
      .then(() => {
        setTimeout(() => {
          processRequests(objects, callback);
        }, NEXT_BUNDLE_DELAY);
      })
      .catch((e) => {
        closeEverything();
        util.log('[ERROR]', e.message);
        throw new Error(e);
      });

    return;
  }

  ProgressBar.stop();
  util.log('No more requests pending... ALL OK');
  csvStream.end(() => {
    callback();
  });
}

try {
  util.log(`Start Date: ${startDateObject.fullStr}`);
  util.log(`End Date: ${endDateObject.fullStr}`);
  util.log('-----------------------------------');

  RedshiftClient.connect(async (connectErr) => {
    if (connectErr) {
      util.log(connectErr);
      throw new Error(connectErr);
    } else {
      util.log('Connected to Redshift!');
      util.log('-----------------------------------');
      util.log(`Querying region: ${region}`);

      try {
        const requests = await query()
          .catch((e) => {
            util.log(e);
            throw new Error(e.err);
          });

        util.log('-----------------------------------');
        util.log(`Requests: ${requests.length}`);
        util.log(`Bundle size: ${BUNDLE_SIZE}`);
        util.log(`Timeout between bundles: ${NEXT_BUNDLE_DELAY}ms`);
        util.log(`Bundles: ${Math.ceil(requests.length / BUNDLE_SIZE)}`);

        ProgressBar.start(requests.length, 0);

        processRequests(requests, () => {
          util.log('Uploading to s3');

          s3multipartUpload(fileName, property.aws.toBucketName,
            property.aws.tvaugWeeklySnapshotFolder, (cb) => {
              util.log(cb);

              const filePath = path.join(__dirname, 'JSON', fileName);

              // Remove the temporary file
              try {
                fs.unlinkSync(filePath);
                util.log('File removed');
              } catch (fileErr) {
                throw new Error(fileErr);
              }

              // Run Redshift query
              util.log('Running Redshift query...');

              const copyCmd = `COPY tv_aug_weekly_match_result from \'s3://${property.aws.toBucketName}/${property.aws.tvaugWeeklySnapshotFolder}/${fileName}\' iam_role \'arn:aws:iam::077497804067:role/RedshiftS3Role\' CSV dateformat AS \'MM/DD/YYYY\' IGNOREHEADER 1 REGION AS \'eu-central-1\';`;

              RedshiftClient = new Redshift(property.redshift, { rawConnection: true });

              RedshiftClient.connect((cErr) => {
                if (cErr) {
                  throw new Error(cErr);
                } else {
                  util.log('Connected to Redshift!');
                  RedshiftClient.query(copyCmd, (queryErr, migrateData) => {
                    if (queryErr) {
                      throw new Error(queryErr);
                    } else {
                      util.log(migrateData);
                      util.log('Migrated to Redshift');
                      RedshiftClient.close();
                    }
                  });
                }
              });
            });
        });
      } catch (e) {
        throw new Error(e);
      }
    }
  });
} catch (e) {
  util.log(`Error :${e}`);
  throw new Error(e);
}
