require('dotenv').config();

const Redshift = require('node-redshift');
const axios = require('axios');
const cliProgress = require('cli-progress');
const property = require('./property');

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

const regions = {
  ie: {},
  // be: {}, ch: {}, uk: {}, ie: {},
};

// an object to store each redshift query
Object.keys(regions).forEach((region) => {
  regions[region].queryKPICmd = `select
  logs.external_identifier as original_title_id,
  titles.is_adult as adult,
  titles.name as title_name,
  titles.short_synopsis as description,
  case when titles.episode_number<0 THEN NULL ELSE titles.episode_number END as episode_number,
  series.season_number as season_number,
  series.series_name as series_name,
  case when titles.episode_number<0 THEN 'movie' ELSE 'series' END as type,
  titles.region as content_region,
  contents.discoverable_as_vod as VOD,
  count (logs.request_time) as hits
  from cwl_metadata_api_fr as logs
  left join (
    SELECT *
    FROM
      (SELECT title_id, name, episode_number, series_id, region, is_adult, short_synopsis,
              ROW_NUMBER() OVER (PARTITION BY title_id
                                 ORDER BY name, episode_number, series_id, region, is_adult, short_synopsis) AS title_id_ranked
       FROM tv_aug_titles_metadata
       WHERE is_adult=FALSE
       ORDER BY title_id, name, episode_number, series_id, region, is_adult, short_synopsis) AS ranked
    WHERE ranked.title_id_ranked = 1
  ) as titles
  on logs.external_identifier=titles.title_id
  left join (
    SELECT *
    FROM
      (SELECT title_id, discoverable_as_vod, provider_id,
              ROW_NUMBER() OVER (PARTITION BY title_id, provider_id
                                 ORDER BY discoverable_as_vod) AS title_id_ranked
       FROM tv_aug_contents_metadata
       ORDER BY title_id, discoverable_as_vod, provider_id) AS ranked
    WHERE ranked.title_id_ranked = 1
  ) as contents on contents.title_id=logs.external_identifier
  left join (
    SELECT *
    FROM
      (SELECT series_id, series_name, season_number,
              ROW_NUMBER() OVER (PARTITION BY series_id
                                 ORDER BY series_name, season_number) AS series_id_ranked
       FROM tv_aug_series_metadata
       ORDER BY series_id, series_name, season_number) AS ranked
    WHERE ranked.series_id_ranked = 1
  ) as series on series.series_id=titles.series_id
  left join (
    SELECT *
    FROM
      (SELECT title_id, channel_id,
              ROW_NUMBER() OVER (PARTITION BY title_id
                                 ORDER BY channel_id) AS title_id_ranked
       FROM tv_aug_events_metadata
       ORDER BY title_id, channel_id) AS ranked
    WHERE ranked.title_id_ranked = 1
  ) as events on events.title_id=logs.external_identifier
  where logs.request_time>='${startStrMonth}-${startStrDay}-${startYear}' and logs.request_time<'${endStrMonth}-${endStrDay}-${endYear}' and (original_title_id <> '') IS TRUE and vod=TRUE and content_region='${region}'
  group by original_title_id, title_name, episode_number, content_region, VOD, season_number, series_name, is_adult, description
  order by hits desc
  limit 1000;`;

  regions[region].insertKPICmd = 'INSERT INTO tv_aug_kpi_results (start_time, end_time, query_date, type, crid, adult, title_name, description, episode_number, season_number, series_name, region, is_on_demand, hits, api_request_number, video_results, video_response_code) VALUES ';
  regions[region].device = property.tvaug[region].device;
  regions[region].token = property.tvaug[region].token;
});

const startDate = `${startStrMonth}/${startStrDay}/${startYear}`;
const endDate = `${endStrMonth}/${endStrDay}/${endYear}`;
const queryDate = `${queryMonth}/${queryDay}/${queryYear}`;

console.log(`Start Date: ${startDate}`);
console.log(`End Date: ${endDate}`);

let strArr = [];
let respond = [];
let recorded = false;

let i = 0;
let n = 0;

function request(url, headers, bar1, temp, retry = 0) {
  return new Promise((resolve) => {
    axios.get(url, {
      headers,
      muteHttpExceptions: true,
      validateStatus(status) {
        return [200, 404, 500, 502, 504].indexOf(status) !== -1;
      },
    })
      .then((response) => {
        if (response.status === 504 || response.status === 500 || response.status === 502) {
          const { status } = response;
          console.log(`Encountered ${status}`);
          let re = retry;
          if (re < 5) {
            setTimeout(async () => {
              await request(url, headers, bar1, temp, re += 1);
              console.log('Retry successful');
              if (!recorded) {
                bar1.increment();
                respond.push(response);
                strArr.push(temp);
                recorded = true;
              }
              resolve('complete');
            }, 2000);
          } else {
            console.log('Retry failed');
            bar1.stop();
            redshiftClient2.close(() => console.log('\nclosed db'));
            throw new Error(`Encountered ${status} but retry failed after 4 times`);
          }
        } else {
          if (!recorded) {
            bar1.increment();
            respond.push(response);
            strArr.push(temp);
            recorded = true;
          }
          resolve('complete');
        }
      })
      .catch((err) => {
        bar1.stop();
        redshiftClient2.close(() => console.log('\nclosed db'));
        throw new Error(err);
      });
  }).catch((err) => {
    bar1.stop();
    redshiftClient2.close(() => console.log('\nclosed db'));
    throw new Error(err);
  });
}

function query(region) {
  return new Promise((reso, rej) => {
    // redshift query to get the top 1000 crids
    redshiftClient2.query(regions[region].queryKPICmd, async (queryErr, queryData) => {
      i += 1;
      if (queryErr) rej(new Error({ err: queryErr }));
      else {
        console.log(Buffer.byteLength(JSON.stringify(queryData.rows), 'utf8'));
        console.log(`Got region: ${region}`);

        const str = `${region} [{bar}] {percentage}% | ETA: {eta}s | {value}/{total}`;
        const bar1 = new cliProgress.SingleBar({ format: str });
        bar1.start(queryData.rows.length, 0);

        // for each crid, call the prd-lgi-api to get wikidata for that item
        for (let p = 0; p < queryData.rows.length; p += 1) {
          const row = queryData.rows[p];
          const url = `http://prd-lgi-api.frequency.com/api/2.0/programs/videos?video_image=256w144h,solid,rectangle&external_identifier_source=LGI&external_identifier=${row.original_title_id}&page_size=43`;
          const headers = { 'X-Frequency-DeviceId': regions[region].device, 'X-Frequency-Auth': regions[region].token };
          const original_title_id = row.original_title_id ? row.original_title_id.replace(/('|")/g, "\\'") : row.original_title_id;
          let title_name = row.title_name ? row.title_name.replace(/('|")/g, "\\'") : row.title_name;
          title_name = title_name === 'null' ? '' : `'${title_name}'`;
          let description = row.description ? row.description.replace(/('|")/g, "\\'") : row.description;
          description = description === 'null' ? '' : `'${description}'`;
          let series_name = row.series_name ? row.series_name.replace(/('|")/g, "\\'") : row.series_name;
          series_name = series_name === 'null' ? '' : `'${series_name}'`;
          const temp = `'${startDate}','${endDate}','${queryDate}','${row.type}','${original_title_id}',${row.adult},${title_name},${description},${row.episode_number},${row.season_number},${series_name},'${row.content_region}',${row.vod},${row.hits}`;

          recorded = false;
          await request(url, headers, bar1, temp);
        }


        // manipulate the data for the kpi table
        for (let x = 0; x < respond.length; x += 1) {
          let res = false;
          if (respond[x].data.message === 'no program exists with for the external identifier provided!') {
            res = true;
            strArr[x] = `${strArr[x]},-1,'',${respond[x].status}`;
          } else if (respond[x].data.videos) {
            res = true;
            let resultsVideos = '';
            for (let videosIndex = 0; videosIndex < Math.min(5, respond[x]
              .data.videos.length); videosIndex += 1) {
              if (resultsVideos === '') {
                resultsVideos = `${respond[x].data.videos[videosIndex].title}~${respond[x].data.videos[videosIndex].duration}~${respond[x].data.videos[videosIndex].image_url}~${respond[x].data.videos[videosIndex].media_url}`;
              } else {
                resultsVideos += `~${respond[x].data.videos[videosIndex].title}~${respond[x].data.videos[videosIndex].duration}~${respond[x].data.videos[videosIndex].image_url}~${respond[x].data.videos[videosIndex].media_url}`;
              }
            }
            strArr[x] = `${strArr[x]},${respond[x].data.videos.length},'${resultsVideos.replace(/('|")/g, "\\'")}',${respond[x].status}`;
          } else {
            console.log(respond[x]);
          }

          // append the result to the query string
          strArr[x] = `(${strArr[x]})`;
          if (res) {
            if (x !== strArr.length - 1) { regions[region].insertKPICmd += `${strArr[x]},`; } else { regions[region].insertKPICmd += `${strArr[x]};`; }
          }
        }

        if (Buffer.byteLength(regions[region].insertKPICmd, 'utf8') >= 16777216) {
          rej(new Error({ bar: bar1, err: 'Too Big!' }));
        }
        const md = regions[region].insertKPICmd.replace(/(\r\n|\r|\n)/g, '').replace(/\\\\/g, '\\');
        n += 1;
        console.log(`\n${n}`);

        // run the redshift query to insert the top 1000 data
        redshiftClient2.query(md, (e, d) => {
          if (e) {
            console.log(e);
            bar1.stop();
            throw new Error(e);
          } else {
            console.log(`\nAll data written into table for region: ${region}`);
            if (i === Object.keys(regions).length) {
              redshiftClient2.close(() => {
                console.log('\nclosed db');
                reso({ bar: bar1, data: d });
              });
            } else {
              reso({ bar: bar1, data: d });
            }
            // reso({ bar: bar1, data: d });
          }
        });
      }
    });
  });
}

try {
  redshiftClient2.connect(async (connectErr) => {
    if (connectErr) throw new Error(connectErr);
    else {
      console.log('Connected to Redshift!');
      for (let x = 0; x < Object.keys(regions).length; x += 1) {
        strArr = [];
        respond = [];
        console.log(`\nquerying region: ${Object.keys(regions)[x]}`);
        try {
          const complete = await query(Object.keys(regions)[x]).catch((e) => {
            if (e.bar) {
              e.bar.stop();
            }
            console.log(e);
            throw new Error(e.err);
          });
          complete.bar.stop();
          console.log(complete.data);

          // to remove data from tv_aug_(table)_metadata that is 1 month or older
          // const ti = ['events', 'channels', 'contents', 'credits', 'genres',
          //   'pictures', 'products', 'series', 'titles'];

          // ti.forEach((table) => {
          // const deleteCmd = `DELETE FROM tv_aug_${table}_metadata
          //    WHERE ingest_time<${startDate}`;

          //   redshiftClient2.query(deleteCmd, (err, data) => {
          //     if (err) throw new Error(err);
          //     else {
          //       console.log(data);
          //       if (table === 'titles') {
          //         redshiftClient2.close(() => {
          //           console.log('\nclosed db');
          //         });
          //       }
          //     }
          //   });
          // });
        } catch (e) {
          console.log(e);
          throw new Error(e);
        }
      }
    }
  });
} catch (e) {
  throw new Error(e);
}
