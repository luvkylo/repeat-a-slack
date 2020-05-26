require('dotenv').config();

const Redshift = require('node-redshift');
const axios = require('axios');
const cliProgress = require('cli-progress');
const fs = require('fs');
const path = require('path');
const property = require('./property');

const client = property.redshift;

// Create Redshift connection
const redshiftClient2 = new Redshift(client, { rawConnection: true });

// Get date for file name
let date = new Date();

date = new Date(Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), date.getUTCDate() - 3));

const endMonth = date.getUTCMonth() + 1;
const endYear = date.getUTCFullYear();
const endD = date.getUTCDate();

const endStrMonth = endMonth < 10 ? `0${endMonth}` : endMonth;
const endStrDay = endD < 10 ? `0${endD}` : endD;

date = new Date();
date = new Date(Date.UTC(date.getUTCFullYear(), date.getUTCMonth(), date.getUTCDate() - 10));

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
  // be: {},
  be: {}, ch: {}, nl: {}, uk: {}, ie: {},
};

// an object to store each redshift query
Object.keys(regions).forEach((region) => {
  regions[region].queryKPICmd = `select
  logs.external_identifier as original_title_id,
  logs.http_code as http_code,
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
  where logs.request_time>='${startStrMonth}-${startStrDay}-${startYear}' and logs.request_time<'${endStrMonth}-${endStrDay}-${endYear}' and (original_title_id <> '') IS TRUE and vod IS TRUE and content_region='${region}' and http_code='404'
  group by original_title_id, title_name, episode_number, content_region, VOD, season_number, series_name, is_adult, description, http_code
  order by hits desc
  limit 5000;`;
});

const startDate = `${startStrMonth}/${startStrDay}/${startYear}`;
const endDate = `${endStrMonth}/${endStrDay}/${endYear}`;
const queryDate = `${queryMonth}/${queryDay}/${queryYear}`;

console.log(`Start Date: ${startDate}`);
console.log(`End Date: ${endDate}`);

let promises = [];

let i = 0;

function query(region) {
  return new Promise((reso, rej) => {
    // redshift query to get the top 1000 crids
    redshiftClient2.query(regions[region].queryKPICmd, (queryErr, queryData) => {
      i += 1;
      if (queryErr) rej(new Error({ err: queryErr }));
      else {
        const filename = `tvaug_metadata_manual_match_${region}_${startDate.replace(/\//g, '')}_to_${endDate.replace(/\//g, '')}.csv`;
        const filepath = path.join(__dirname, 'JSON', filename);
        const stream = fs.createWriteStream(filepath, { flags: 'w' });
        console.log(`Writing into file: ${filepath}`);

        stream.write('start_date,end_date,query_date,region,hits,title_name,type,series_name,season_number,episode_number,description,crid,imdb_id,imdb_title,wiki_id\n');

        console.log(`Got region: ${region}`);

        const str = `${region} [{bar}] {percentage}% | ETA: {eta}s | {value}/{total}`;
        const bar1 = new cliProgress.SingleBar({ format: str });
        bar1.start(queryData.rows.length, 0);

        let p = 0;

        // for each crid, call the prd-lgi-api to get wikidata for that item
        queryData.rows.forEach((row) => {
          const original_title_id = row.original_title_id ? row.original_title_id.replace(/('|")/g, "\\'") : row.original_title_id;
          let title_name = row.title_name ? row.title_name.replace(/('|")/g, "\\'") : row.title_name;
          title_name = title_name === null ? '' : `"${title_name}"`;
          let description = row.description ? row.description.replace(/('|")/g, "\\'") : row.description;
          description = description === null ? '' : `"${description}"`;
          let series_name = row.series_name ? row.series_name.replace(/('|")/g, "\\'") : row.series_name;
          series_name = series_name === null ? '' : `"${series_name}"`;
          const episode_number = row.episode_number === null ? '' : row.episode_number;
          const season_number = row.season_number === null ? '' : row.season_number;
          let temp = `${startDate},${endDate},${queryDate},"${row.content_region}",${row.hits},${title_name},"${row.type}",${series_name},${episode_number},${season_number},${description},"${original_title_id.replace("'", '')}"`;
          const imdbUrl = `https://imdb-internet-movie-database-unofficial.p.rapidapi.com/search/${encodeURIComponent(title_name.replace(/&/g, '').replace(/%/g, '').replace(/#/g, '').replace(/\./g, '')
            .replace(/"/g, '')
            .replace(/\\/g, '')
            .replace(/\//g, ''))}`;
          const imdbHeaders = {
            'x-rapidapi-host': 'imdb-internet-movie-database-unofficial.p.rapidapi.com',
            'x-rapidapi-key': 'QcF3JtuVDHmshivO9b1f7ji4jb7Fp1AJQlGjsnJ9OfB3RZ56N8',
          };


          promises.push(new Promise((resolve) => {
            setTimeout(() => {
              axios.get(imdbUrl, {
                headers: imdbHeaders,
                muteHttpExceptions: false,
              })
                .then((response) => {
                  const { data } = response;
                  let imdbID = '';
                  let imdbTitle = '';
                  let wikiID = '';

                  if (data.titles.length > 0) {
                    for (const title of data.titles) {
                      if (title_name.replace(/"|#|\\/g, '').toLowerCase().includes(title.title.toLowerCase()) || title.title.toLowerCase().includes(title_name.replace(/"|#|\\/g, '').toLowerCase())) {
                        imdbID = title.id;
                        imdbTitle = title.title;
                        break;
                      }
                    }
                  }


                  temp += `,${imdbID},"${imdbTitle}"`;

                  if (imdbTitle !== '') {
                    const wikiUrl = `https://www.wikidata.org/w/api.php?action=wbsearchentities&search=${encodeURIComponent(imdbTitle)}&format=json&language=en&uselang=en&type=item`;

                    axios.get(wikiUrl, {
                      muteHttpExceptions: false,
                    })
                      .then((resp) => {
                        const wikidata = resp.data;
                        let year = 0;

                        let tempWikiId = '';

                        if (wikidata.search.length === 1) {
                          wikiID = wikidata.search[0].id;
                        } else {
                          for (const item of wikidata.search) {
                            if (row.type === 'movie' && item.description) {
                              if (item.description.includes('film')) {
                                tempWikiId = item.id;
                                const yearExtract = item.description.match(/\d{4}/);
                                if (yearExtract && +yearExtract[0] > year) {
                                  year = +yearExtract[0];
                                  wikiID = item.id;
                                }
                              }
                            } else if (row.type === 'series' && item.description) {
                              if (item.description.includes('series')) {
                                tempWikiId = item.id;
                                const yearExtract = item.description.match(/\d{4}/);
                                if (yearExtract && +yearExtract[0] > year) {
                                  year = +yearExtract[0];
                                  wikiID = item.id;
                                }
                              }
                            }
                          }
                        }


                        if (wikiID === '') {
                          wikiID = tempWikiId;
                        }

                        temp += `,${wikiID}`;
                        stream.write(`${temp}\n`);
                        bar1.increment();
                        resolve([`Completed for title: ${title_name}`]);
                      })
                      .catch((err) => {
                        bar1.increment();
                        console.log(err);
                        resolve(['error', title_name + err]);
                      });
                  } else {
                    temp += `,${wikiID}`;
                    stream.write(`${temp}\n`);
                    bar1.increment();
                    resolve([`Completed for title: ${title_name}`]);
                  }
                })
                .catch((err) => {
                  bar1.increment();
                  console.log(err);
                  resolve(['error', title_name + err]);
                });
            }, p * 250);
          }).catch((err) => {
            console.log(err);
            throw new Error(err);
          }));
          p += 1;
        });

        Promise.all(promises)
          .then((responses) => {
            // for each response from the api, make sure there are no error
            responses.forEach((response) => {
              if (response[0] === 'error') {
                console.log(`Error: ${response[1]}`);
                throw new Error(response[1]);
              }
            });

            stream.end(() => {
              if (i === Object.keys(regions).length) {
                redshiftClient2.close(() => {
                  console.log('\nclosed db');
                  reso({ bar: bar1, data: 'Completed' });
                });
              } else {
                reso({ bar: bar1, data: 'Completed' });
              }
            });
          })
          .catch((err) => {
            if (err.responses) {
              console.log(err.responses.data);
            } else {
              console.log(err);
            }

            if (i === Object.keys(regions).length) {
              redshiftClient2.close(() => console.log('\nclosed db'));
            }
            rej(new Error({ bar: bar1, err }));
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
        promises = [];
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
