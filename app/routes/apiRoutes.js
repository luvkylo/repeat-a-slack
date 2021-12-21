require('dotenv').config();

const axios = require('axios');
const { WebClient, ErrorCode } = require('@slack/web-api');

let InAlarm = [];

function sendMessage(client, msg) {
    try {
        setTimeout(() => {
            client.chat.postMessage({
                text: msg,
                channel: '#alerts-playout',
            });
        }, 300000);
    } catch (error) {
        console.log(error.code);
        console.log(error.data);
    }
}

function removeItemOnce(arr, value) {
    var index = arr.indexOf(value);
    if (index > -1) {
      arr.splice(index, 1);
    }
    return arr;
  }

module.exports = function (app) {
    app.post("/api/slack", function (req, res) {

        const token = process.env.SLACK_TOKEN;
        const web = new WebClient(token);

        if (req.body.challenge) {
            let challenge = req.body.challenge;
            res.json({"challenge":challenge});
        } else {
            if (req.body.event && req.body.event.files) {
                // console.log(req.body.event.files[0]);
                axios.get(`https://slack.com/api/files.info?file=${req.body.event.files[0].id}`, {
                    headers: {
                        'Authorization': `Bearer ${token}`
                    }
                })
                    .then(response => {
                        if (response.data.plain_text) {
                            if (response.data.plain_text.match(/ALARM:/)) {
                                let original_text = response.data.plain_text;
                                let original_link = response.data.url_private;
                                let found = '';
                                let name = '';
                                let time = '';

                                if (original_text.match(/\"Linear-\d+/)) {
                                    found = original_text.match(/\"Linear-(?<channel_id>\d+)/g);
                                    console.log(found.groups.channel_id);
                                    found = found.groups.channel_id
                                }
                                if (original_text.match(/Name:\s+.+/)) {
                                    name = original_text.match(/Name:\s+(?<name>.+)/);
                                    console.log(name.groups.name);
                                    name = name.groups.name;
                                }
                                if (original_text.match(/Timestamp:\s+.+/)) {
                                    time = original_text.match(/Timestamp:\s+(?<timestamp>.+)/);
                                    console.log(time.groups.timestamp);
                                    time = time.groups.timestamp
                                }

                                InAlarm.push(name);
                                console.log(InAlarm);

                                let msg = 
                                `
                                ALARM from Repeat An Alert bot for Channel ${found}
                                name: ${name}
                                time: ${time}
                                link to original issue: ${original_link}
                                `;

                                console.log('Starting message');

                                sendMessage(web, msg);
                            } else if (response.data.plain_text.match(/OK:/)) {
                                let name = original_text.match(/Name:\s+(?<name>.+)/);
                                console.log(name.groups.name);
                                name = name.groups.name;
                                if (InAlarm.includes(name)) {
                                    console.log('Returning to normal');
                                    removeItemOnce(InAlarm, name);
                                    console.log(InAlarm);
                                }
                            }
                            // console.log(response.data.plain_text);
                            res.json({});
                        } else {
                            console.log('Unrecognized response...');
                            res.json({});
                        }
                    })
                    .catch(error => {
                        console.log(error);
                    });
            } else {
                if (req.body.text && req.body.text.includes('Repeat An Alert')) {
                    let msg = req.body.text;
                    let name = msg.match(/name:\s+(?<name>.+)/);
                    console.log(name.groups.name);
                    name = name.groups.name;
                    if (InAlarm.includes(name)) {
                        sendMessage(web, msg);
                        console.log('Repeating an alert...');
                    }
                    res.json({});
                } else {
                    console.log(req.body);
                    res.json({});
                }
            }
            
            // sendMessage(web, 'test');
            // res.json({});
        }
    });

};