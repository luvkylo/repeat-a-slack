require('dotenv').config();

const axios = require('axios');
const { WebClient, ErrorCode } = require('@slack/web-api');

let InAlarm = [];

function sendMessage(client, msg) {
    try {
        setTimeout(() => {
            let name = msg.match(/name:\s+(?<name>.+)/);
            console.log(name.groups.name);
            name = name.groups.name;
            if (InAlarm.includes(name)) {
                client.chat.postMessage({
                    text: msg,
                    channel: '#alerts-playout',
                });
            } else {
                console.log("Already resolved");
            }
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
                        if (response.data.file && response.data.file.plain_text) {
                            if (response.data.file.plain_text.match(/entered the ALARM state/)) {
                                let original_text = response.data.file.plain_text;
                                let original_link = response.data.file.url_private;
                                let found = '';
                                let name = '';
                                let time = '';

                                if (original_text.match(/\"Linear-\d+/)) {
                                    found = original_text.match(/"Linear-(\d+)-/);
                                    console.log(found[1]);
                                    found = found[1];
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

                                if (!InAlarm.includes(name)) {
                                    InAlarm.push(name);
                                    console.log(InAlarm);

                                    let msg = `ALARM from Repeat An Alert bot for Channel ${found}\nname: ${name}\ntime: ${time}\nlink to original issue: ${original_link}`;

                                    console.log('Starting message');

                                    sendMessage(web, msg);
                                }
                            } else if (response.data.file.plain_text.match(/entered the OK state/)) {
                                let name = response.data.file.plain_text.match(/Name:\s+(?<name>.+)/);
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
                        res.json({});
                    });
            } else {
                if (req.body.text && req.body.text.includes('Repeat An Alert')) {
                    let msg = req.body.text;
                    sendMessage(web, msg);
                    console.log('Repeating an alert...');
                    res.json({});
                } else {
                    if (req.body.event && req.body.event.type && req.body.event.type == 'reaction_added') {
                        let channel = req.body.event.item.channel;
                        let ts = req.body.event.item.ts;

                        axios.get(`https://slack.com/api/reactions.get?channel=${channel}&timestamp=${ts}`, {
                            headers: {
                                'Authorization': `Bearer ${token}`
                            }
                        })
                            .then(resp => {
                                if (resp.data.message && resp.data.message.text && resp.data.message.text.includes('Repeat An Alert')) {
                                    let txt = resp.data.message.text;
                                    let name = txt.match(/name:\s+(?<name>.+)/);
                                    name = name.groups.name;
                                    console.log("Got user reaction")
                                    if (InAlarm.includes(name)) {
                                        console.log('User reacted to remove the alarm');
                                        removeItemOnce(InAlarm, name);
                                        console.log(InAlarm);
                                    }
                                }
                                res.json({});
                            })
                            .catch(error => {
                                console.log(error);
                                res.json({});
                            });
                    } else {
                        console.log(req.body);
                        console.log('unrecognize request...');
                        res.json({});
                    }
                    
                }
            }
            
            // sendMessage(web, 'test');
            // res.json({});
        }
    });

};