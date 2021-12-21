require('dotenv').config();

const axios = require('axios');
const { WebClient, ErrorCode } = require('@slack/web-api');

function sendMessage(client, msg) {
    try {
        // setTimeout(() => {
        //     client.chat.postMessage({
        //         text: msg,
        //         channel: '#alerts-playout',
        //     });
        // }, 30000);
    } catch (error) {
        console.log(error.code);
        console.log(error.data);
    }
}

module.exports = function (app) {
    app.post("/api/slack", function (req, res) {

        const token = process.env.SLACK_TOKEN;
        const web = new WebClient(token);

        if (req.body.challenge) {
            let challenge = req.body.challenge;
            res.json({"challenge":challenge});
        } else {
            console.log("....................................................");
            console.log(req.body);
            if (req.body.event.files) {
                console.log("....................................................");
                // console.log(req.body.event.files[0]);
                axios.get(`https://slack.com/api/files.info?file=${req.body.event.files[0].id}`, {
                    headers: {
                        'Authorization': `Bearer ${token}`
                    }
                })
                    .then(response => {
                        console.log(response.data);
                        res.json({});
                    })
                    .catch(error => {
                        console.log(error);
                    });
                console.log("++++++++++++++++++++++++++++++++++++++++")
            } else {
                res.json({});
            }
            
            // sendMessage(web, 'test');
            // res.json({});
        }
    });

};