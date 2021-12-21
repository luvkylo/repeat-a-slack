require('dotenv').config();

const { WebClient, ErrorCode } = require('@slack/web-api');

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

module.exports = function (app) {
    app.post("/api/slack", function (req, res) {

        const token = process.env.SLACK_TOKEN;
        const web = new WebClient(token);

        sendMessage(web, 'test');

        if (req.body.challenge) {
            let challenge = req.body.challenge;
            res.json({"challenge":challenge});
        } else {
            res.json();
        }
    });

};