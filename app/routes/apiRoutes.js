module.exports = function (app) {
    app.post("/api/slack", function (req, res) {
        let challenge = req.body.challenge;

        res.json({"challenge":challenge});
    });

};