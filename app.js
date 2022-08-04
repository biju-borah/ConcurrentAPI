const bodyParser = require("body-parser");
const { query } = require("express");
const { writeRecords } = require("./crud")
var express = require("express");
var app = express();
const AWS = require("aws-sdk");
AWS.config.update({ region: "us-east-1" });

var https = require('https');
var agent = new https.Agent({
    maxSockets: 5000
});
writeClient = new AWS.TimestreamWrite({
    maxRetries: 10,
    httpOptions: {
        timeout: 20000,
        agent: agent
    }
});

app.use(bodyParser.urlencoded({ extended: false }));
app.use(bodyParser.json());

app.post("/", (req, res, next) => {
    console.log(req.body)
    res.json(req.body)
})
app.get("/", (req, res, next) => {
    const id = req.params.id
    const timeInterval = req.params.timeInterval
    await writeRecords(id, timeInterval)
    res.status(200)
});

app.listen(process.env.PORT, () => {
    console.log("Server running......");
});