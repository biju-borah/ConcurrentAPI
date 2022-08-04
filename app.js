const bodyParser = require("body-parser");
var express = require("express");
var app = express();
const AWS = require("aws-sdk");
AWS.config.update({ region: "us-east-1" });
var http = require('http');
const { DATABASE_NAME, TABLE_NAME } = require('./constant')


var agent = new http.Agent({
    maxSockets: 5000
});
writeClient = new AWS.TimestreamWrite({
    accessKeyId: process.env.AWS_ACCESS_KEY_ID,
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
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
    const currentTime = Date.now().toString(); // Unix time in milliseconds

    // const data = {
    //     'x': 1.2,
    //     'y': 1.2,
    //     'z': 1.2,
    //     'a': 1.2,
    //     'b': 1.2,
    //     'c': 1.2,
    //     'm': 1.2,
    //     'n': 1.2,
    //     'p': 1.2,
    //     'Time': currentTime.toString()
    // };

    // const records = data;

    const params = {
        "DatabaseName": "IoT",
        "Records": [
            {
                "MeasureValues": [
                    {
                        "Name": "x",
                        "Type": "1.2",
                        "Value": "BIGINT"
                    },
                    {
                        "Name": "y",
                        "Type": "1.2",
                        "Value": "BIGINT"
                    },
                    {
                        "Name": "z",
                        "Type": "1.2",
                        "Value": "BIGINT"
                    },
                    {
                        "Name": "a",
                        "Type": "1.2",
                        "Value": "BIGINT"
                    },
                    {
                        "Name": "b",
                        "Type": "1.2",
                        "Value": "BIGINT"
                    },
                    {
                        "Name": "c",
                        "Type": "1.2",
                        "Value": "BIGINT"
                    },
                    {
                        "Name": "m",
                        "Type": "life",
                        "Value": "STRING"
                    },
                    {
                        "Name": "n",
                        "Type": "is",
                        "Value": "STRING"
                    },
                    {
                        "Name": "p",
                        "Type": "beautiful",
                        "Value": "STRING"
                    }
                ],
                "Time": currentTime.toString(),
                "TimeUnit": "SECONDS",
                "Version": 1
            }
        ],
        "TableName": `IoT_${id}_${timeInterval}`
    };

    const request = writeClient.writeRecords(params);

    request.promise().then(
        (data) => {
            console.log("Write records successful");
        },
        (err) => {
            console.log("Error writing records:", err);
            if (err.code === 'RejectedRecordsException') {
                const responsePayload = JSON.parse(request.response.httpResponse.body.toString());
                console.log("RejectedRecords: ", responsePayload.RejectedRecords);
                console.log("Other records were written successfully. ");
            }
        }
    );
    res.status(200)
});

app.listen(process.env.PORT, () => {
    console.log("Server running......");
});

