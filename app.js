const bodyParser = require("body-parser");
var express = require("express");
var app = express();
const AWS = require("aws-sdk");
AWS.config.update({ region: "us-east-1" });
var https = require('https');
const { DATABASE_NAME, TABLE_NAME } = require('./constant')


var agent = new https.Agent({
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
                Dimensions: [
                    {
                        Name: 'SensorID',
                        Value: `${id}`,
                        DimensionValueType: "VARCHAR"
                    },
                ],
                // "MeasureValues": [
                //     {
                //         "Name": "x",
                //         "Type": "DOUBLE",
                //         "Value": "1.2"
                //     },
                //     {
                //         "Name": "y",
                //         "Type": "DOUBLE",
                //         "Value": "1.2"
                //     },
                //     {
                //         "Name": "z",
                //         "Type": "BIGINT",
                //         "Value": "1.2"
                //     },
                //     {
                //         "Name": "a",
                //         "Type": "BIGINT",
                //         "Value": "1.2"
                //     },
                //     {
                //         "Name": "b",
                //         "Type": "BIGINT",
                //         "Value": "1.2"
                //     },
                //     {
                //         "Name": "c",
                //         "Type": "BIGINT",
                //         "Value": "1.2"
                //     },
                //     {
                //         "Name": "m",
                //         "Type": "VARCHAR",
                //         "Value": "life"
                //     },
                //     {
                //         "Name": "n",
                //         "Type": "VARCHAR",
                //         "Value": "is"
                //     },
                //     {
                //         "Name": "p",
                //         "Type": "VARCHAR",
                //         "Value": "beautiful"
                //     }
                // ],
                'MeasureName': 'X',
                'MeasureValue': '1.2',
                'MeasureValueType': 'DOUBLE',

                "Time": `${currentTime.toString()}`,
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

