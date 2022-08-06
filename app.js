const bodyParser = require("body-parser");
var express = require("express");
var app = express();
const AWS = require("aws-sdk");
AWS.config.update({ region: "us-east-1" });
var https = require('https');
const { DATABASE_NAME, TABLE_NAME } = require('./constant');

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

queryClient = new AWS.TimestreamQuery();

app.use(bodyParser.urlencoded({ extended: false }));
app.use(bodyParser.json());

app.get('/', (req, res, next) => {
    res.status(200).json({});
})

app.post("/write", (req, res, next) => {
    console.log(req.body);
    res.json(req.body);
})

app.get("/write", (req, res, next) => {
    if (req.query.api_key !== "tPmAT5Ab3j7F9") {
        res.status(401).json({ err: "Invalid Api Key" });
        return next();
    }


    const id = String(req.query.id)
    const timeInterval = String(req.query.timeInterval)
    const currentTime = Date.now().toString(); // Unix time in milliseconds

    const params = {
        "DatabaseName": DATABASE_NAME,
        "Records": [
            {
                Dimensions: [
                    {
                        Name: 'SensorID',
                        Value: id,
                        DimensionValueType: "VARCHAR"
                    },
                ],

                'MeasureName': 'data',
                "MeasureValues": [
                    {
                        "Name": "x",
                        "Type": "DOUBLE",
                        "Value": "1.2"
                    },
                    {
                        "Name": "y",
                        "Type": "DOUBLE",
                        "Value": "1.2"
                    },
                    {
                        "Name": "z",
                        "Type": "DOUBLE",
                        "Value": "1.2"
                    },
                    {
                        "Name": "a",
                        "Type": "DOUBLE",
                        "Value": "1.2"
                    },
                    {
                        "Name": "b",
                        "Type": "DOUBLE",
                        "Value": "1.2"
                    },
                    {
                        "Name": "c",
                        "Type": "BOOLEAN",
                        "Value": "True"
                    },
                    {
                        "Name": "m",
                        "Type": "VARCHAR",
                        "Value": "life"
                    },
                    {
                        "Name": "n",
                        "Type": "VARCHAR",
                        "Value": "is"
                    },
                    {
                        "Name": "p",
                        "Type": "VARCHAR",
                        "Value": "beautiful"
                    }
                ],
                'MeasureValueType': 'MULTI',

                "Time": currentTime,
                "TimeUnit": "MILLISECONDS",
                "Version": 1
            }
        ],
        "TableName": TABLE_NAME
    };

    const request = writeClient.writeRecords(params);

    request.promise().then(
        (data) => {
            console.log("Write records successful");
            res.status(200).json(data)
        },
        (err) => {
            console.log("Error writing records:", err);
            if (err.code === 'RejectedRecordsException') {
                const responsePayload = JSON.parse(request.response.httpResponse.body.toString());
                console.log("RejectedRecords: ", responsePayload.RejectedRecords);
                console.log("Other records were written successfully. ");
            }
            res.json(err)
        }
    );
});

app.post("/write", (req, res, next) => {
    if (req.body === {}) {
        res.status(200).json({ err: "Bad request" });
        return next();
    }

    if (req.query.api_key !== "tPmAT5Ab3j7F9") {
        res.status(401).json({ err: "Invalid Api Key" });
        return next();
    }

    const id = String(req.body.sensorID)
    const x = String(req.body.x)
    const timeInterval = String(req.body.timeInterval)
    const currentTime = Date.now().toString(); // Unix time in milliseconds

    const params = {
        "DatabaseName": DATABASE_NAME,
        "Records": [
            {
                Dimensions: [
                    {
                        Name: 'SensorID',
                        Value: id,
                        DimensionValueType: "VARCHAR"
                    },
                ],

                'MeasureName': 'data',
                "MeasureValues": [
                    {
                        "Name": "x",
                        "Type": "DOUBLE",
                        "Value": "1.2"
                    },
                    {
                        "Name": "y",
                        "Type": "DOUBLE",
                        "Value": "1.2"
                    },
                    {
                        "Name": "z",
                        "Type": "DOUBLE",
                        "Value": "1.2"
                    },
                    {
                        "Name": "a",
                        "Type": "DOUBLE",
                        "Value": "1.2"
                    },
                    {
                        "Name": "b",
                        "Type": "DOUBLE",
                        "Value": "1.2"
                    },
                    {
                        "Name": "c",
                        "Type": "BOOLEAN",
                        "Value": "True"
                    },
                    {
                        "Name": "m",
                        "Type": "VARCHAR",
                        "Value": "life"
                    },
                    {
                        "Name": "n",
                        "Type": "VARCHAR",
                        "Value": "is"
                    },
                    {
                        "Name": "p",
                        "Type": "VARCHAR",
                        "Value": "beautiful"
                    }
                ],
                'MeasureValueType': 'MULTI',

                "Time": currentTime,
                "TimeUnit": "MILLISECONDS",
                "Version": 1
            }
        ],
        "TableName": TABLE_NAME
    };

    const request = writeClient.writeRecords(params);

    request.promise().then(
        (data) => {
            console.log("Write records successful");
            res.status(200).json(data)
        },
        (err) => {
            console.log("Error writing records:", err);
            if (err.code === 'RejectedRecordsException') {
                const responsePayload = JSON.parse(request.response.httpResponse.body.toString());
                console.log("RejectedRecords: ", responsePayload.RejectedRecords);
                console.log("Other records were written successfully. ");
            }
            res.json(err)
        }
    );
});

app.listen(process.env.PORT, () => {
    console.log("Server running");
});

