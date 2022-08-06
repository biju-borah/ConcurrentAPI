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

app.use(bodyParser.urlencoded({ extended: false }));
app.use(bodyParser.json());

app.get('/', (req, res, next) => {
    res.status(200);
})

app.post("/write", (req, res, next) => {
    console.log(req.body);
    res.json(req.body);
})

app.get("/write", (req, res, next) => {
    if (req.query.id === undefined) {
        res.status(200).json({ err: "ID is undefined" });
        return next();
    }

    const id = String(req.query.id)
    const timeInterval = String(req.query.timeInterval)
    const currentTime = Date.now().toString(); // Unix time in milliseconds

    const params = {
        "DatabaseName": DATABASE_NAME,
        "TableName": TABLE_NAME,

        "CommonAttributes": {
            'Dimensions': [
                {
                    'Name': 'sensorID',
                    'Value': id,
                    'DimensionValueType': 'VARCHAR'
                },
            ],
            'Time': currentTime,
            'TimeUnit': 'MILLISECONDS',
            // 'MILLISECONDS' | 'SECONDS' | 'MICROSECONDS' | 'NANOSECONDS'
        },

        "Records": [
            {
                'MeasureName': 'X',
                'MeasureValue': '1.2',
                'MeasureValueType': 'DOUBLE',
            },
            {
                'MeasureName': 'X',
                'MeasureValue': '1.2',
                'MeasureValueType': 'DOUBLE',
            },
            {
                'MeasureName': 'Z',
                'MeasureValue': '1.2',
                'MeasureValueType': 'DOUBLE',
            },
            {
                'MeasureName': 'A',
                'MeasureValue': '1.2',
                'MeasureValueType': 'DOUBLE',
            },
            {
                'MeasureName': 'B',
                'MeasureValue': '1.2',
                'MeasureValueType': 'DOUBLE',
            },
            {
                'MeasureName': 'C',
                'MeasureValue': 'True',
                'MeasureValueType': 'BOOLEAN',
            },
            {
                'MeasureName': 'M',
                'MeasureValue': 'life',
                'MeasureValueType': 'VARCHAR',
            },
            {
                'MeasureName': 'N',
                'MeasureValue': 'is',
                'MeasureValueType': 'VARCHAR',
            },
            {
                'MeasureName': 'P',
                'MeasureValue': 'beautiful',
                'MeasureValueType': 'VARCHAR',
            }
        ],
        // MeasureValue desired types : 'DOUBLE'|'BIGINT'|'VARCHAR'|'BOOLEAN'|'TIMESTAMP'|'MULTI'

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

