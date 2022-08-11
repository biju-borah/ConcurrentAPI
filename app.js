const bodyParser = require("body-parser");
var express = require("express");
var app = express();
const AWS = require("aws-sdk");
AWS.config.update({ region: "us-east-1" });
var https = require('https');
const { DATABASE_NAME, TABLE_NAME } = require('./constant');
const { json } = require("body-parser");
const { serialize } = require("v8");

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

app.get('/fetch', (req, res, next) => {
    if (req.query.api_key !== "tPmAT5Ab3j7F9") {
        res.status(401).json({ err: "Invalid API key" })
        return next();
    }

    const sensor = String(req.query.sensor);
    const timeInterval = Number(req.query.timeInterval);


    query = `select * from ${DATABASE_NAME}.${TABLE_NAME} where sensor = '${sensor}' order by time limit 30`;
    let response;
    try {
        response = queryClient.query(params = {
            QueryString: query,
        }).promise();
    } catch (err) {
        console.error("Error while querying:", err);
        throw err;
    }

    response.then((data) => {
        var lastEntryTime = new Date(data.Rows[data.Rows.length() - 1].Data[2].ScalarValue)
        var curTime = new Date(Date.now())
        if ((curTime.getTime() - lastEntryTime.getTime()) * 0.001 > 60) {
            res.status(200).json({ err: "No new data has been entered for the last 60 secs" });
            return next();
        }

        records = data.Rows;
        colinfo = data.ColumnInfo;
        let datas = { "data": [] };

        records.forEach(record => {
            const values = record.Data;
            let datapoint = {};
            for (let i = 0; i < values.length; i++) {
                let key = colinfo[i].Name;
                let value = values[i].ScalarValue;
                if (colinfo[i].Type.ScalarType === "DOUBLE" || colinfo[i].Type.ScalarType === "BIGINT") value = Number(value);
                if (colinfo[i].Type.ScalarType === "BOOLEAN") value = Boolean(value);
                datapoint[key] = value;
            }
            datas.data.reverse()
            datas.data.push(datapoint);
        });
        res.status(200).json(datas)

    }, (err) => {
        console.error("Error while querying:", err);
        res.json(err)
    });
})

app.post("/write", (req, res, next) => {
    if (!Object.keys(req.body).length || !Object.keys(req.body.data).length) {
        res.status(400).json({ err: "Bad request" });
        return next();
    }

    if (!req.body.hasOwnProperty("sensor") || req.body.sensor === "" || req.body.sensor === undefined) {
        res.status(400).json({ err: "Bad request" });
        return next();
    }

    if (req.body.api_key === undefined || req.body.api_key !== "tPmAT5Ab3j7F9") {
        res.status(401).json({ err: "Invalid Api Key" });
        return next();
    }

    const id = String(req.body.sensor)
    const data = req.body.data;
    const currentTime = Date.now().toString(); // Unix time in milliseconds

    const params = {
        "DatabaseName": DATABASE_NAME,
        "Records": [
            {
                Dimensions: [
                    {
                        Name: 'sensor',
                        Value: id,
                        DimensionValueType: "VARCHAR"
                    },
                ],

                'MeasureName': 'data',
                "MeasureValues": [
                    {
                        "Name": "x",
                        "Type": "DOUBLE",
                        "Value": String(data.x)
                    },
                    {
                        "Name": "y",
                        "Type": "DOUBLE",
                        "Value": String(data.y)
                    },
                    {
                        "Name": "z",
                        "Type": "DOUBLE",
                        "Value": String(data.z)
                    },
                    {
                        "Name": "a",
                        "Type": "DOUBLE",
                        "Value": String(data.a)
                    },
                    {
                        "Name": "b",
                        "Type": "DOUBLE",
                        "Value": String(data.b)
                    },
                    {
                        "Name": "c",
                        "Type": "BOOLEAN",
                        "Value": String(data.c)
                    },
                    {
                        "Name": "m",
                        "Type": "VARCHAR",
                        "Value": String(data.m)
                    },
                    {
                        "Name": "n",
                        "Type": "VARCHAR",
                        "Value": String(data.n)
                    },
                    {
                        "Name": "p",
                        "Type": "VARCHAR",
                        "Value": String(data.p)
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

