const bodyParser = require("body-parser");
var express = require("express");
var app = express();
const AWS = require("aws-sdk");
AWS.config.update({ region: "us-east-1" });
var https = require('https');
const { DATABASE_NAME, TABLE_NAME } = require('./constant');
const cors = require('cors');

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
app.use(cors());

app.get('/', (req, res, next) => {
    res.status(200).json({});
})

app.get('/fetch', (req, res, next) => {
    if (req.query.api_key !== process.env.API_KEY) {
        res.status(401).json({ err: "Invalid API key" })
        return next();
    }

    const sensor = String(req.query.sensor);
    var timeInterval = Number(req.query.timeInterval);
    let queryLength = 30
    if (timeInterval == 60) {
        queryLength = 31
    }
    else if (timeInterval > 60) {
        queryLength = (timeInterval / 60) * 2 + 29
    }

    query = `select * from ${DATABASE_NAME}.${TABLE_NAME} where sensor = '${sensor}' order by time desc limit ${queryLength}`;

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
        var lastEntryTime = new Date(data.Rows[0].Data[2].ScalarValue)
        var curTime = new Date(Date.now())
        if ((curTime.getTime() - lastEntryTime.getTime()) * 0.001 > timeInterval * 2) {
            res.status(200).json({ err: `No new data has been entered for the last ${timeInterval * 2} secs ` });
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
                if (colinfo[i].Type.ScalarType === "TIMESTAMP") {
                    var dateUTC = new Date(value);
                    var dateUTC = dateUTC.getTime()
                    var dateIST = new Date(dateUTC);
                    // dateIST.setHours(dateIST.getHours() + 5);
                    // dateIST.setMinutes(dateIST.getMinutes() + 30);
                    value = dateIST.toLocaleString(undefined, { timeZone: 'Asia/Kolkata' });
                }
                datapoint[key] = value;
            }
            datas.data.push(datapoint);
        });

        datas.data.reverse()

        if (timeInterval == 60) {
            let datas_60 = { "data": [] }

            for (let i = 1; i < datas.data.length; i++) {
                let data = {}
                data["a"] = (Number(datas.data[i].a) + Number(datas.data[i - 1].a)) / 2
                data["b"] = (Number(datas.data[i].b) + Number(datas.data[i - 1].b)) / 2
                data["x"] = (Number(datas.data[i].x) + Number(datas.data[i - 1].x)) / 2
                data["y"] = (Number(datas.data[i].y) + Number(datas.data[i - 1].y)) / 2
                data["z"] = (Number(datas.data[i].z) + Number(datas.data[i - 1].z)) / 2

                var dateUTC = new Date(datas.data[i - 1].time);
                var dateUTC = dateUTC.getTime()
                var dateIST = new Date(dateUTC);
                dateIST.setHours(dateIST.getHours() + 5);
                dateIST.setMinutes(dateIST.getMinutes() + 30);
                data["time"] = dateIST.toLocaleString(undefined, { timeZone: 'Asia/Kolkata' });

                datas_60.data.push(data)
            }
            res.status(200).json(datas_60);
        }

        else if (timeInterval > 60) {
            let datas_1800 = { "data": [] }

            for (let i = 0; i < 30; i++) {
                let data = {}
                let a = 0
                let b = 0
                let x = 0
                let y = 0
                let z = 0
                for (let j = i; j < datas.data.length; j++) {
                    if (j - i == (timeInterval / 60) * 2) {
                        break
                    }
                    a += Number(datas.data[i].a)
                    b += Number(datas.data[i].a)
                    x += Number(datas.data[i].a)
                    y += Number(datas.data[i].a)
                    z += Number(datas.data[i].a)
                }
                data["a"] = a / 60
                data["b"] = b / 60
                data["x"] = x / 60
                data["y"] = y / 60
                data["z"] = z / 60

                var dateUTC = new Date(datas.data[i - 1].time);
                var dateUTC = dateUTC.getTime()
                var dateIST = new Date(dateUTC);
                dateIST.setHours(dateIST.getHours() + 5);
                dateIST.setMinutes(dateIST.getMinutes() + 30);
                data["time"] = dateIST.toLocaleString(undefined, { timeZone: 'Asia/Kolkata' });

                datas_1800.data.push(data)
            }
            res.status(200).json(datas_1800);
        }
        else res.status(200).json(datas);

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
                        "Name": "d",
                        "Type": "DOUBLE",
                        "Value": String(data.c)
                    },
                    {
                        "Name": "e",
                        "Type": "DOUBLE",
                        "Value": String(data.d)
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
