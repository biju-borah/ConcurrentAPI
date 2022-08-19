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

    if (timeInterval > 30) {
        queryLength = (timeInterval / 60) * 2 * 30
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

        if (timeInterval > 30) {
            let datas_60 = { "data": [] }
            let j = 0
            let t = 0
            let multi = 1
            var factor = (timeInterval / 60) * 2
            for (let i = 0; i < 30; i++) {
                let data = {}
                let a = 0
                let b = 0
                let x = 0
                let y = 0
                let z = 0
                let d = 0
                let e = 0
                let f = 0
                let g = 0

                if (j < datas.data.length) {
                    var dateUTC = new Date(datas.data[j].time);
                    var dateUTC = dateUTC.getTime()
                    var dateIST = new Date(dateUTC);
                    data["time"] = dateIST.toLocaleString(undefined, { timeZone: 'Asia/Kolkata' });
                    t = j
                }
                else {
                    var dateUTC = new Date(datas.data[t].time);
                    var dateUTC = dateUTC.getTime()
                    var dateIST = new Date(dateUTC);
                    dateIST.setSeconds(dateIST.getSeconds() - timeInterval * multi)
                    multi += 1
                    data["time"] = dateIST.toLocaleString(undefined, { timeZone: 'Asia/Kolkata' });
                }
                for (let k = 0; k < factor; k++) {
                    if (j == datas.data.length) {
                        break
                    }
                    a += Number(datas.data[j].a)
                    b += Number(datas.data[j].b)
                    x += Number(datas.data[j].x)
                    y += Number(datas.data[j].y)
                    z += Number(datas.data[j].z)
                    d += Number(datas.data[j].d)
                    e += Number(datas.data[j].e)
                    f += Number(datas.data[j].f)
                    g += Number(datas.data[j].g)

                    j++
                }

                data["a"] = a / factor
                data["b"] = b / factor
                data["x"] = x / factor
                data["y"] = y / factor
                data["z"] = z / factor
                data["d"] = d / factor
                data["e"] = e / factor
                data["f"] = f / factor
                data["g"] = g / factor

                datas_60.data.push(data)


            }

            datas_60.data.reverse()
            res.status(200).json(datas_60)
        }
        else {
            if (datas.data.length < 30) {
                let multi = 1
                let t = datas.data.length - 1
                for (let i = 0; i < 30; i++) {
                    if (datas.data.length == 30) {
                        break
                    }
                    data = {}

                    var dateUTC = new Date(datas.data[t].time);
                    var dateUTC = dateUTC.getTime()
                    var dateIST = new Date(dateUTC);
                    dateIST.setSeconds(dateIST.getSeconds() - 30 * multi)
                    data["time"] = dateIST.toLocaleString(undefined, { timeZone: 'Asia/Kolkata' });
                    multi += 1


                    data["a"] = 0
                    data["b"] = 0
                    data["x"] = 0
                    data["y"] = 0
                    data["z"] = 0
                    data["d"] = 0
                    data["e"] = 0
                    data["f"] = 0
                    data["g"] = 0
                    datas.data.push(data)
                }
            }
            datas.data.reverse()
            res.status(200).json(datas);
        }

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
                        "Value": String(data.d)
                    },
                    {
                        "Name": "e",
                        "Type": "DOUBLE",
                        "Value": String(data.e)
                    },
                    {
                        "Name": "f",
                        "Type": "DOUBLE",
                        "Value": String(data.f)
                    },
                    {
                        "Name": "g",
                        "Type": "DOUBLE",
                        "Value": String(data.g)
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
