const { TimestreamQueryClient, QueryCommand } = require("@aws-sdk/client-timestream-query");
const AWS = require("aws-sdk");
var sensor_mob_number = require('./sensors.json')
var queryClient = new TimestreamQueryClient({ region: "us-east-1" })
var https = require('https');
var agent = new https.Agent({
    maxSockets: 5000
});
const NodeCache = require( "node-cache" );
const messageService  = require('./messageSender.js');
var cron = require('node-cron');
queryClient = new AWS.TimestreamQuery(
    {
    accessKeyId: process.env.AWS_ACCESS_KEY_ID,
    secretAccessKey: process.env.AWS_SECRET_ACCESS_KEY,
    maxRetries: 10,
    httpOptions: {
        timeout: 20000,
        agent: agent
    }
}
);

// 48 hours === 172800 seconds
// per 30 seconds check the db if 

const MESSAGE_TIME=process.env.MESSAGE_TIME;
const CACHE_CHECK_PERIOD=process.env.CACHE_CHECK_PERIOD
const CONSTANT_CHECK_DB_TIME=process.env.CONSTANT_CHECK_DB_TIME;

const cache = new NodeCache({ stdTTL: MESSAGE_TIME, checkperiod: CACHE_CHECK_PERIOD });

async function check_condition(var_name, var_val) {
    if(!var_val) return false;
    switch(var_name) {
        case 'a':
            return var_val>1 && var_val<5;
        case 'b':
            return var_val>1 && var_val<5;
        case 'd':
            return var_val>1 && var_val<5;
        case 'e':
            return var_val>1 && var_val<5;
        case 'f':
            return var_val>1 && var_val<5;
        case 'x':
            return var_val>1 && var_val<5;
        case 'y':
            return var_val>1 && var_val<5;
        case 'z':
            return var_val>1 && var_val<5;
        default:
            return true;
    }
}


async function cron_query() {

    query = `SELECT sensor,time,a,b,d,e,f,x,y,z
                FROM "IoT"."IoT_30" s1
                WHERE 
                time = (SELECT MAX(time) FROM  "IoT"."IoT_30" s2 WHERE s1.sensor = s2.sensor)
                and
                (sensor = '1'  or sensor = '2'  or sensor = '3'  or sensor = '4'  or sensor = '5')`;

    let response;
    try {
        response = queryClient.query(params = {
            QueryString: query,
        }).promise();
    } catch (err) {
        console.error("Error while querying:", err);
        throw err;
    }

    const db = await response.then(data => {
        const {Rows, ColumnInfo} = data;
        const datas = [];
        Rows.forEach(record => {
            const values = record.Data;
            let datapoint = {};
            for (let i = 0; i < values.length; i++) {
                let key = ColumnInfo[i].Name;
                let value = values[i].ScalarValue;
                if (ColumnInfo[i].Type.ScalarType === "DOUBLE" || ColumnInfo[i].Type.ScalarType === "BIGINT") value = Number(value);
                if (ColumnInfo[i].Type.ScalarType === "BOOLEAN") value = Boolean(value);
                if (ColumnInfo[i].Type.ScalarType === "TIMESTAMP") {
                    var dateUTC = new Date(value);
                    var dateUTC = dateUTC.getTime()
                    var dateIST = new Date(dateUTC);
                    value = dateIST.toLocaleString(undefined, { timeZone: 'Asia/Kolkata' });
                }
                datapoint[key] = value;
            }
            datas.push(datapoint);
        });
        return datas;
    })
    db.forEach(async(ob) => {
        const {sensor, a, b, d, e, f, x, y, z} = ob;
        const variables = {a, b, d, e, f, x, y, z};
        let check = true;
        for(const var_name in variables) {
            const var_val = variables[var_name];
            if(!var_val) continue;
            check&=(await check_condition(var_name, var_val));
        }
        if(!check) {
            if(!cache.get(sensor)) {
                try {
                    const res = await messageService.send_sms(`Sensor: ${sensor} is invalid and is not in range. Will notify after ${MESSAGE_TIME} seconds again!!`, sensor_mob_number[sensor]);
                    cache.set(sensor, -1)
                } catch(e) {
                    console.log("Error while sending message!!!")
                }       
            } else {
                console.log(`Already Sent: ${sensor} is not in range:`);
            }
        } else {
            console.log(`${sensor} is not in range`);
        }
    })
}


const schedule = async() => {
    cron.schedule(`*/${CONSTANT_CHECK_DB_TIME} * * * * *`, () => {
        cron_query();
    });
}

module.exports = {schedule}