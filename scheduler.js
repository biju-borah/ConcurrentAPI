var sensor_mob_number = require('./sensors.json')
const NodeCache = require( "node-cache" );
const messageService  = require('./messageSender.js');
var cron = require('node-cron');

// 48 hours === 172800 seconds
// per 30 seconds check the db if 

const MESSAGE_TIME=process.env.MESSAGE_TIME;
const CACHE_CHECK_PERIOD=process.env.CACHE_CHECK_PERIOD
const CONSTANT_CHECK_DB_TIME=process.env.CONSTANT_CHECK_DB_TIME;

const cache = new NodeCache({ stdTTL: MESSAGE_TIME, checkperiod: CACHE_CHECK_PERIOD });

async function check_condition(var_name, var_val) {
    switch(var_name) {
        case 'a':
            return var_val>1 && var_val<5;
        case 'b':
            return var_val>1 && var_val<5;
        case 'd':
            return var_val>1 && var_val<5;
        case 'e':
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
    const db = [
        {
            "sensor": "2",
            "var_name": "a",
            "var_val": 2
        },
        {
            "sensor": "3",
            "var_name": "b",
            "var_val": 4
        },
        {
            "sensor": "4",
            "var_name": "d",
            "var_val": 0
        }
    ]
    db.forEach(async(ob) => {
        const {sensor, var_name, var_val} = ob;
        const check = await check_condition(var_name, var_val);
        if(!check) {
            if(!cache.get(var_name)) {
                try {
                    const res = await messageService.send_sms(`${var_name} is ${var_val} and is not in range. Will notify after ${MESSAGE_TIME} seconds again!!`, sensor_mob_number[sensor]);
                    console.log(res);
                    const check = cache.set(var_name, {var_val});
                } catch(e) {
                    console.log("Error while sending message!!!")
                }       
            } else {
                console.log(`Already Sent: ${var_name} is not in range:`);
            }
        } else {
            console.log(`${var_name} is ${var_val} and in range`);
        }
    })
}


const schedule = async() => {
    cron.schedule(`*/${CONSTANT_CHECK_DB_TIME} * * * * *`, () => {
        cron_query();
    });
}

module.exports = {schedule}