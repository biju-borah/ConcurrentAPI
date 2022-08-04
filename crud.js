const constants = require('./constant.js');

async function writeRecords(sensorID, timeStamp) {
    console.log("Writing records");
    const currentTime = Date.now().toString(); // Unix time in milliseconds

    const data = {
        'x': 1.2,
        'y': 1.2,
        'z': 1.2,
        'a': 1.2,
        'b': 1.2,
        'c': 1.2,
        'm': 1.2,
        'n': 1.2,
        'p': 1.2,
        'Time': currentTime.toString()
    };



    const records = data;

    const params = {
        DatabaseName: constants.DATABASE_NAME,
        TableName: constants.TABLE_NAME + sensorID + "_" + timeStamp,
        Records: records
    };

    const request = writeClient.writeRecords(params);

    await request.promise().then(
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
}

export default writeRecords