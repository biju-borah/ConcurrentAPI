var api = require('./node_modules/clicksend/api.js');
class MessageClient {
    constructor() {
        // this.client = twilio(accountSid, authToken);
    }

    async send_sms(message, to) {
        var smsApi = new api.SMSApi(process.env.CLICKSEND_USERNAME, process.env.CLICKSEND_TOKEN);
        var smsMessage = new api.SmsMessage();
        smsMessage.source = "sdk";
        smsMessage.to = to;
        smsMessage.body = message;

        var smsCollection = new api.SmsMessageCollection();

        smsCollection.messages = [smsMessage];

        try {
            const resp = await smsApi.smsSendPost(smsCollection);
            return resp.body;
        } catch(e) {
            return e.body;
        }
    }
}
module.exports = new MessageClient();