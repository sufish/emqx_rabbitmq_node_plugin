const amqp = require('amqplib/callback_api');
const bson = require('bson')

class RabbitmqHelper {
    constructor(uri, exchange) {
        this.uri = uri;
        this.exchange = exchange;
        this.channel = null;
    }

    connect() {
        let that = this
        amqp.connect(this.uri, function (error0, connection) {
            if (error0) {
                console.log(error0);
            } else {
                console.log(`rabbitmq connected: ${that.uri}`);
                connection.createChannel(function (error1, channel) {
                    if (error1) {
                        console.log(error1)
                    } else {
                        that.channel = channel;
                        channel.assertExchange(that.exchange, 'direct', {durable: true})
                    }
                });
            }
        });
    }

    publish(data, event) {
        if (this.channel != null) {
            this.channel.publish(this.exchange, event, bson.serialize(data), {persistent: true})
        }
    }
}

module.exports = RabbitmqHelper;