require('dotenv').config()
const grpc = require("@grpc/grpc-js");
const PROTO_PATH = "./proto/exhook.proto";
const RabbitmqHelper = require("./rabbitmq_helper")
var protoLoader = require("@grpc/proto-loader");
const options = {
    keepCase: true,
    longs: String,
    enums: String,
    defaults: true,
    oneofs: true,
};
var packageDefinition = protoLoader.loadSync(PROTO_PATH, options);
const exhookProto = grpc.loadPackageDefinition(packageDefinition);
const server = new grpc.Server();
const rabbitmqHelper = new RabbitmqHelper(process.env.RABBITMQ_URI, process.env.EXCHANGE)
server.addService(exhookProto.emqx.exhook.v2.HookProvider.service, {
    OnProviderLoaded: (call, callback) => {
        let hooks = [{name: "client.connected"}, {name: "client.disconnected"}, {name: "message.publish"}]
        console.log(`provider loaded`)
        callback(null, {hooks});
    },
    OnClientConnected: (call, callback) => {
        rabbitmqHelper.publish({
            client_id: call.request.clientinfo.clientid,
            username: call.request.clientinfo.username,
            ipaddress: call.request.clientinfo.peerhost,
            connected_at: Date.now()
        }, "client.connected")
        callback(null, {});
    },
    OnClientDisconnected: (call, callback) => {
        rabbitmqHelper.publish({
            client_id: call.request.clientinfo.clientid,
            username: call.request.clientinfo.username,
            ipaddress: call.request.clientinfo.peerhost,
            disconnected_at: Date.now(),
            reason: call.request.reason
        }, "client.disconnected")
        callback(null, {});
    },
    OnMessagePublish: (call, callback) => {
        rabbitmqHelper.publish({
            client_id: call.request.message.from,
            username: call.request.message.headers["username"],
            topic: call.request.message.topic,
            qos: call.request.message.qos,
            payload: call.request.message.payload,
            published_at: call.request.message.timestamp
        }, "message.publish")
        callback(null, {type: 0, message: call.request.message});
    }
});


rabbitmqHelper.connect();
server.bindAsync(
    "0.0.0.0:9000",
    grpc.ServerCredentials.createInsecure(),
    (error, port) => {
        if (error != null) {
            console.log(`error start server ${error}`)
        } else {
            console.log(`Server running at ${port}`);
        }
    }
);
