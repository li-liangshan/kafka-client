"use strict";
var __awaiter = (this && this.__awaiter) || function (thisArg, _arguments, P, generator) {
    return new (P || (P = Promise))(function (resolve, reject) {
        function fulfilled(value) { try { step(generator.next(value)); } catch (e) { reject(e); } }
        function rejected(value) { try { step(generator["throw"](value)); } catch (e) { reject(e); } }
        function step(result) { result.done ? resolve(result.value) : new P(function (resolve) { resolve(result.value); }).then(fulfilled, rejected); }
        step((generator = generator.apply(thisArg, _arguments || [])).next());
    });
};
Object.defineProperty(exports, "__esModule", { value: true });
const Kafka = require("kafka-node");
const Debug = require("debug");
const ConsumerStreamClient_1 = require("../src/ConsumerStreamClient");
const debug = Debug('LLS:testSpec');
const url = 'localhost:2181/';
const existTopics = ['topic1', 'topic2'];
const numberOfMessages = 100;
const client = new Kafka.Client(url);
const producer = new Kafka.Producer(client);
const options = { autoCommit: false, groupId: 'groupId', fetchMaxBytes: 512 };
const testRun = () => __awaiter(this, void 0, void 0, function* () {
    const consumerStreamClient = new ConsumerStreamClient_1.ConsumerStreamClient(client, existTopics, options);
    yield consumerStreamClient.connect();
    consumerStreamClient.isConnected();
    new Promise((resolve, reject) => {
        producer.on('ready', () => {
            producer.createTopics(existTopics, () => {
                const messages = [];
                for (let i = 1; i <= numberOfMessages; i++) {
                    messages.push('stream message ' + i);
                }
                producer.send([
                    { topic: existTopics[0], messages },
                    { topic: existTopics[1], messages: messages[numberOfMessages - 1] },
                ], (err, data) => {
                    // if (err) {
                    //   debug(`error ===> ${JSON.stringify(err)}`);
                    //   return reject(err);
                    // }
                    const topics = [{ topic: existTopics[0] }];
                    resolve(data);
                });
            });
        });
    }).then((msg) => {
        debug(`send data ==>${JSON.stringify(msg)}`);
    }).catch((err) => {
        debug(`send data failed==>${JSON.stringify(err)}`);
    });
});
testRun();
//# sourceMappingURL=Spec.js.map