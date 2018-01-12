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
const ava_1 = require("ava");
const Kafka = require("kafka-node");
const Debug = require("debug");
const _ = require("lodash");
const ProducerStreamClient_1 = require("../src/ProducerStreamClient");
const stream_1 = require("stream");
const debug = Debug('LLS:testSpec');
const url = 'localhost:2181/';
ava_1.default.beforeEach(t => {
    debug('ava producerStreamClient test start');
});
ava_1.default.afterEach(t => {
    debug('ava producerStreamClient test end');
});
ava_1.default('producerStreamClient status on connected | closed', (t) => __awaiter(this, void 0, void 0, function* () {
    const producerStreamClient = new ProducerStreamClient_1.ProducerStreamClient({});
    yield producerStreamClient.connect();
    t.true(producerStreamClient.isConnected());
    yield producerStreamClient.close();
    t.false(producerStreamClient.isConnected());
}));
ava_1.default('producerStreamClient send stream successful', (t) => __awaiter(this, void 0, void 0, function* () {
    const producerStreamClient = new ProducerStreamClient_1.ProducerStreamClient();
    yield producerStreamClient.connect();
    t.true(producerStreamClient.isConnected());
    const stdinTransform = new stream_1.Transform({
        objectMode: true,
        decodeStrings: true,
        transform(msg, encoding, callback) {
            const text = _.trim(msg.toString());
            debug(`pushing message ${text} to ExampleTopic`);
            callback(null, {
                topic: 'ExampleTopic',
                messages: text,
            });
            t.pass();
        },
    });
    const consumerOptions = {
        host: '127.0.0.1:2181',
        groupId: 'ExampleTestGroup',
        sessionTimeout: 15000,
        protocol: ['roundrobin'],
        fromOffset: 'latest',
    };
    const consumerGroup = new Kafka.ConsumerGroup(consumerOptions, ['topic-named']);
    yield new Promise((resolve, reject) => {
        consumerGroup.on('connect', () => {
            resolve();
        });
    });
    const message = yield new Promise((resolve, reject) => {
        consumerGroup.on('message', (message) => {
            resolve(message);
        });
    });
    debug(`consumerGroup message => ${JSON.stringify(message)}`);
    const producerStream = yield producerStreamClient.getProducerStream();
    const wrote = producerStream.write({
        topic: 'topic-named',
        message: 'world',
    });
    debug(`producerStream wrote => ${wrote}`);
    t.true(wrote);
    t.pass();
}));
//# sourceMappingURL=ProducerStreamClient.spec.js.map