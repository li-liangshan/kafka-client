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
const ConsumerStreamClient_1 = require("../src/ConsumerStreamClient");
const ProducerClient_1 = require("../src/ProducerClient");
const debug = Debug('LLS:testSpec:ConsumerStreamClient');
const url = 'localhost:2181/';
const existTopics = ['topic1'];
const numberOfMessages = 100;
const options = { groupId: 'groupId', fetchMaxBytes: 512 };
ava_1.default.beforeEach(t => {
    debug('ava consumerStreamClient test start');
});
ava_1.default.afterEach(t => {
    debug('ava consumerStreamClient test end');
});
ava_1.default('consumerStreamClient status on connected | closed', (t) => __awaiter(this, void 0, void 0, function* () {
    const client = new Kafka.Client(url);
    const consumerStreamClient = new ConsumerStreamClient_1.ConsumerStreamClient(client, existTopics, options);
    yield consumerStreamClient.connect();
    t.true(consumerStreamClient.isConnected());
    yield consumerStreamClient.close();
    t.false(consumerStreamClient.isConnected());
}));
ava_1.default('consumerStreamClient should emit a message', (t) => __awaiter(this, void 0, void 0, function* () {
    const client = new Kafka.Client(url);
    const producerClient = new ProducerClient_1.ProducerClient({ client });
    yield producerClient.connect();
    const payloads = [
        { topic: 'topic-node', message: 'Iyyyy', partition: 0 },
    ];
    try {
        yield producerClient.createTopics(['topic-node'], true);
        const data = yield producerClient.send(payloads);
        debug(`data ===========================> ${JSON.stringify(data)}`);
        const consumerStreamClient = new ConsumerStreamClient_1.ConsumerStreamClient(client, ['topic-node'], options);
        yield consumerStreamClient.connect();
        debug(`cms ======= ${consumerStreamClient.isConnected()}`);
        const consumerStream = yield consumerStreamClient.getConsumerStream();
        const consumeHandler = (message) => {
            debug(`consume-stream-message ===>=> ${JSON.stringify(message)}`);
        };
        yield consumerStreamClient.createCommitStream();
        const message = yield consumerStreamClient.consumeStreamMessage(consumeHandler);
        debug(`consume-stream-message ====> ${JSON.stringify(message)}`);
        t.pass();
    }
    catch (err) {
        debug(`error ======> ${JSON.stringify(err)}`);
        t.fail();
    }
}));
//# sourceMappingURL=ConsumerStreamClient.spec.js.map