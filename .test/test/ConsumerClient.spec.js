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
const ConsumerClient_1 = require("../src/ConsumerClient");
const Kafka = require("kafka-node");
const Debug = require("debug");
const debug = Debug('LLS:testSpec');
const url = 'localhost:2181/';
ava_1.default.beforeEach(t => {
    debug('ava test start');
});
ava_1.default.afterEach(t => {
    debug('ava test end');
});
ava_1.default('consumerClient status on connected with different topics, same partitions', (t) => __awaiter(this, void 0, void 0, function* () {
    const client = new Kafka.Client(url);
    const consumerClient = new ConsumerClient_1.ConsumerClient({ client, topics: [
            { topic: 'topic1', partition: 0 },
            { topic: 'topic2', partition: 0 },
        ], options: null });
    yield consumerClient.connect();
    t.true(consumerClient.isConnected());
    yield consumerClient.close();
    t.false(consumerClient.isConnected());
}));
ava_1.default('consumerClient status on connected with different topics, different partitions', (t) => __awaiter(this, void 0, void 0, function* () {
    const client = new Kafka.Client(url);
    const consumerClient = new ConsumerClient_1.ConsumerClient({ client, topics: [
            { topic: 'topic1', partition: 0 },
            { topic: 'topic2', partition: 1 },
        ], options: null });
    yield consumerClient.connect();
    t.true(consumerClient.isConnected());
    yield consumerClient.close();
    t.false(consumerClient.isConnected());
}));
ava_1.default('consumerClient status on connected with same topics, different partitions', (t) => __awaiter(this, void 0, void 0, function* () {
    const client = new Kafka.Client(url);
    const consumerClient = new ConsumerClient_1.ConsumerClient({ client, topics: [
            { topic: 'topic1', partition: 0 },
            { topic: 'topic1', partition: 1 },
        ], options: null });
    yield consumerClient.connect();
    t.true(consumerClient.isConnected());
    yield consumerClient.close();
    t.false(consumerClient.isConnected());
}));
ava_1.default('consumerClient status on connected with different topics, same partitions', (t) => __awaiter(this, void 0, void 0, function* () {
    const client = new Kafka.Client(url);
    const consumerClient = new ConsumerClient_1.ConsumerClient({ client, topics: [
            { topic: 'topic1', partition: 0 },
            { topic: 'topic2', partition: 0 },
        ], options: null });
    yield consumerClient.connect();
    t.true(consumerClient.isConnected());
    yield consumerClient.close();
    t.false(consumerClient.isConnected());
}));
ava_1.default('consumerClient status pause and resume', (t) => __awaiter(this, void 0, void 0, function* () {
    const client = new Kafka.Client(url);
    const consumerClient = new ConsumerClient_1.ConsumerClient({ client, topics: [
            { topic: 'topic1', partition: 0 },
            { topic: 'topic2', partition: 0 },
        ], options: null });
    yield consumerClient.connect();
    t.true(consumerClient.isConnected());
    yield consumerClient.pause();
    yield consumerClient.consumeMessage((message) => debug(`message==>${message}`));
    yield consumerClient.resume();
    yield consumerClient.consumeMessage((message) => debug(`message==>${message}`));
    yield consumerClient.close();
    t.false(consumerClient.isConnected());
}));
ava_1.default('consumerClient commit', (t) => __awaiter(this, void 0, void 0, function* () {
    const client = new Kafka.Client(url);
    const consumerClient = new ConsumerClient_1.ConsumerClient({ client, topics: [
            { topic: 'topic1', partition: 0 },
            { topic: 'topic2', partition: 0 },
        ], options: { autoCommit: false } });
    yield consumerClient.connect();
    t.true(consumerClient.isConnected());
    const commit = yield consumerClient.commit();
    debug(`commit1==>${JSON.stringify(commit)}`);
    yield consumerClient.close();
    t.false(consumerClient.isConnected());
}));
ava_1.default('consumerClient addTopics topic3 not exists', (t) => __awaiter(this, void 0, void 0, function* () {
    const client = new Kafka.Client(url);
    const consumerClient = new ConsumerClient_1.ConsumerClient({ client, topics: [
            { topic: 'topic1', partition: 0 },
            { topic: 'topic2', partition: 0 },
        ], options: { autoCommit: false } });
    try {
        yield consumerClient.connect();
        t.true(consumerClient.isConnected());
        const addTopics = yield consumerClient.addTopics('topic3');
        debug(`addTopics==>${JSON.stringify(addTopics)}`);
        yield consumerClient.close();
        t.false(consumerClient.isConnected());
    }
    catch (err) {
        debug(`addTopics err => ${JSON.stringify(err)}`);
        t.pass();
    }
}));
ava_1.default('consumerClient addTopics topic1 exists', (t) => __awaiter(this, void 0, void 0, function* () {
    const client = new Kafka.Client(url);
    const consumerClient = new ConsumerClient_1.ConsumerClient({ client, topics: [
            { topic: 'topic1', partition: 0 },
            { topic: 'topic2', partition: 0 },
        ], options: { autoCommit: false } });
    try {
        yield consumerClient.connect();
        t.true(consumerClient.isConnected());
        const addTopics = yield consumerClient.addTopics('topic1');
        debug(`addTopics==>${JSON.stringify(addTopics)}`);
        yield consumerClient.close();
        t.false(consumerClient.isConnected());
    }
    catch (err) {
        debug(`addTopics err => ${JSON.stringify(err)}`);
        t.pass();
    }
}));
ava_1.default('consumerClient removeTopics topic1 successful', (t) => __awaiter(this, void 0, void 0, function* () {
    const client = new Kafka.Client(url);
    const consumerClient = new ConsumerClient_1.ConsumerClient({ client, topics: [
            { topic: 'topic1', partition: 0 },
            { topic: 'topic2', partition: 0 },
        ], options: { autoCommit: false } });
    try {
        yield consumerClient.connect();
        t.true(consumerClient.isConnected());
        const removeTopics = yield consumerClient.removeTopics(['topic1']);
        debug(`removeTopics==>${JSON.stringify(removeTopics)}`);
        yield consumerClient.close();
        t.false(consumerClient.isConnected());
    }
    catch (err) {
        debug(`removeTopics err => ${JSON.stringify(err)}`);
        t.fail();
    }
}));
ava_1.default('consumerClient setOffset successful', (t) => __awaiter(this, void 0, void 0, function* () {
    const client = new Kafka.Client(url);
    const consumerClient = new ConsumerClient_1.ConsumerClient({ client, topics: [
            { topic: 'topic1', partition: 0, offset: 0 },
            { topic: 'topic2', partition: 0, offset: 0 },
        ], options: { autoCommit: false } });
    yield consumerClient.connect();
    t.true(consumerClient.isConnected());
    const consumer = yield consumerClient.getConsumer();
    yield consumerClient.setOffset('topic1', 0, 14);
    t.is(consumer.payloads[0].offset, 14);
    yield consumerClient.setOffset('topic2', 0, 13);
    t.is(consumer.payloads[1].offset, 13);
    yield consumerClient.close();
    t.false(consumerClient.isConnected());
}));
ava_1.default('consumerClient pauseTopic | resumeTopics successful', (t) => __awaiter(this, void 0, void 0, function* () {
    const client = new Kafka.Client(url);
    const consumerClient = new ConsumerClient_1.ConsumerClient({ client, topics: [
            { topic: 'topic1', partition: 0 },
            { topic: 'topic2', partition: 0 },
        ], options: {} }); // groupId 数据格式注意
    yield consumerClient.connect();
    t.true(consumerClient.isConnected());
    const consumer = yield consumerClient.getConsumer();
    debug(`payloads pause before ===> ${JSON.stringify(consumer.payloads)}`);
    yield consumerClient.pauseTopics(['topic1']);
    debug(`payloads pause after ===> ${JSON.stringify(consumer.payloads)}`);
    yield consumerClient.resumeTopics(['topic1']);
    debug(`payloads resume ===> ${JSON.stringify(consumer.payloads)}`);
    yield consumerClient.close();
    t.false(consumerClient.isConnected());
}));
ava_1.default('consumerClient getTopicPayloads successful', (t) => __awaiter(this, void 0, void 0, function* () {
    const client = new Kafka.Client(url);
    const consumerClient = new ConsumerClient_1.ConsumerClient({ client, topics: [
            { topic: 'topic1', partition: 0 },
            { topic: 'topic2', partition: 0 },
        ], options: {} }, false);
    yield consumerClient.connect();
    t.true(consumerClient.isConnected());
    const beforeTopicPayloads = yield consumerClient.getTopicPayloads();
    debug(`topicPayloads pause before ===> ${JSON.stringify(beforeTopicPayloads)}`);
    yield consumerClient.pauseTopics(['topic1']);
    const afterTopicPayloads = yield consumerClient.getTopicPayloads();
    debug(`topicPayloads pause after ===> ${JSON.stringify(afterTopicPayloads)}`);
    yield consumerClient.resumeTopics(['topic1']);
    const resumeTopicPayloads = yield consumerClient.getTopicPayloads();
    debug(`topicPayloads resume ===> ${JSON.stringify(resumeTopicPayloads)}`);
    yield consumerClient.close();
    t.false(consumerClient.isConnected());
}));
ava_1.default('consumerClient consumeMessage successful', (t) => __awaiter(this, void 0, void 0, function* () {
    const client = new Kafka.Client(url);
    const producer = new Kafka.Producer(client);
    const km = new Kafka.KeyedMessage('ky', 'message');
    const payloads = [
        { topic: 'topic-noded', messages: ['hello', 'world', km], partition: 0 },
    ];
    yield new Promise((resolve, reject) => {
        producer.on('ready', () => {
            producer.send(payloads, (err, data) => {
                if (err) {
                    debug(`send km message failed ===> ${JSON.stringify(err)}`);
                    return reject(err);
                }
                debug(`send successful ${JSON.stringify(data)}`);
                resolve(data);
            });
        });
    });
    const consumerClient = new ConsumerClient_1.ConsumerClient({ client, topics: [
            { topic: 'topic1', partition: 0 },
            { topic: 'topic-noded', partition: 0 },
        ], options: {} });
    yield consumerClient.connect();
    t.true(consumerClient.isConnected());
    const handler = (message) => {
        debug(`consume message =>${JSON.stringify(message)}`);
    };
    yield consumerClient.consumeMessage(handler);
}));
ava_1.default('consumerClient consumeOffsetOutOfRange successful', (t) => __awaiter(this, void 0, void 0, function* () {
    const client = new Kafka.Client(url);
    const consumerClient = new ConsumerClient_1.ConsumerClient({ client, topics: [
            { topic: 'topic1', partition: 0 },
            { topic: 'topic2', partition: 0 },
        ], options: {} });
    yield consumerClient.connect();
    t.true(consumerClient.isConnected());
    const offsetHandler = (error) => {
        debug(`consumeOffsetOutOfRange err =>${JSON.stringify(error)}`);
    };
    yield consumerClient.consumeOffsetOutOfRange(offsetHandler);
    yield consumerClient.close();
    t.false(consumerClient.isConnected());
}));
//# sourceMappingURL=ConsumerClient.spec.js.map