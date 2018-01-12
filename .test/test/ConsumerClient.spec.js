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
            {
                topic: 'topic1',
                partitions: 0,
            },
            {
                topic: 'topic2',
                partitions: 0,
            },
        ], options: null });
    yield consumerClient.connect();
    t.true(consumerClient.isConnected());
    yield consumerClient.close();
    t.false(consumerClient.isConnected());
}));
ava_1.default('consumerClient status on connected with different topics, different partitions', (t) => __awaiter(this, void 0, void 0, function* () {
    const client = new Kafka.Client(url);
    const consumerClient = new ConsumerClient_1.ConsumerClient({ client, topics: [
            {
                topic: 'topic1',
                partitions: 0,
            },
            {
                topic: 'topic2',
                partitions: 1,
            },
        ], options: null });
    yield consumerClient.connect();
    t.true(consumerClient.isConnected());
    yield consumerClient.close();
    t.false(consumerClient.isConnected());
}));
ava_1.default('consumerClient status on connected with same topics, different partitions', (t) => __awaiter(this, void 0, void 0, function* () {
    const client = new Kafka.Client(url);
    const consumerClient = new ConsumerClient_1.ConsumerClient({ client, topics: [
            {
                topic: 'topic1',
                partitions: 0,
            },
            {
                topic: 'topic1',
                partitions: 1,
            },
        ], options: null });
    yield consumerClient.connect();
    t.true(consumerClient.isConnected());
    yield consumerClient.close();
    t.false(consumerClient.isConnected());
}));
ava_1.default('consumerClient status on connected with different topics, same partitions', (t) => __awaiter(this, void 0, void 0, function* () {
    const client = new Kafka.Client(url);
    const consumerClient = new ConsumerClient_1.ConsumerClient({ client, topics: [
            {
                topic: 'topic1',
                partitions: 0,
            },
            {
                topic: 'topic2',
                partitions: 0,
            },
        ], options: null });
    yield consumerClient.connect();
    t.true(consumerClient.isConnected());
    yield consumerClient.close();
    t.false(consumerClient.isConnected());
}));
ava_1.default('consumerClient status pause and resume', (t) => __awaiter(this, void 0, void 0, function* () {
    const client = new Kafka.Client(url);
    const consumerClient = new ConsumerClient_1.ConsumerClient({ client, topics: [
            {
                topic: 'topic1',
                partitions: 0,
            },
            {
                topic: 'topic2',
                partitions: 0,
            },
        ], options: null });
    yield consumerClient.connect();
    // const message = await consumerClient.consumeMessage((message) => message);
    // debug(`message1234==>${JSON.stringify(message)}`);
    t.true(consumerClient.isConnected());
    yield consumerClient.pause();
    const message1 = yield consumerClient.consumeMessage((message) => debug(`message==>${message}`));
    debug(`message12345==>${JSON.stringify(message1)}`);
    yield consumerClient.resume();
    const message2 = yield consumerClient.consumeMessage((message) => debug(`message==>${message}`));
    debug(`message12345d6==>${JSON.stringify(message2)}`);
    yield consumerClient.close();
    t.false(consumerClient.isConnected());
}));
ava_1.default('consumerClient commit', (t) => __awaiter(this, void 0, void 0, function* () {
    const client = new Kafka.Client(url);
    const consumerClient = new ConsumerClient_1.ConsumerClient({ client, topics: [
            {
                topic: 'topic1',
                partitions: 0,
            },
            {
                topic: 'topic2',
                partitions: 0,
            },
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
            {
                topic: 'topic1',
                partitions: 0,
            },
            {
                topic: 'topic2',
                partitions: 0,
            },
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
            {
                topic: 'topic1',
                partitions: 0,
            },
            {
                topic: 'topic2',
                partitions: 0,
            },
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
            {
                topic: 'topic1',
                partitions: 0,
            },
            {
                topic: 'topic2',
                partitions: 0,
            },
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
            {
                topic: 'topic1',
                partitions: 0,
            },
            {
                topic: 'topic2',
                partitions: 0,
            },
        ], options: { autoCommit: false } });
    try {
        yield consumerClient.connect();
        t.true(consumerClient.isConnected());
        yield consumerClient.setOffset('topic1', 0, 14);
        debug(`setOffset==>${JSON.stringify('setOffset')}`);
        yield consumerClient.close();
        t.false(consumerClient.isConnected());
    }
    catch (err) {
        debug(`setOffset err => ${JSON.stringify(err)}`);
        t.fail();
    }
}));
//# sourceMappingURL=ConsumerClient.spec.js.map