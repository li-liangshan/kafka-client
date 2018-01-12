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
const ProducerClient_1 = require("../src/ProducerClient");
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
ava_1.default('producerClient status on connected', (t) => __awaiter(this, void 0, void 0, function* () {
    const client = new Kafka.Client(url);
    const producerClient = new ProducerClient_1.ProducerClient({ client });
    yield producerClient.connect();
    t.true(producerClient.isConnected());
    yield producerClient.close();
    t.false(producerClient.isConnected());
}));
ava_1.default('producerClient send message topic different and partition same...', (t) => __awaiter(this, void 0, void 0, function* () {
    const client = new Kafka.Client(url);
    const producerClient = new ProducerClient_1.ProducerClient({ client });
    const payloads = [
        { topic: 'topic1', message: 'I am topic one...', partitions: 0 },
        { topic: 'topic2', message: 'I am topic two...', partitions: 0 },
    ];
    try {
        debug(`payloads => ${JSON.stringify(payloads)}`);
        const data = yield producerClient.send(payloads);
        debug(`data => ${JSON.stringify(data)}`);
        t.pass();
    }
    catch (err) {
        debug(err);
        t.fail();
    }
}));
ava_1.default('producerClient send message topic same and partition same...', (t) => __awaiter(this, void 0, void 0, function* () {
    const client = new Kafka.Client(url);
    const producerClient = new ProducerClient_1.ProducerClient({ client });
    const payloads = [
        { topic: 'topic1', message: 'I am topic one...', partitions: 0 },
        { topic: 'topic1', message: 'I am topic two...', partitions: 0 },
    ];
    try {
        debug(`payloads => ${JSON.stringify(payloads)}`);
        const data = yield producerClient.send(payloads);
        debug(`data => ${JSON.stringify(data)}`);
        t.pass();
    }
    catch (err) {
        debug(err);
        t.fail();
    }
}));
ava_1.default('producerClient send message topic same and partition different...', (t) => __awaiter(this, void 0, void 0, function* () {
    const client = new Kafka.Client(url);
    const producerClient = new ProducerClient_1.ProducerClient({ client, requireAcks: 0 }, true);
    const payloads = [
        { topic: 'topic1', message: 'I am topic one...', partitions: 0 },
        { topic: 'topic1', message: 'I am topic two...', partitions: 1 },
    ];
    try {
        debug(`payloads => ${JSON.stringify(payloads)}`);
        const data = yield producerClient.send(payloads);
        debug(`data => ${JSON.stringify(data)}`);
        t.pass();
    }
    catch (err) {
        debug(`err => ${JSON.stringify(err)}`);
        t.fail();
    }
}));
ava_1.default('producerClient create topics', (t) => __awaiter(this, void 0, void 0, function* () {
    const client = new Kafka.Client(url);
    const producerClient = new ProducerClient_1.ProducerClient({ client });
    try {
        const topicName1 = yield producerClient.createTopics(['yong', 'named'], true);
        debug(`topicsName1 ==> ${JSON.stringify(topicName1)}`);
        const topicName2 = yield producerClient.createTopics(['yong', 'manand'], true);
        debug(`topicsName2 ==> ${JSON.stringify(topicName2)}`);
        t.pass();
    }
    catch (err) {
        t.fail();
    }
}));
//# sourceMappingURL=ProducerClient.spec.js.map