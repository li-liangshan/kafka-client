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
const events_1 = require("events");
const stream_1 = require("stream");
const ProducerClient_1 = require("./ProducerClient");
const ConsumerClient_1 = require("./ConsumerClient");
const ConsumerGroupClient_1 = require("./ConsumerGroupClient");
const ConsumerStreamClient_1 = require("./ConsumerStreamClient");
const ConsumerGroupStreamClient_1 = require("./ConsumerGroupStreamClient");
const ProducerStreamClient_1 = require("./ProducerStreamClient");
const OffsetClient_1 = require("./OffsetClient");
const AdminClient_1 = require("./AdminClient");
const debug = Debug('coupler:kafka-mq:client');
class Client extends events_1.EventEmitter {
    constructor(options) {
        super();
        this.closing = false;
        this.connecting = false;
        this.connected = false;
        this.init(options);
    }
    connect() {
        return __awaiter(this, void 0, void 0, function* () {
            if (this.connecting) {
                debug('connect request ignored. client is currently connecting!');
                return;
            }
            this.connecting = true;
            debug('client start to connect...');
            try {
                this.clientInstance = yield this.onConnected();
                debug('client onConnected!!!');
                this.connected = true;
            }
            catch (err) {
                debug('client connect failed...');
                this.clientInstance = null;
                this.connected = false;
            }
            finally {
                this.connecting = false;
            }
        });
    }
    onConnected() {
        return __awaiter(this, void 0, void 0, function* () {
            if (this.connected && this.clientInstance) {
                return this.clientInstance;
            }
            this.connected = false;
            this.clientInstance = null;
            const client = new Kafka.Client(this.options);
            return new Promise((resolve, reject) => {
                if (!client) {
                    return reject('kafka instance create failed...');
                }
                client.on('ready', () => resolve(client));
            });
        });
    }
    close() {
        return __awaiter(this, void 0, void 0, function* () {
            if (this.closing) {
                debug('close request ignored. client has been closed currently.');
                return;
            }
            this.closing = true;
            try {
                const result = yield this.onClosed();
                this.connected = false;
                this.clientInstance = null;
                this.closing = false;
                this.connecting = false;
                debug('client close success...');
                return result;
            }
            catch (err) {
                this.closing = false;
                debug(`client close failed, err => ${JSON.stringify(err)}`);
                throw err;
            }
        });
    }
    onClosed(instance) {
        return __awaiter(this, void 0, void 0, function* () {
            if (!this.closing) {
                debug('close request ignored, closing status must be true...');
                return null;
            }
            if (!this.clientInstance) {
                this.connected = false;
                debug('close request refused. client has been closed currently.');
                return null;
            }
            return new Promise((resolve, reject) => {
                this.clientInstance.close(resolve);
            });
        });
    }
    checkOrConnect(funName) {
        return __awaiter(this, void 0, void 0, function* () {
            debug(`check the client Connected`);
            if (!this.connected) {
                debug(`KafkaClient is not connected [${funName}]`);
                debug('KafkaClient start connecting ....');
                yield this.connect();
            }
            debug(`KafkaClient connected before ${funName}`);
        });
    }
    init(options) {
        return __awaiter(this, void 0, void 0, function* () {
            this.options = options;
            this.clientId = options.clientId;
            this.producerClients = new Map();
            this.consumerClients = new Map();
            this.consumerGroupClients = new Map();
            this.consumerStreamClients = new Map();
            this.consumerGroupStreamClients = new Map();
            this.producerStreamClients = new Map();
            this.offsetClients = new Map();
            this.adminClients = new Map();
            this.producerStreamTransforms = new Map();
            this.consumerStreamTransforms = new Map();
            this.consumerGroupStreamTransforms = new Map();
        });
    }
    addProducerClient(producerId, options, isHighLevel = false, autoConnect = false) {
        return __awaiter(this, void 0, void 0, function* () {
            yield this.checkExists(this.producerClients, producerId, 'producer');
            const producerClient = new ProducerClient_1.ProducerClient(options, isHighLevel, autoConnect);
            this.producerClients.set(producerId, producerClient);
        });
    }
    getProducerClient(producerId) {
        return __awaiter(this, void 0, void 0, function* () {
            return this.getMapItem(this.producerClients, producerId);
        });
    }
    getProducerClientMap() {
        return __awaiter(this, void 0, void 0, function* () {
            return this.producerClients;
        });
    }
    addConsumerClient(consumerId, options, isHighLevel, autoReconnectCount) {
        return __awaiter(this, void 0, void 0, function* () {
            yield this.checkExists(this.consumerClients, consumerId, 'consumer');
            const consumerClient = new ConsumerClient_1.ConsumerClient(options, isHighLevel, autoReconnectCount);
            this.consumerClients.set(consumerId, consumerClient);
        });
    }
    getConsumerClient(consumerId) {
        return __awaiter(this, void 0, void 0, function* () {
            return this.getMapItem(this.consumerClients, consumerId);
        });
    }
    getConsumerClientMap() {
        return __awaiter(this, void 0, void 0, function* () {
            return this.consumerClients;
        });
    }
    addConsumerGroupClient(groupId, options, topics) {
        return __awaiter(this, void 0, void 0, function* () {
            yield this.checkExists(this.consumerGroupClients, groupId, 'ConsumerGroup');
            const consumerGroupClient = new ConsumerGroupClient_1.ConsumerGroupClient(options, topics);
            this.consumerGroupClients.set(groupId, consumerGroupClient);
        });
    }
    getConsumerGroupClient(groupId) {
        return __awaiter(this, void 0, void 0, function* () {
            return this.getMapItem(this.consumerGroupClients, groupId);
        });
    }
    getConsumerGroupClientMap() {
        return __awaiter(this, void 0, void 0, function* () {
            return this.consumerGroupClients;
        });
    }
    addConsumerStreamClient(consumerStreamId, options, topics) {
        return __awaiter(this, void 0, void 0, function* () {
            yield this.checkExists(this.consumerStreamClients, consumerStreamId, 'ConsumerStream');
            const consumerStreamClient = new ConsumerStreamClient_1.ConsumerStreamClient(this.clientInstance, topics, options);
            this.consumerStreamClients.set(consumerStreamId, consumerStreamClient);
        });
    }
    getConsumerStreamClient(consumerStreamId) {
        return __awaiter(this, void 0, void 0, function* () {
            return this.getMapItem(this.consumerStreamClients, consumerStreamId);
        });
    }
    getConsumerStreamClientMap() {
        return __awaiter(this, void 0, void 0, function* () {
            return this.consumerGroupClients;
        });
    }
    addConsumerGroupStreamClients(groupStreamId, options, topics) {
        return __awaiter(this, void 0, void 0, function* () {
            yield this.checkExists(this.consumerGroupStreamClients, groupStreamId, 'ConsumerGroupStream');
            const consumerGroupStreamClient = new ConsumerGroupStreamClient_1.ConsumerGroupStreamClient(options, topics);
            this.consumerGroupStreamClients.set(groupStreamId, consumerGroupStreamClient);
        });
    }
    getConsumerGroupStreamClient(groupStreamId) {
        return __awaiter(this, void 0, void 0, function* () {
            return this.getMapItem(this.consumerGroupStreamClients, groupStreamId);
        });
    }
    getConsumerGroupStreamClientMap() {
        return __awaiter(this, void 0, void 0, function* () {
            return this.consumerGroupStreamClients;
        });
    }
    addProducerStreamClient(producerStreamId, options, autoReconnectCount) {
        return __awaiter(this, void 0, void 0, function* () {
            yield this.checkExists(this.producerStreamClients, producerStreamId, 'ProducerStream');
            const producerStreamClient = new ProducerStreamClient_1.ProducerStreamClient(options, autoReconnectCount);
            this.producerStreamClients.set(producerStreamId, producerStreamClient);
        });
    }
    getProducerStreamClient(producerStreamId) {
        return __awaiter(this, void 0, void 0, function* () {
            return this.getMapItem(this.producerStreamClients, producerStreamId);
        });
    }
    getProducerStreamClientMap() {
        return __awaiter(this, void 0, void 0, function* () {
            return this.producerStreamClients;
        });
    }
    addOffsetClient(offsetId, options) {
        return __awaiter(this, void 0, void 0, function* () {
            yield this.checkExists(this.offsetClients, offsetId, 'Offset');
            const offsetClient = new OffsetClient_1.OffsetClient(options);
            this.offsetClients.set(offsetId, offsetClient);
        });
    }
    getOffsetClient(offsetId) {
        return __awaiter(this, void 0, void 0, function* () {
            return this.getMapItem(this.offsetClients, offsetId);
        });
    }
    getOffsetClientMap() {
        return __awaiter(this, void 0, void 0, function* () {
            return this.offsetClients;
        });
    }
    addAdminClient(adminId, options) {
        return __awaiter(this, void 0, void 0, function* () {
            yield this.checkExists(this.adminClients, adminId, 'Admin');
            const adminClient = new AdminClient_1.AdminClient(this.clientInstance);
            this.adminClients.set(adminId, adminClient);
        });
    }
    getAdminClient(adminId) {
        return __awaiter(this, void 0, void 0, function* () {
            return this.getMapItem(this.adminClients, adminId);
        });
    }
    getAdminClientMap() {
        return __awaiter(this, void 0, void 0, function* () {
            return this.adminClients;
        });
    }
    // producer method as follows:
    sendProducerMessage(producerId, payloads) {
        return __awaiter(this, void 0, void 0, function* () {
            const producerClient = yield this.getProducerClient(producerId);
            if (!producerClient || !(producerClient instanceof ProducerClient_1.ProducerClient)) {
                throw new Error('producer not exists. you must add a producer...');
            }
            return producerClient.send(payloads);
        });
    }
    createProducerTopics(producerId, topics, async) {
        return __awaiter(this, void 0, void 0, function* () {
            const producerClient = yield this.getProducerClient(producerId);
            if (!producerClient || !(producerClient instanceof ProducerClient_1.ProducerClient)) {
                throw new Error('producer not exists. you must add a producer...');
            }
            return producerClient.createTopics(topics, async);
        });
    }
    // consumer method as follows:
    pauseConsumer(consumerId) {
        return __awaiter(this, void 0, void 0, function* () {
            const consumerClient = yield this.getConsumerClientInstance(consumerId);
            return consumerClient.pause();
        });
    }
    closeConsumer(consumerId, force) {
        return __awaiter(this, void 0, void 0, function* () {
            const consumerClient = yield this.getConsumerClientInstance(consumerId);
            return consumerClient.close(force);
        });
    }
    resumeConsumer(consumerId) {
        return __awaiter(this, void 0, void 0, function* () {
            const consumerClient = yield this.getConsumerClientInstance(consumerId);
            return consumerClient.resume();
        });
    }
    commitConsumer(consumerId, force) {
        return __awaiter(this, void 0, void 0, function* () {
            const consumerClient = yield this.getConsumerClientInstance(consumerId);
            return consumerClient.commit(force);
        });
    }
    addConsumerTopics(consumerId, topics, fromOffset) {
        return __awaiter(this, void 0, void 0, function* () {
            const consumerClient = yield this.getConsumerClientInstance(consumerId);
            return consumerClient.addTopics(topics, fromOffset);
        });
    }
    removeConsumerTopics(consumerId, topics) {
        return __awaiter(this, void 0, void 0, function* () {
            const consumerClient = yield this.getConsumerClientInstance(consumerId);
            return consumerClient.removeTopics(topics);
        });
    }
    setConsumerOffset(consumerId, topic, partition, offset) {
        return __awaiter(this, void 0, void 0, function* () {
            const consumerClient = yield this.getConsumerClientInstance(consumerId);
            return consumerClient.setOffset(topic, partition, offset);
        });
    }
    pauseConsumerTopic(consumerId, topics) {
        return __awaiter(this, void 0, void 0, function* () {
            const consumerClient = yield this.getConsumerClientInstance(consumerId);
            return consumerClient.pauseTopic(topics);
        });
    }
    resumeConsumerTopics(consumerId, topics) {
        return __awaiter(this, void 0, void 0, function* () {
            const consumerClient = yield this.getConsumerClientInstance(consumerId);
            return consumerClient.resumeTopics(topics);
        });
    }
    consumeMessage(consumerId, handler) {
        return __awaiter(this, void 0, void 0, function* () {
            const consumerClient = yield this.getConsumerClientInstance(consumerId);
            return consumerClient.consumeMessage(handler);
        });
    }
    consumeOffsetOutOfRange(consumerId, offsetHandler) {
        return __awaiter(this, void 0, void 0, function* () {
            const consumerClient = yield this.getConsumerClientInstance(consumerId);
            return consumerClient.consumeOffsetOutOfRange(offsetHandler);
        });
    }
    handleConsumerError(consumerId, errorHandler) {
        return __awaiter(this, void 0, void 0, function* () {
            const consumerClient = yield this.getConsumerClientInstance(consumerId);
            return consumerClient.handleError(errorHandler);
        });
    }
    // consumerGroup method as follows:
    sendConsumerGroupOffsetCommitRequest(groupId, commits) {
        return __awaiter(this, void 0, void 0, function* () {
            const consumerGroupClient = yield this.getConsumerGroupStreamClient(groupId);
            return consumerGroupClient.sendOffsetCommitRequest(commits);
        });
    }
    addConsumerGroupTopics(groupId, topics) {
        return __awaiter(this, void 0, void 0, function* () {
            const consumerGroupClient = yield this.getConsumerGroupStreamClient(groupId);
            return consumerGroupClient.addTopics(topics);
        });
    }
    scheduleConsumerGroupReconnect(timeout) {
        return __awaiter(this, void 0, void 0, function* () {
            const consumerGroupClient = yield this.getConsumerGroupStreamClient(groupId);
            return consumerGroupClient.scheduleReconnect(timeout);
        });
    }
    // producerStream method as follows:
    setProducerTransform(transformName, transFormOptions) {
        return __awaiter(this, void 0, void 0, function* () {
            yield this.checkExists(this.producerStreamTransforms, transformName, 'ProducerStreamTransform');
            const transform = new stream_1.Transform(transFormOptions);
            this.producerStreamTransforms.set(transformName, transform);
        });
    }
    updateProducerTransform(transformName, transFormOptions) {
        return __awaiter(this, void 0, void 0, function* () {
            const transform = new stream_1.Transform(transFormOptions);
            this.producerStreamTransforms.set(transformName, transform);
        });
    }
    deleteProducerTransform(transformName) {
        return __awaiter(this, void 0, void 0, function* () {
            return this.deleteMapItem(this.producerStreamTransforms, transformName);
        });
    }
    sendStream(streamSource, producerId, transformName = producerId) {
        return __awaiter(this, void 0, void 0, function* () {
            if (!streamSource) {
                throw new Error('streamSource not exists...');
            }
            const transform = yield this.getMapItem(this.producerStreamTransforms, transformName);
            const producerStreamClient = yield this.getProducerStreamClient(producerId);
            const producerStream = yield producerStreamClient.getProducerStream();
            if (!producerStream) {
                throw new Error(`[producerStream:${producerId} not exists.`);
            }
            if (!transform) {
                throw new Error(`[producerStream:${transformName}] transform exists.`);
            }
            return streamSource.pipe(transform).pipe(producerStream);
        });
    }
    // consumerStream method as follows:
    createConsumerCommitStream(consumerStreamId, options) {
        return __awaiter(this, void 0, void 0, function* () {
            const consumerStreamClient = yield this.getMapItem(this.consumerStreamClients, consumerStreamId);
            return consumerStreamClient.createCommitStream(options);
        });
    }
    consumeStreamMessage(consumerStreamId, handler) {
        return __awaiter(this, void 0, void 0, function* () {
            const consumerStreamClient = yield this.getMapItem(this.consumerStreamClients, consumerStreamId);
            return consumerStreamClient.consumeStreamMessage(handler);
        });
    }
    // consumerGroupStream method as follows:
    consumeGroupStream(groupStreamId, destStream, transformName = groupStreamId) {
        return __awaiter(this, void 0, void 0, function* () {
            if (!destStream) {
                throw new Error('destStream not exists...');
            }
            const transform = yield this.getMapItem(this.consumerGroupStreamTransforms, transformName);
            const consumerGroupStreamClient = yield this.getConsumerGroupStreamClient(groupStreamId);
            const consumerGroupStream = yield consumerGroupStreamClient.getConsumerGroupStream();
            if (!consumerGroupStream) {
                throw new Error(`[consumerGroupStream:${groupStreamId} not exists.`);
            }
            if (!transform) {
                throw new Error(`[consumerGroupStream transform:${transformName}] not exists.`);
            }
            return consumerGroupStream.pipe(transform).pipe(destStream);
        });
    }
    // offset method as follows:
    offsetClose(offsetId) {
        return __awaiter(this, void 0, void 0, function* () {
            const offsetClient = yield this.getOffsetClient(offsetId);
            return offsetClient.close();
        });
    }
    offsetFetch(offsetId, payloads) {
        return __awaiter(this, void 0, void 0, function* () {
            const offsetClient = yield this.getOffsetClient(offsetId);
            return offsetClient.fetch(payloads);
        });
    }
    offsetCommit(offsetId, groupId, payloads) {
        return __awaiter(this, void 0, void 0, function* () {
            const offsetClient = yield this.getOffsetClient(offsetId);
            return offsetClient.commit(groupId, payloads);
        });
    }
    offsetFetchCommits(offsetId, groupId, payloads) {
        return __awaiter(this, void 0, void 0, function* () {
            const offsetClient = yield this.getOffsetClient(offsetId);
            return offsetClient.fetchCommits(groupId, payloads);
        });
    }
    fetchLatestOffsets(offsetId, topics) {
        return __awaiter(this, void 0, void 0, function* () {
            const offsetClient = yield this.getOffsetClient(offsetId);
            return offsetClient.fetchLatestOffsets(topics);
        });
    }
    fetchEarliestOffsets(offsetId, topics) {
        return __awaiter(this, void 0, void 0, function* () {
            const offsetClient = yield this.getOffsetClient(offsetId);
            return offsetClient.fetchEarliestOffsets(topics);
        });
    }
    // Admin method as follows:
    listGroups(adminId) {
        return __awaiter(this, void 0, void 0, function* () {
            const adminClient = yield this.getAdminClient(adminId);
            return adminClient.listGroups();
        });
    }
    describeGroups(adminId, consumerGroups) {
        return __awaiter(this, void 0, void 0, function* () {
            const adminClient = yield this.getAdminClient(adminId);
            return adminClient.describeGroups(consumerGroups);
        });
    }
    // private functions as follows:
    getConsumerClientInstance(consumerId) {
        return __awaiter(this, void 0, void 0, function* () {
            const consumerClient = yield this.getConsumerClient(consumerId);
            if (!consumerClient || !(consumerClient instanceof ConsumerClient_1.ConsumerClient)) {
                throw new Error('consumer not exists. you must add a consumer...');
            }
            return consumerClient;
        });
    }
    checkExists(mapInstance, primaryId, entityType) {
        return __awaiter(this, void 0, void 0, function* () {
            yield this.checkMapInstanceExists(mapInstance);
            if (mapInstance.has(primaryId)) {
                debug(`add ${entityType} request refused. ${entityType}: ${primaryId} exists...`);
                throw new Error(`${entityType}: ${primaryId} exists...`);
            }
            return;
        });
    }
    getMapItem(mapInstance, primaryId) {
        return __awaiter(this, void 0, void 0, function* () {
            yield this.checkMapInstanceExists(mapInstance);
            if (mapInstance.has(primaryId)) {
                return mapInstance.get(primaryId);
            }
            return null;
        });
    }
    deleteMapItem(mapInstance, primaryId) {
        return __awaiter(this, void 0, void 0, function* () {
            if (mapInstance.has(primaryId)) {
                return mapInstance.delete(primaryId);
            }
            return false;
        });
    }
    checkMapInstanceExists(mapInstance) {
        return __awaiter(this, void 0, void 0, function* () {
            if (!mapInstance && mapInstance instanceof Map) {
                throw new Error('mapInstance must be map instance...');
            }
            return;
        });
    }
}
exports.Client = Client;
//# sourceMappingURL=Client.js.map