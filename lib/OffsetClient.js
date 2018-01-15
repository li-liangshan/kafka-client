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
const _ = require("lodash");
const helper_1 = require("./helper");
const debug = Debug('coupler:kafka-mq:OffsetClient');
class OffsetClient {
    constructor(options) {
        this.closing = false;
        this.connecting = false;
        this.connected = false;
        this.offsetInstance = null;
        const { client, autoReconnectCount = 0 } = options;
        if (!client) {
            throw new Error('client can not be null or undefined!');
        }
        this.kafkaClient = options.client;
        this.autoReconnectCount = autoReconnectCount;
    }
    connect() {
        return __awaiter(this, void 0, void 0, function* () {
            if (this.connecting) {
                debug('connect request ignored. KafkaClient is currently connecting!');
                return;
            }
            this.connecting = true;
            try {
                this.offsetInstance = yield this.onConnected();
                this.connecting = false;
                this.connected = true;
            }
            catch (err) {
                debug('OffsetClient connect failed!!!');
                debug(`error ==> ${JSON.stringify(err)}`);
                this.connecting = false;
                yield this.onReconnecting();
            }
            finally {
                this.connecting = false;
            }
        });
    }
    onConnected() {
        return __awaiter(this, void 0, void 0, function* () {
            if (this.connected && this.offsetInstance) {
                return this.offsetInstance;
            }
            this.connected = false;
            const offsetClient = new Kafka.Offset(this.kafkaClient);
            return new Promise((resolve, reject) => {
                if (!offsetClient) {
                    return reject('new Kafka.Offset instance failed!');
                }
                offsetClient.on('ready', () => resolve(offsetClient));
            });
        });
    }
    onReconnecting() {
        return __awaiter(this, void 0, void 0, function* () {
            if (this.connecting) {
                debug('reconnect request ignored. OffsetClient is currently reconnecting...');
                return;
            }
            if (this.connected && this.offsetInstance) {
                debug('reconnect refused. OffsetClient have been connected..');
                return;
            }
            if (this.autoReconnectCount <= 0) {
                debug('OffsetClient can not reconnect.');
                return;
            }
            debug('start reconnecting [OffsetClient]...');
            this.autoReconnectCount -= 1;
            return this.connect();
        });
    }
    // todo
    close() {
        return __awaiter(this, void 0, void 0, function* () {
            if (this.closing) {
                debug('close request ignored. OffsetClient is currently closing...');
                return;
            }
            this.closing = true;
            try {
                const result = yield this.onClosed();
                this.connected = false;
                this.closing = false;
                return result;
            }
            catch (err) {
                this.closing = false;
                debug(`close ConsumerClient failed; err => ${JSON.stringify(err)}`);
                return Promise.reject(err);
            }
        });
    }
    // todo
    onClosed() {
        return __awaiter(this, void 0, void 0, function* () {
            this.offsetInstance = null;
            if (!this.connected) {
                return true;
            }
            return true;
        });
    }
    fetch(payloads) {
        return __awaiter(this, void 0, void 0, function* () {
            debug(`OffsetClient fetching and payloads =${JSON.stringify(payloads)}`);
            yield this.checkOrConnectClient();
            return new Promise((resolve, reject) => {
                this.offsetInstance.fetch(payloads, (err, data) => {
                    if (err) {
                        return reject(err);
                    }
                    return resolve(data);
                });
            });
        });
    }
    commit(groupId, payloads) {
        return __awaiter(this, void 0, void 0, function* () {
            debug(`OffsetClient committing and groupId =${groupId}, payloads =${JSON.stringify(payloads)}`);
            yield this.checkOrConnectClient();
            return new Promise((resolve, reject) => {
                this.offsetInstance.commit(groupId, payloads, (err, data) => {
                    if (!err) {
                        return reject(err);
                    }
                    return resolve(data);
                });
            });
        });
    }
    fetchCommits(groupId, payloads) {
        return __awaiter(this, void 0, void 0, function* () {
            debug(`OffsetClient fetching of commits and groupId=${groupId}, payloads=${JSON.stringify(payloads)}`);
            yield this.checkOrConnectClient();
            return new Promise((resolve, reject) => {
                this.offsetInstance.fetchCommits(groupId, payloads, (err, data) => {
                    if (!err) {
                        return reject(err);
                    }
                    return resolve(data);
                });
            });
        });
    }
    fetchLatestOffsets(topics) {
        return __awaiter(this, void 0, void 0, function* () {
            debug(`OffsetClient fetching latest offsets for topics=${JSON.stringify(topics)}}`);
            yield this.checkOrConnectClient();
            const topicArr = yield this.parseTopics(topics);
            return new Promise((resolve, reject) => {
                this.offsetInstance.fetchLatestOffsets(topicArr, (err, offsets) => {
                    if (err) {
                        return reject(err);
                    }
                    return resolve(offsets);
                });
            });
        });
    }
    fetchEarliestOffsets(topics) {
        return __awaiter(this, void 0, void 0, function* () {
            debug(`OffsetClient fetching earliest offsets for topics=${JSON.stringify(topics)}}`);
            yield this.checkOrConnectClient();
            const topicArr = yield this.parseTopics(topics);
            return new Promise((resolve, reject) => {
                this.offsetInstance.fetchEarliestOffsets(topicArr, (err, offsets) => {
                    if (err) {
                        return reject(err);
                    }
                    return resolve(offsets);
                });
            });
        });
    }
    parseTopics(topics) {
        return __awaiter(this, void 0, void 0, function* () {
            const topicArr = yield helper_1.generateTrimStringArray(topics);
            if (!topicArr.length) {
                throw new Error('topics must not be null,"", " " or [] and so on...');
            }
            if (_.isArray(topics) && topics.length !== topicArr.length) {
                throw new Error('topics must not be [" ", " "], ["", " "]. and so on...');
            }
            return topicArr;
        });
    }
    checkOrConnectClient() {
        return __awaiter(this, void 0, void 0, function* () {
            if (this.connected && this.offsetInstance) {
                return;
            }
            debug('OffsetClient disconnected now start connecting....');
            yield this.connect();
        });
    }
}
exports.OffsetClient = OffsetClient;
//# sourceMappingURL=OffsetClient.js.map