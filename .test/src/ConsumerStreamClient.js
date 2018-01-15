"use strict";
/* *******************************************
 * @Author: liliangshan
 * @Date: 2018-01-14 13:46:19
 * @Last Modified by: liliangshan
 * @Last Modified time: 2018-01-15 14:46:56
 * *******************************************/
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
const debug = Debug('coupler:kafka-mq:ConsumerStreamClient');
class ConsumerStreamClient {
    constructor(client, topics, options) {
        this.closing = false;
        this.connecting = false;
        this.connected = false;
        this.streamInstance = null;
        this.init(client, topics, options);
    }
    connect() {
        return __awaiter(this, void 0, void 0, function* () {
            if (this.connecting) {
                debug('connect request ignored. ConsumerStreamClient is currently connecting...');
                return;
            }
            this.connecting = true;
            debug('ConsumerStreamClient start to connect...');
            try {
                this.streamInstance = yield this.onConnected();
                debug('ConsumerStreamClient onConnected!!!');
                this.connected = true;
            }
            catch (err) {
                debug('ConsumerStreamClient connect failed...');
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
            if (this.connected && this.streamInstance) {
                return;
            }
            this.connected = false;
            this.streamInstance = null;
            const consumerStream = new Kafka.ConsumerStream(this.kafkaClient, this.topics, this.options);
            return new Promise((resolve, reject) => {
                if (!consumerStream) {
                    return reject('consumerStream instance is null!');
                }
                consumerStream.on('readable', () => resolve(consumerStream));
            });
        });
    }
    onReconnecting() {
        return __awaiter(this, void 0, void 0, function* () {
            if (this.connecting) {
                debug('reconnect request ignored. ConsumerStreamClient is currently reconnecting...');
                return;
            }
            if (this.connected) {
                debug('reconnect request ignored. ConsumerStreamClient have been connected!');
                return;
            }
            if (this.autoReconnectCount <= 0) {
                debug('reconnect request refused. current reconnect count <= 0');
                return;
            }
            debug('start reconnecting !!!');
            this.autoReconnectCount -= 1;
            return this.connect();
        });
    }
    close(force = false) {
        return __awaiter(this, void 0, void 0, function* () {
            if (this.closing) {
                debug('close request ignored. client is currently closing.');
                return;
            }
            this.closing = true;
            try {
                const result = yield this.onClosed(force);
                this.connected = false;
                this.closing = false;
                this.connecting = false;
                this.streamInstance = null;
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
    onClosed(force = false) {
        return __awaiter(this, void 0, void 0, function* () {
            if (!this.closing) {
                debug('close request error, maybe onClosed...');
                return null;
            }
            if (!this.connected || !this.streamInstance) {
                debug('close request refused. client has been closed currently.');
                return null;
            }
            return new Promise((resolve, reject) => {
                this.streamInstance.close(force, resolve);
            });
        });
    }
    createCommitStream(options) {
        return __awaiter(this, void 0, void 0, function* () {
            if (!this.connected || !this.streamInstance) {
                this.connected = false;
                this.streamInstance = null;
                yield this.connect();
            }
            debug('create commit stream...');
            return this.streamInstance.createCommitStream(options);
        });
    }
    consumeStreamMessage(handler) {
        return __awaiter(this, void 0, void 0, function* () {
            if (!this.connected || !this.streamInstance) {
                yield this.connect();
            }
            return new Promise((resolve, reject) => {
                this.streamInstance.on('data', (err, message) => {
                    if (err) {
                        return reject(err);
                    }
                    resolve(message);
                });
            });
            // .then((msg) => this.createMessageHandler(handler));
        });
    }
    isConnected() {
        return this.connected;
    }
    getConsumerStream() {
        return __awaiter(this, void 0, void 0, function* () {
            if (!this.connected || !this.streamInstance) {
                yield this.connect();
            }
            return this.streamInstance;
        });
    }
    createMessageHandler(handler) {
        return __awaiter(this, void 0, void 0, function* () {
            debug('start to create messageHandler!');
            return (message) => this.onMessage(message, handler);
        });
    }
    onMessage(message, handler) {
        debug(`message is being handled! message=>${JSON.stringify(message)}`);
        if (!message) {
            return;
        }
        if (!_.isFunction(handler)) {
            debug('message omitted due to no messageHandler!!!');
            return;
        }
        try {
            handler(message);
        }
        catch (err) {
            debug(`handle message failed due to error => ${JSON.stringify(err)}`);
        }
    }
    init(client, topics, options) {
        this.options = options;
        this.autoReconnectCount = options.autoReconnectCount || 0;
        const topicArr = helper_1.generateTrimStringArray(topics);
        if (!topicArr || !topicArr.length) {
            throw new Error('topics must not be null, [], [" "] or ""...');
        }
        this.topics = topicArr;
        if (!client) {
            throw new Error('kafka instance must not be null!');
        }
        this.kafkaClient = client;
    }
}
exports.ConsumerStreamClient = ConsumerStreamClient;
//# sourceMappingURL=ConsumerStreamClient.js.map