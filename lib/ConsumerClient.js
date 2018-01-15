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
const lodash_1 = require("lodash");
const helper_1 = require("./helper");
const debug = Debug('LLS:testSpec');
/****************************************************************
 *** HighLevelConsumer has been deprecated in the latest version
 *** of Kafka (0.10.1) and is likely to be removed in the future.
 *** Please use the ConsumerGroup instead
 ****************************************************************/
class ConsumerClient {
    constructor(options, isHighLevel, autoReconnect) {
        this.closing = false;
        this.connecting = false;
        this.connected = false;
        this.consumerInstance = null;
        this.options = options;
        this.kafkaClient = options.client;
        this.isHighLevel = isHighLevel || false;
        this.autoReconnect = autoReconnect || false;
        // this.connect();
    }
    connect() {
        return __awaiter(this, void 0, void 0, function* () {
            if (this.connecting) {
                debug('connect request ignored. ConsumerClient is currently connecting!');
                return;
            }
            this.connecting = true;
            debug(`${this.isHighLevel ? 'HighLevelConsumer' : 'Consumer'} connecting...`);
            try {
                this.consumerInstance = yield this.onConnected();
                debug(`${this.isHighLevel ? 'HighLevelConsumer' : 'Consumer'} onConnected!`);
                this.connected = true;
                this.connecting = false;
            }
            catch (err) {
                debug(`${this.isHighLevel ? 'HighLevelProducer' : 'Producer'} connect failed!!!`);
                debug(`failed err ==> ${JSON.stringify(err)}`);
                this.connecting = false;
                yield this.onReconnecting();
            }
        });
    }
    onConnected() {
        return __awaiter(this, void 0, void 0, function* () {
            if (this.connected && this.consumerInstance) {
                return this.consumerInstance;
            }
            this.connected = false;
            const KafkaConsumer = this.isHighLevel ? Kafka.HighLevelConsumer : Kafka.Consumer;
            const consumer = new KafkaConsumer(this.kafkaClient, this.options.topics, this.options.options);
            return consumer ? consumer : Promise.reject('consumer is null!!!');
        });
    }
    onReconnecting() {
        return __awaiter(this, void 0, void 0, function* () {
            if (this.connecting) {
                debug('reconnect request ignored. ConsumerClient is currently reconnecting!');
                return;
            }
            if (this.connected) {
                debug('reconnect request ignored. ConsumerClient have been connected!');
                return;
            }
            if (!this.autoReconnect) {
                return;
            }
            debug('start reconnecting [ConsumerClient]!!!');
            return this.connect();
        });
    }
    isConnected() {
        return this.connected;
    }
    close(force = false) {
        return __awaiter(this, void 0, void 0, function* () {
            if (this.closing) {
                return;
            }
            this.closing = true;
            try {
                const result = yield this.onClosed(force);
                this.consumerInstance = null;
                this.connected = false;
                this.closing = false;
                return result;
            }
            catch (err) {
                this.closing = false;
                debug(`close ConsumerClient failed; err => ${JSON.stringify(err)}`);
                throw err;
            }
        });
    }
    onClosed(force = false) {
        return __awaiter(this, void 0, void 0, function* () {
            if (!this.connected || !this.consumerInstance) {
                return null;
            }
            return helper_1.promiseFn(this.consumerInstance.close, this.consumerInstance)(force);
        });
    }
    pause() {
        return __awaiter(this, void 0, void 0, function* () {
            if (!this.connected) {
                yield this.connect();
            }
            return this.consumerInstance.pause();
        });
    }
    resume() {
        return __awaiter(this, void 0, void 0, function* () {
            if (!this.connected) {
                yield this.connect();
            }
            return this.consumerInstance.resume();
        });
    }
    commit(force = false) {
        return __awaiter(this, void 0, void 0, function* () {
            if (!this.connected) {
                yield this.connect();
            }
            return helper_1.promiseFn(this.consumerInstance.commit, this.consumerInstance)(force);
            // return new Promise((resolve, reject) => {
            //   this.consumerInstance.commit(force, (err, data) => {
            //     if (err) {
            //       return reject(err);
            //     }
            //     return resolve(data);
            //   });
            // });
        });
    }
    addTopics(topics, fromOffset = false) {
        return __awaiter(this, void 0, void 0, function* () {
            if (!topics) {
                return null;
            }
            if (typeof topics === 'string' && !Boolean(topics.trim())) {
                return null;
            }
            if (!this.connected) {
                yield this.connect();
            }
            const topicArr = typeof topics === 'string' ? [topics] : topics.filter((topic) => topic.trim());
            if (!topicArr.length) {
                debug('ConsumerClient no topic added due to no topics!');
                return null;
            }
            return new Promise((resolve, reject) => {
                this.consumerInstance.addTopics(topicArr, (err, addedTopics) => {
                    if (err) {
                        return reject(err);
                    }
                    return resolve(addedTopics);
                }, fromOffset);
            });
        });
    }
    removeTopics(topics) {
        return __awaiter(this, void 0, void 0, function* () {
            if (!this.connected || !this.consumerInstance) {
                debug('ConsumerClient disConnected, not topics can be removed!');
                return null;
            }
            if (!topics.length) {
                debug('ConsumerClient no topic removed due to no topics!');
                return null;
            }
            // return new Promise((resolve, reject) => {
            //   this.consumerInstance.removeTopics(topics, (err, removedTopics) => {
            //     if (err) {
            //       return reject(err);
            //     }
            //     return resolve(removedTopics);
            //   });
            // });
            return helper_1.promiseFn(this.consumerInstance.removeTopics, this.consumerInstance)(topics);
        });
    }
    setOffset(topic, partition = 0, offset = 0) {
        return __awaiter(this, void 0, void 0, function* () {
            if (!this.connected) {
                yield this.connect();
            }
            if (!topic.trim()) {
                debug('ConsumerClient no topic removed due to no topics!');
                throw new Error('topic can not be \' \'');
            }
            return this.consumerInstance.setOffset(topic, partition, offset);
        });
    }
    pauseTopics(topics) {
        return __awaiter(this, void 0, void 0, function* () {
            if (!this.connected) {
                yield this.connect();
            }
            return this.consumerInstance.pauseTopics(topics);
        });
    }
    resumeTopics(topics) {
        return __awaiter(this, void 0, void 0, function* () {
            if (!this.connected) {
                yield this.connect();
            }
            return this.consumerInstance.resumeTopics(topics);
        });
    }
    getTopicPayloads() {
        return __awaiter(this, void 0, void 0, function* () {
            if (!this.connected) {
                yield this.connect();
            }
            if (!this.isHighLevel) {
                return this.consumerInstance.payloads;
            }
            return this.consumerInstance.getTopicPayloads();
        });
    }
    consumeMessage(handler) {
        return __awaiter(this, void 0, void 0, function* () {
            if (!this.connected) {
                yield this.connect();
            }
            return this.consumerInstance.on('message', (message) => this.createMessageHandler(handler));
        });
    }
    handleError(errorHandler) {
        return __awaiter(this, void 0, void 0, function* () {
            if (!this.connected) {
                yield this.connect();
            }
            return this.consumerInstance.on('error', (error) => this.createErrorHandler(errorHandler));
        });
    }
    consumeOffsetOutOfRange(offsetHandler) {
        return __awaiter(this, void 0, void 0, function* () {
            if (!this.connected) {
                yield this.connect();
            }
            return this.consumerInstance.on('offsetOutOfRange', (err) => this.createOffsetOutOfRange(err));
        });
    }
    getConsumer() {
        return __awaiter(this, void 0, void 0, function* () {
            if (!this.connected) {
                yield this.connect();
            }
            return this.consumerInstance;
        });
    }
    createMessageHandler(handler) {
        return __awaiter(this, void 0, void 0, function* () {
            debug('start to create messageHandler!');
            return (message) => this.onMessage(message, handler);
        });
    }
    createErrorHandler(errorHandler) {
        return __awaiter(this, void 0, void 0, function* () {
            debug('start to create errorHandler!');
            return (error) => this.onError(error, errorHandler);
        });
    }
    createOffsetOutOfRange(offsetHandler) {
        return __awaiter(this, void 0, void 0, function* () {
            debug('start to create offsetOutOfRangeHandler!');
            return (err) => this.onOffsetOutOfRange(err, offsetHandler);
        });
    }
    onMessage(message, handler) {
        debug(`message is being handled! message=>${JSON.stringify(message)}`);
        if (!message) {
            return;
        }
        if (!lodash_1.isFunction(handler)) {
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
    onError(error, handler) {
        debug(`consumerClient error is being handled! error=>${JSON.stringify(error)}`);
        if (!error) {
            return;
        }
        if (!lodash_1.isFunction(handler)) {
            debug('message omitted due to no errorHandler!!!');
            return;
        }
        try {
            handler(error);
        }
        catch (err) {
            debug(`consumerClient handle onError failed due to err => ${JSON.stringify(err)}`);
        }
    }
    onOffsetOutOfRange(error, offsetHandler) {
        if (!error) {
            debug('nothing to do due to no err!!!');
            return;
        }
        if (!lodash_1.isFunction(offsetHandler)) {
            debug('nothing to do due to no offsetHandler!!!');
            return;
        }
        try {
            offsetHandler(error);
        }
        catch (err) {
            debug(`handle offsetOutOfRange topic failed due to err => ${JSON.stringify(err)}`);
        }
    }
}
exports.ConsumerClient = ConsumerClient;
//# sourceMappingURL=ConsumerClient.js.map