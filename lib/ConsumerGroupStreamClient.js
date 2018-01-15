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
const helper_1 = require("./helper");
const debug = Debug('coupler:kafka-mq:ConsumerGroupStreamClient');
class ConsumerGroupStreamClient {
    constructor(options, topics) {
        this.closing = false;
        this.connecting = false;
        this.connected = false;
        this.groupInstance = null;
        this.init(options, topics);
    }
    connect() {
        return __awaiter(this, void 0, void 0, function* () {
            if (this.connecting) {
                debug('connect request ignored. client is currently connecting...');
                return;
            }
            this.connecting = true;
            debug('client start to connect...');
            try {
                this.groupInstance = yield this.onConnected();
                debug('client onConnected!!!');
                this.connected = true;
            }
            catch (err) {
                debug('client connect failed...');
                this.connecting = false;
                yield this.onReconnecting();
            }
            finally {
                this.connecting = false;
            }
        });
    }
    close() {
        return __awaiter(this, void 0, void 0, function* () {
            if (this.closing) {
                debug('close request ignored. client is currently closing...');
                return;
            }
            this.closing = true;
            try {
                const result = yield this.onClosed();
                this.closing = false;
                this.connected = false;
                this.connecting = false;
                this.groupInstance = null;
                return result;
            }
            catch (err) {
                debug(`client close failed err=${JSON.stringify(err)}`);
                this.closing = false;
                throw err;
            }
        });
    }
    getConsumerGroupStream() {
        return __awaiter(this, void 0, void 0, function* () {
            if (!this.connected || !this.groupInstance) {
                yield this.connect();
            }
            return this.groupInstance;
        });
    }
    onConnected() {
        return __awaiter(this, void 0, void 0, function* () {
            if (this.connected && this.groupInstance) {
                return;
            }
            this.connected = false;
            this.groupInstance = null;
            const consumerGroupStream = new Kafka.ConsumerGroupStream(this.options, this.topics);
            return new Promise((resolve, reject) => {
                if (!consumerGroupStream || !consumerGroupStream.consumerGroup) {
                    return reject('consumerGroupStream instance is null!');
                }
                consumerGroupStream.consumerGroup.on('connect', () => resolve(consumerGroupStream));
            });
        });
    }
    onReconnecting() {
        return __awaiter(this, void 0, void 0, function* () {
            if (this.connecting) {
                debug('reconnect request ignored. client is currently reconnecting...');
                return;
            }
            if (this.connected) {
                debug('reconnect request ignored. client has been connected!');
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
    init(options, topics) {
        return __awaiter(this, void 0, void 0, function* () {
            this.options = options;
            this.autoReconnectCount = options.autoReconnectCount || 0;
            const topicArr = helper_1.generateTrimStringArray(topics);
            if (!topicArr || !topicArr.length) {
                throw new Error('topics must not be null, [], [" "] or ""...');
            }
            this.topics = topicArr;
            return this.connect();
        });
    }
    onClosed() {
        return __awaiter(this, void 0, void 0, function* () {
            if (!this.closing) {
                debug('close request error, maybe onClosed...');
                return null;
            }
            if (!this.connected || !this.groupInstance) {
                debug('close request refused. client has been closed currently.');
                return null;
            }
            return new Promise((resolve, reject) => {
                this.groupInstance.close(resolve);
            });
        });
    }
}
exports.ConsumerGroupStreamClient = ConsumerGroupStreamClient;
//# sourceMappingURL=ConsumerGroupStreamClient.js.map