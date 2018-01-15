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
const debug_1 = require("debug");
const ConsumerClient_1 = require("./ConsumerClient");
const helper_1 = require("./helper");
const log = debug_1.default('coupler:kafka-mq:ConsumerGroupClient');
class ConsumerGroupClient extends ConsumerClient_1.ConsumerClient {
    constructor(options, topics) {
        super(options);
        this.closing = false;
        this.connecting = false;
        this.connected = false;
        this.reconnecting = false;
        this.consumerInstance = null;
        this.init(options, topics);
    }
    connect() {
        return __awaiter(this, void 0, void 0, function* () {
            if (this.connecting) {
                log('connect request ignored. ConsumerGroupClient is currently connecting!!!');
                return;
            }
            this.connecting = true;
            try {
                this.consumerInstance = yield this.onConnected();
                log('ConsumerGroupClient onConnected!!!');
                this.connected = true;
            }
            catch (err) {
                log('ConsumerGroupClient connect on failed!!!');
                log(`failed err = ${JSON.stringify(err)}`);
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
            this.consumerInstance = null;
            const consumerGroup = new Kafka.ConsumerGroup(this.options, this.topics);
            return new Promise((resolve, reject) => {
                consumerGroup.on('connect', () => {
                    if (!consumerGroup) {
                        return reject('consumerGroup is null!!!');
                    }
                    resolve(consumerGroup);
                });
            });
        });
    }
    // todo 这里需要进一步处理，
    scheduleReconnect(timeout) {
        return __awaiter(this, void 0, void 0, function* () {
            while (this.connecting) {
                log('ConsumerGroupClient is connecting!');
            }
            if (!this.connected) {
                throw new Error('ConsumerGroupClient be disconnected!!!');
            }
            return this.consumerInstance.scheduleReconnect(timeout);
        });
    }
    sendOffsetCommitRequest(commits) {
        return __awaiter(this, void 0, void 0, function* () {
            if (!this.connected) {
                throw new Error('ConsumerGroupClient be disconnected!!!');
            }
            return helper_1.promiseFn(this.consumerInstance.sendOffsetCommitRequest, this.consumerInstance)(commits);
        });
    }
    addTopics(topics) {
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
                log('ConsumerClient no topic added due to no topics!');
                return null;
            }
            return new Promise((resolve, reject) => {
                this.consumerInstance.addTopics(topicArr, (err, addedTopics) => {
                    if (err) {
                        return reject(err);
                    }
                    return resolve(addedTopics);
                });
            });
        });
    }
    pauseTopic(topics) {
        return __awaiter(this, void 0, void 0, function* () {
            return;
        });
    }
    resumeTopics(topics) {
        return __awaiter(this, void 0, void 0, function* () {
            return;
        });
    }
    init(options, topics) {
        this.options = options;
        this.topics = topics;
        // this.connect();
    }
}
exports.ConsumerGroupClient = ConsumerGroupClient;
//# sourceMappingURL=ConsumerGroupClient.js.map