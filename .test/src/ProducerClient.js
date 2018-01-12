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
const debug = Debug('coupler:kafka-mq:ProducerClient');
class ProducerClient {
    constructor(options, isHighLevel = false, autoReconnect = false) {
        this.closing = false;
        this.connecting = false;
        this.connected = false;
        this.init(options, isHighLevel, autoReconnect);
    }
    connect() {
        return __awaiter(this, void 0, void 0, function* () {
            if (this.connecting) {
                debug('connect request ignored. ProducerClient is currently connecting!');
                return;
            }
            this.connecting = true;
            debug(`${this.isHighLevel ? 'HighLevelProducer' : 'Producer'} connecting...`);
            try {
                this.producerInstance = yield this.onConnected();
                debug(`${this.isHighLevel ? 'HighLevelProducer' : 'Producer'} onConnected!`);
                this.connected = true;
            }
            catch (err) {
                debug(`${this.isHighLevel ? 'HighLevelProducer' : 'Producer'} connect failed!`);
                debug(`failed err ==> ${JSON.stringify(err)}`);
                this.connecting = false;
                yield this.onReconnecting();
            }
            finally {
                this.connecting = false;
            }
        });
    }
    onReconnecting() {
        return __awaiter(this, void 0, void 0, function* () {
            if (!this.autoReconnect) {
                return;
            }
            if (this.connecting) {
                debug('reconnect request ignored. ProducerClient is currently reconnecting!');
                return;
            }
            debug('start reconnecting [ProducerClient]!!!');
            return this.connect();
        });
    }
    onConnected() {
        return __awaiter(this, void 0, void 0, function* () {
            if (this.connected && this.producerInstance) {
                return this.producerInstance;
            }
            this.connected = false;
            const KafkaProducer = this.isHighLevel ? Kafka.HighLevelProducer : Kafka.Producer;
            const producer = new KafkaProducer(this.kafkaClient, this.options.producerOptions, this.options.partitioner);
            return new Promise((resolve, reject) => {
                producer.on('ready', () => {
                    if (!producer) {
                        return reject('producer instance not exits...');
                    }
                    resolve(producer);
                });
            });
        });
    }
    close() {
        return __awaiter(this, void 0, void 0, function* () {
            if (this.closing) {
                return;
            }
            this.closing = true;
            try {
                yield this.onClosed();
                this.producerInstance = null;
                this.connected = false;
            }
            catch (err) {
                debug(`close ProducerClient failed; err => ${JSON.stringify(err)}`);
            }
            finally {
                this.closing = false;
            }
        });
    }
    onClosed() {
        return __awaiter(this, void 0, void 0, function* () {
            if (!this.connected || !this.producerInstance) {
                return null;
            }
            return new Promise((resolve, reject) => {
                this.producerInstance.close(resolve);
            });
        });
    }
    createTopics(topics, async = true) {
        return __awaiter(this, void 0, void 0, function* () {
            if (!this.connected) {
                yield this.connect();
            }
            return new Promise((resolve, reject) => {
                this.producerInstance.createTopics(topics, async, (err, topicNames) => {
                    if (err) {
                        return reject(err);
                    }
                    return resolve(topicNames);
                });
            });
        });
    }
    send(payloads) {
        return __awaiter(this, void 0, void 0, function* () {
            if (!this.connected) {
                yield this.connect();
            }
            debug(`producerClient is sending message = ${JSON.stringify(payloads)}`);
            return new Promise((resolve, reject) => {
                this.producerInstance.send(payloads, (err, data) => {
                    if (err) {
                        return reject(err);
                    }
                    return resolve(data);
                });
            });
        });
    }
    isConnected() {
        return this.connected;
    }
    init(options, isHighLevel, autoReconnect) {
        this.options = options;
        this.kafkaClient = options.client;
        this.isHighLevel = isHighLevel;
        this.autoReconnect = autoReconnect;
    }
}
exports.ProducerClient = ProducerClient;
//# sourceMappingURL=ProducerClient.js.map