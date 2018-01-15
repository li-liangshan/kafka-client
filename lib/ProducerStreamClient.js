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
const debug = Debug('coupler:kafka-mq:ProducerStreamClient');
class ProducerStreamClient {
    constructor(options, autoReconnectCount = 0) {
        this.closing = false;
        this.connecting = false;
        this.connected = false;
        this.streamInstance = null;
        this.options = options;
        this.autoReconnectCount = autoReconnectCount;
    }
    connect() {
        return __awaiter(this, void 0, void 0, function* () {
            if (this.connecting) {
                debug('connect request ignored. ProducerStreamClient is currently connecting...');
                return;
            }
            this.connecting = true;
            try {
                this.streamInstance = yield this.onConnected();
                debug('producerStreamClient onConnected!');
                this.connected = true;
            }
            catch (err) {
                debug('producerStreamClient connect failed!!!');
                debug(`failed err ==> ${JSON.stringify(err)}`);
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
                return this.streamInstance;
            }
            this.connected = false;
            try {
                return new Kafka.ProducerStream(this.options);
            }
            catch (err) {
                throw err;
            }
        });
    }
    onReconnecting() {
        return __awaiter(this, void 0, void 0, function* () {
            if (this.connecting) {
                debug('connect request ignored. ProducerStreamClient is currently connecting...');
                return;
            }
            if (this.connected && this.streamInstance) {
                debug('reconnect refused. ProducerStreamClient has been connected...');
                return;
            }
            if (this.autoReconnectCount <= 0) {
                debug('ProducerStreamClient can not reconnect...');
                return;
            }
            debug('start reconnecting [ProducerStreamClient]...');
            this.autoReconnectCount -= 1;
            return this.connect();
        });
    }
    close() {
        return __awaiter(this, void 0, void 0, function* () {
            if (this.closing) {
                debug('close request ignored. ProducerStreamClient is currently closing...');
                return;
            }
            if (!this.connected || !this.streamInstance) {
                debug('close request refused. ProducerStreamClient has been closed...');
                return;
            }
            this.closing = true;
            try {
                const result = yield this.onClosed();
                this.connected = false;
                this.closing = false;
                this.connecting = false;
                this.streamInstance = null;
                debug('ProducerStreamClient  onClosed...');
                return result;
            }
            catch (err) {
                debug(`ProducerStreamClient close err ==> ${JSON.stringify(err)}`);
                this.closing = false;
                throw err;
            }
        });
    }
    onClosed() {
        return __awaiter(this, void 0, void 0, function* () {
            if (!this.connected || !this.streamInstance) {
                return null;
            }
            return new Promise((resolve, reject) => {
                this.streamInstance.close(resolve);
            });
        });
    }
    getProducerStream() {
        return __awaiter(this, void 0, void 0, function* () {
            if (!this.connected || !this.streamInstance) {
                yield this.connect();
            }
            return this.streamInstance;
        });
    }
    isConnected() {
        return this.connected;
    }
}
exports.ProducerStreamClient = ProducerStreamClient;
//# sourceMappingURL=ProducerStreamClient.js.map