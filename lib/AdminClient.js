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
const es6_promise_1 = require("es6-promise");
const debug = Debug('coupler:kafka-mq:AdminClient');
class AdminClient {
    constructor(client) {
        this.closing = false;
        this.connecting = false;
        this.connected = false;
        this.adminInstance = null;
        if (!client) {
            throw new Error('kafka-mq client is not exists...');
        }
        this.kafkaClient = client;
    }
    connect() {
        return __awaiter(this, void 0, void 0, function* () {
            if (this.connecting) {
                debug('connect request ignored. AdminClient is currently connecting...');
                return;
            }
            this.connecting = true;
            debug('AdminClient start to connect...');
            try {
                this.adminInstance = yield this.onConnected();
                debug('AdminClient onConnected!!!');
                this.connected = true;
            }
            catch (err) {
                debug('AdminClient connect failed...');
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
            if (this.connected && this.adminInstance) {
                return;
            }
            this.connected = false;
            this.adminInstance = null;
            const adminInstance = new Kafka.Admin(this.kafkaClient, this.topics, this.options);
            return new es6_promise_1.Promise((resolve, reject) => {
                if (!adminInstance) {
                    return reject('adminInstance instance is null!');
                }
                adminInstance.on('readable', () => resolve(adminInstance));
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
                this.adminInstance = null;
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
            if (!this.connected || !this.adminInstance) {
                debug('close request refused. client has been closed currently.');
                return null;
            }
            return new es6_promise_1.Promise((resolve, reject) => {
                this.adminInstance.close(force, resolve);
            });
        });
    }
    listGroups() {
        return __awaiter(this, void 0, void 0, function* () {
            if (!this.connected || !this.adminInstance) {
                this.connected = false;
                yield this.connect();
            }
            return new es6_promise_1.Promise((resolve, reject) => {
                this.adminInstance.listGroups(resolve);
            });
        });
    }
    describeGroups(consumerGroups) {
        return __awaiter(this, void 0, void 0, function* () {
            if (!this.connected || !this.adminInstance) {
                this.connected = false;
                yield this.connect();
            }
            return new es6_promise_1.Promise((resolve, reject) => {
                this.adminInstance.describeGroups(consumerGroups, resolve);
            });
        });
    }
}
exports.AdminClient = AdminClient;
//# sourceMappingURL=AdminClient.js.map