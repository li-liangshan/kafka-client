
import * as Kafka from 'kafka-node';
import debug from 'debug';
import * as _ from 'lodash';
import { Promise } from 'es6-promise';
import { resolve, sep } from 'path';
import { error } from 'util';

const log = debug('coupler:kafka-mq:client');

export interface IKafkaOptions {
  clientId?: string;
  sslOptions?: any;
  ssl?: boolean;

  kafkaHost?: string;
  connectTimeout?: number;
  requestTimeout?: number;
  idleConnection?: number;
  autoConnect?: boolean;
  versions?: {
    disabled?: boolean;
    requestTimeout?: boolean;
  };
  connectRetryOptions?: {
    retries?: number;
    factor?: number;
    minTimeout?: number;
    maxTimeout?: number;
    randomize?: boolean;
  };
  maxAsyncRequests?: number;
}

export interface ISendRequest {
  payloads?: any;
  requireAcks?: number;
  ackTimeoutMs?: number;
}

export interface IFetchRequest {
  consumer: any;
  payloads?: any;
  fetchMaxWaitMs?: number;
  fetchMinBytes?: number;
  maxTickMessages?: number;
}

export class KafkaClient {
  closing: boolean = false;
  connecting: boolean = false;
  connected: boolean = false;
  reconnecting: boolean = false;

  options: IKafkaOptions;
  private clientInstance: any;

  constructor(options: IKafkaOptions) {
    this.options = options;
  }

  async connect() {
    if (this.connecting) {
      log('connect request ignored. KafkaClient is currently connecting!');
      return;
    }

    this.connecting = true;
    await this.onConnecting();
    this.connecting = false;
    this.connected = true;
  }

  async onConnecting(): Promise<any> {
    if (this.connected && this.clientInstance) {
      return this.clientInstance;
    }
    this.clientInstance = new Kafka.KafkaClient(this.options);
    this.clientInstance.on('close', (instance) => this.onClosed(instance));
    this.clientInstance.on('reconnect', () => this.onReconnecting());
    try {
      let isConnected: any = false;
      isConnected = await new Promise((resolve, reject) => {
        this.clientInstance.on('connect', () => resolve(true));
        this.clientInstance.on('error', (error) => reject(error));
      });
      if (isConnected === true) {
       return this.onConnected();
      }
      return this.disConnected('KafkaClient disconnected!');
    } catch (err) {
      return this.disConnected(err);
    }

  }

  async onConnected() {
    log('KafkaClient connected!');
    this.connecting = false;
    this.connected = true;
  }

  async disConnected(error) {
    log(`KafkaClient connecting error: ${JSON.stringify(error)}`);
    this.connecting = false;
    this.connected = false;
    this.clientInstance = null;
  }

  async onReconnecting() {
    this.reconnecting = true;
    this.connecting = true;
    this.connected = false;
    this.closing = false;
  }

  async close() {
    log('close KafkaClient!');
    this.closing = true;
    return new Promise((resolve, reject) => {
      this.clientInstance.close(resolve);
    });
  }
  async onClosed(instance?: any) {
    log('closed KafkaClient!');
    this.closing = false;
    this.connected = false;
  }

  async checkOrConnect(funName: string) {
    log(`check the client Connected`);
    if (!this.connected) {
      log(`KafkaClient is not connected [${funName}]`);
      log('KafkaClient start connecting ....');
      await this.connect();
    }
    log(`KafkaClient connected before ${funName}`);
  }

  async sendProduceRequest(request: ISendRequest) {
    await this.checkOrConnect('sendRequest');
    log('sending request!');
    return new Promise((resolve, reject) => {
      this.clientInstance.sendProduceRequest(
        request.payloads,
        request.requireAcks,
        request.ackTimeoutMs,
        (err) => {
        if (err) {
          return reject(err);
        }
        return resolve();
      });
    });
  }

  async topicExists(topics: string[]) {
    await this.checkOrConnect('topicExists');
    return new Promise((resolve, reject) => {
      this.clientInstance.topicExists(topics, (err) => {
        if (err) {
          return reject(err);
        }
        return resolve();
      });
    });
  }

  async addTopics(topics: string[]) {
    await this.checkOrConnect('addTopics');
    return new Promise((resolve, reject) => {
      this.clientInstance.addTopics(topics, (err) => {
        if (err) {
          return reject(err);
        }
        return resolve();
      });
    });
  }

  async createTopics(topics: string[]) {
    await this.checkOrConnect('createTopics');
    return new Promise((resolve, reject) => {
      this.clientInstance.createTopics(topics, (err, keys) => {
        if (err) {
          return reject(err);
        }
        return resolve(keys);
      });
    });
  }

  async removeTopicMetadata(topics) {
    await this.checkOrConnect('removeTopicMetadata');
    return new Promise((resolve, reject) => {
      this.clientInstance.removeTopicMetadata(topics, (err, length) => {
        if (err) {
          return reject(err);
        }
        return resolve(length);
      });
    });
  }

  async getListGroups() {
    await this.checkOrConnect('getListGroups');
    return new Promise((resolve, reject) => {
      this.clientInstance.getListGroups((err) => {
        if (err) {
          return reject(err);
        }
        return resolve();
      });
    });
  }

  async getDescribeGroups(groups) {
    await this.checkOrConnect('getDescribeGroups');
    return new Promise((resolve, reject) => {
      this.clientInstance.getDescribeGroups(groups, (err, result) => {
        if (err) {
          return reject(err);
        }
        return resolve(result);
      });
    });
  }

  async sendFetchRequest(request: IFetchRequest) {
    await this.checkOrConnect('sendFetchRequest');
    const { consumer, payloads, fetchMaxWaitMs, fetchMinBytes, maxTickMessages } = request;
    return this.clientInstance.sendFetchRequest(consumer, payloads, fetchMaxWaitMs, fetchMinBytes, maxTickMessages);
  }

  async sendOffsetCommitRequest(group, payloads) {
    await this.checkOrConnect('sendOffsetCommitRequest');
    return new Promise((resolve, reject) => {
      this.clientInstance.sendOffsetCommitRequest(group, payloads, (err) => {
        if (err) {
          return reject(err);
        }
        return resolve();
      });
    });
  }

  async sendOffsetCommitV2Request(group, generationId, memberId, payloads) {
    await this.checkOrConnect('sendOffsetCommitV2Request');
    return new Promise((resolve, reject) => {
      this.clientInstance.sendOffsetCommitV2Request(group, generationId, memberId, payloads, (err) => {
        if (err) {
          return reject(err);
        }
        return resolve();
      });
    });
  }

  async sendOffsetFetchV1Request(group, payloads) {
    await this.checkOrConnect('sendOffsetFetchV1Request');
    return new Promise((resolve, reject) => {
      this.clientInstance.sendOffsetFetchV1Request(group, payloads, (err) => {
        if (err) {
          return reject(err);
        }
        return resolve();
      });
    });
  }

  async sendOffsetFetchRequest(group, payloads) {
    await this.checkOrConnect('sendOffsetFetchV1Request');
    return new Promise((resolve, reject) => {
      this.clientInstance.sendOffsetFetchRequest(group, payloads, (err) => {
        if (err) {
          return reject(err);
        }
        return resolve();
      });
    });
  }

  async sendGroupCoordinatorRequest(groupId) {
    await this.checkOrConnect('sendGroupCoordinatorRequest');
    return new Promise((resolve, reject) => {
      this.clientInstance.sendGroupCoordinatorRequest(groupId, (err) => {
        if (err) {
          return reject(err);
        }
        return resolve();
      });
    });
  }

  async sendJoinGroupRequest(groupId, memberId, sessionTimeout, groupProtocol) {
    await this.checkOrConnect('sendJoinGroupRequest');
    return new Promise((resolve, reject) => {
      this.clientInstance.sendJoinGroupRequest(groupId, memberId, sessionTimeout, groupProtocol, (err) => {
          if (err) {
            return reject(err);
          }
          return resolve();
      });
    });
  }

  async sendSyncGroupRequest(groupId, generationId, memberId, groupAssignment) {
    await this.checkOrConnect('sendSyncGroupRequest');
    return new Promise((resolve, reject) => {
      this.clientInstance.sendSyncGroupRequest(groupId, generationId, memberId, groupAssignment, (err) => {
        if (err) {
          return reject(err);
        }
        return resolve();
      });
    });
  }

  async sendHeartbeatRequest(groupId, generationId, memberId) {
    await this.checkOrConnect('sendHeartbeatRequest');
    return new Promise((resolve, reject) => {
      this.clientInstance.sendHeartbeatRequest(groupId, generationId, memberId, (err) => {
        if (err) {
          return reject(err);
        }
        return resolve();
      });
    });
  }

  async sendLeaveGroupRequest(groupId, memberId) {
    await this.checkOrConnect('sendLeaveGroupRequest');
    return new Promise((resolve, reject) => {
      this.clientInstance.sendLeaveGroupRequest(groupId, memberId, (err) => {
        if (err) {
          return reject(err);
        }
        return resolve();
      });
    });
  }

  async getKafkaClientInstance() {
    await this.checkOrConnect('getKafkaClientInstance');
    if (!this.clientInstance) {
      throw new Error('kafkaClient connect failed!!!');
    }
    return this.clientInstance;
  }

}
