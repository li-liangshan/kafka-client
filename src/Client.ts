
import * as Kafka from 'kafka-node';
import * as Debug from 'debug';
import * as _ from 'lodash';

const debug = Debug('coupler:kafka-mq:client');

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

export class Client {
  closing: boolean = false;
  connecting: boolean = false;
  connected: boolean = false;

  options: IKafkaOptions;
  private clientInstance: any;

  constructor(options: IKafkaOptions) {
    this.options = options;
  }

  async connect() {
    if (this.connecting) {
      debug('connect request ignored. client is currently connecting!');
      return;
    }
    this.connecting = true;
    debug('client start to connect...');
    try {
      this.clientInstance = await this.onConnected();
      debug('client onConnected!!!');
      this.connected = true;
    } catch (err) {
      debug('client connect failed...');
      this.clientInstance = null;
      this.connected = false;
    } finally {
      this.connecting = false;
    }
  }

  async onConnected(): Promise<any> {
    if (this.connected && this.clientInstance) {
      return this.clientInstance;
    }
    this.connected = false;
    this.clientInstance = null;
    const client = new Kafka.KafkaClient(this.options);
    return new Promise((resolve, reject) => {
      if (!client) {
        return reject('kafka instance create failed...')
      }
      client.on('ready', () => resolve(client));
    });
  }

  async close() {
    if (this.closing) {
      debug('close request ignored. client has been closed currently.');
      return;
    }
    this.closing = true;
    try {
      const result = await this.onClosed();
      this.connected = false;
      this.clientInstance = null;
      this.closing = false;
      this.connecting = false;
      debug('client close success...');
      return result;
    } catch (err) {
      this.closing = false;
      debug(`client close failed, err => ${JSON.stringify(err)}`);
      throw err;
    }
  }
  async onClosed(instance?: any) {
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
  }

 private async checkOrConnect(funName: string) {
    debug(`check the client Connected`);
    if (!this.connected) {
      debug(`KafkaClient is not connected [${funName}]`);
      debug('KafkaClient start connecting ....');
      await this.connect();
    }
    debug(`KafkaClient connected before ${funName}`);
  }

  async sendProduceRequest(request: ISendRequest) {
    await this.checkOrConnect('sendRequest');
    debug('sending request!');
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

  async getDescribeGroups(groupIds) {
    await this.checkOrConnect('getDescribeGroups');
    return new Promise((resolve, reject) => {
      this.clientInstance.getDescribeGroups(groupIds, (err, result) => {
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

  async sendOffsetCommitRequest(groupId, payloads) {
    await this.checkOrConnect('sendOffsetCommitRequest');
    return new Promise((resolve, reject) => {
      this.clientInstance.sendOffsetCommitRequest(groupId, payloads, (err) => {
        if (err) {
          return reject(err);
        }
        return resolve();
      });
    });
  }

  async sendOffsetCommitV2Request(groupId, generationId, memberId, payloads) {
    await this.checkOrConnect('sendOffsetCommitV2Request');
    return new Promise((resolve, reject) => {
      this.clientInstance.sendOffsetCommitV2Request(groupId, generationId, memberId, payloads, (err) => {
        if (err) {
          return reject(err);
        }
        return resolve();
      });
    });
  }

  async sendOffsetFetchV1Request(groupId, payloads) {
    await this.checkOrConnect('sendOffsetFetchV1Request');
    return new Promise((resolve, reject) => {
      this.clientInstance.sendOffsetFetchV1Request(groupId, payloads, (err) => {
        if (err) {
          return reject(err);
        }
        return resolve();
      });
    });
  }

  async sendOffsetFetchRequest(groupId, payloads) {
    await this.checkOrConnect('sendOffsetFetchV1Request');
    return new Promise((resolve, reject) => {
      this.clientInstance.sendOffsetFetchRequest(groupId, payloads, (err) => {
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

  async getKafkaInstance() {
    await this.checkOrConnect('getKafkaClientInstance');
    if (!this.clientInstance) {
      throw new Error('kafka instance is null !!!');
    }
    return this.clientInstance;
  }

}
