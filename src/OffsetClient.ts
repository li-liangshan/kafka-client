import * as Kafka from 'kafka-node';
import * as Debug from 'debug';
import * as _ from 'lodash';
import { generateStringArray, generateTrimStringArray } from './helper';

const debug = Debug('coupler:kafka-mq:OffsetClient');

export class OffsetClient {
  closing: boolean = false;
  connecting: boolean = false;
  connected: boolean = false;

  offsetInstance: any = null;
  autoReconnectCount: number;
  kafkaClient: any;
  constructor(options) {
    const { client, autoReconnectCount = 0 } = options;
    if (!client) {
      throw new Error('client can not be null or undefined!');
    }
    this.kafkaClient = options.client;
    this.autoReconnectCount = autoReconnectCount;
  }

  async connect() {
    if (this.connecting) {
      debug('connect request ignored. KafkaClient is currently connecting!');
      return;
    }

    this.connecting = true;
    try {
      this.offsetInstance = await this.onConnected();
      this.connecting = false;
      this.connected = true;
    } catch (err) {
      debug('OffsetClient connect failed!!!');
      debug(`error ==> ${JSON.stringify(err)}`);
      this.connecting = false;
      await this.onReconnecting();
    } finally {
      this.connecting = false;
    }
  }

  async onConnected() {
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
  }

  async onReconnecting() {
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
  }

  // todo
  async close() {
    if (this.closing) {
      debug('close request ignored. OffsetClient is currently closing...');
      return;
    }
    this.closing = true;
    try {
      const result = await this.onClosed();
      this.connected = false;
      this.closing = false;
      return result;
    } catch (err) {
      this.closing = false;
      debug(`close ConsumerClient failed; err => ${JSON.stringify(err)}`);
      return Promise.reject(err);
    }
  }

  // todo
  async onClosed() {
    this.offsetInstance = null;
    if (!this.connected) {
      return true;
    }
    return true;
  }

  async fetch(payloads: any) {
    debug(`OffsetClient fetching and payloads =${JSON.stringify(payloads)}`);
    await this.checkOrConnectClient();

    return new Promise((resolve, reject) => {
      this.offsetInstance.fetch(payloads, (err, data) => {
        if (err) {
          return reject(err);
        }
        return resolve(data);
      });
    });
  }

  async commit(groupId: string | number, payloads: any) {
    debug(`OffsetClient committing and groupId =${groupId}, payloads =${JSON.stringify(payloads)}`);
    await this.checkOrConnectClient();

    return new Promise((resolve, reject) => {
      this.offsetInstance.commit(groupId, payloads, (err, data) => {
        if (!err) {
          return reject(err);
        }
        return resolve(data);
      });
    });
  }

  async fetchCommits(groupId: string | number, payloads: any) {
    debug(`OffsetClient fetching of commits and groupId=${groupId}, payloads=${JSON.stringify(payloads)}`);
    await this.checkOrConnectClient();

    return new Promise((resolve, reject) => {
      this.offsetInstance.fetchCommits(groupId, payloads, (err, data) => {
        if (!err) {
          return reject(err);
        }
        return resolve(data);
      });
    });
  }

  async fetchLatestOffsets(topics: string | string[]) {
    debug(`OffsetClient fetching latest offsets for topics=${JSON.stringify(topics)}}`);
    await this.checkOrConnectClient();
    const topicArr = await this.parseTopics(topics);

    return new Promise((resolve, reject) => {
      this.offsetInstance.fetchLatestOffsets(topicArr, (err, offsets) => {
        if (err) {
          return reject(err);
        }
        return resolve(offsets);
      });
    });
  }

  async fetchEarliestOffsets(topics: string | string[]) {
    debug(`OffsetClient fetching earliest offsets for topics=${JSON.stringify(topics)}}`);
    await this.checkOrConnectClient();
    const topicArr = await this.parseTopics(topics);

    return new Promise((resolve, reject) => {
      this.offsetInstance.fetchEarliestOffsets(topicArr, (err, offsets) => {
        if (err) {
          return reject(err);
        }
        return resolve(offsets);
      });
    });
  }

  private async parseTopics(topics: string | string[]) {
    const topicArr = await generateTrimStringArray(topics);
    if (!topicArr.length) {
      throw new Error('topics must not be null,"", " " or [] and so on...');
    }
    if (_.isArray(topics) && topics.length !== topicArr.length) {
      throw new Error('topics must not be [" ", " "], ["", " "]. and so on...');
    }
    return topicArr;
  }

  private async checkOrConnectClient() {
    if (this.connected && this.offsetInstance) {
      return;
    }
    debug('OffsetClient disconnected now start connecting....');
    await this.connect();
  }
}
