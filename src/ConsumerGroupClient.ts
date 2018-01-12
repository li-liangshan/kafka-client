import * as Kafka from 'kafka-node';
import debug from 'debug';
import { isFunction } from 'lodash';
import { ConsumerClient } from './ConsumerClient';
import { promiseFn } from './helper';

const log = debug('coupler:kafka-mq:ConsumerGroupClient');

export class ConsumerGroupClient extends ConsumerClient {

  closing: boolean = false;
  connecting: boolean = false;
  connected: boolean = false;
  reconnecting: boolean = false;

  consumerInstance: any = null;
  options: any;
  autoReconnectCount: number;
  kafkaClient: any;

  private topics: string[];

  constructor(options: any, topics: string[]) {
    super(options);
    this.init(options, topics);
  }

  async connect() {
    if (this.connecting) {
      log('connect request ignored. ConsumerGroupClient is currently connecting!!!');
      return;
    }

    this.connecting = true;

    try {
      this.consumerInstance = await this.onConnected();
      log('ConsumerGroupClient onConnected!!!');
      this.connected = true;
    } catch (err) {
      log('ConsumerGroupClient connect on failed!!!');
      log(`failed err = ${JSON.stringify(err)}`);
      await this.onReconnecting();
    }
  }

  async onConnected() {
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
  }

  // todo 这里需要进一步处理，
  async scheduleReconnect(timeout: number) {
    while (this.connecting) {
      log('ConsumerGroupClient is connecting!');
    }
    if (!this.connected) {
      throw new Error('ConsumerGroupClient be disconnected!!!');
    }
    return this.consumerInstance.scheduleReconnect(timeout);
  }

  async sendOffsetCommitRequest(commits: any) {
    if (!this.connected) {
      throw new Error('ConsumerGroupClient be disconnected!!!');
    }
    return promiseFn(
      this.consumerInstance.sendOffsetCommitRequest,
      this.consumerInstance,
    )(commits);
  }

  async addTopics(topics: string | string[]) {
    if (!topics) {
      return null;
    }
    if (typeof topics === 'string' && !Boolean(topics.trim())) {
      return null;
    }
    if (!this.connected) {
      await this.connect();
    }
    const topicArr: string[] = typeof topics === 'string' ? [topics] : topics.filter((topic) => topic.trim());
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
  }

  async pauseTopic(topics: string[]) {
    return;
  }
  async resumeTopics(topics: string[]) {
    return;
  }

  private init(options: any, topics: string[]) {
    this.options = options;
    this.topics = topics;

    // this.connect();
  }
}
