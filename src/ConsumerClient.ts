import * as Kafka from 'kafka-node';
import debug from 'debug';
import { isFunction } from 'lodash';
import { promiseFn } from './helper';
import { Promise } from 'es6-promise';

const log = debug('coupler:kafka-mq:ConsumerClient');

export type MessageHandler = (message: any) => void;
export type ErrorHandler = (error: any) => void;
export type OffsetOutOfRangeHandler = (topic: string) => void;

export class ConsumerClient {
  closing: boolean = false;
  connecting: boolean = false;
  connected: boolean = false;
  reconnecting: boolean = false;

  consumerInstance: any = null;
  options: any;
  autoReconnectCount: number;
  kafkaClient: any;

  private isHighLevel: boolean;

  constructor(options: any, isHighLevel?: boolean, autoReconnectCount?: number) {
    this.options = options;
    this.kafkaClient = options.client;
    this.isHighLevel = isHighLevel || false;
    this.autoReconnectCount = autoReconnectCount || 0;

    // this.connect();
  }

  async connect() {
    if (this.connecting) {
      log('connect request ignored. ConsumerClient is currently connecting!');
      return;
    }
    this.connecting = true;
    log(`${this.isHighLevel ? 'HighLevelConsumer' : 'Consumer'} connecting...`);
    try {
      this.consumerInstance = await this.onConnected();
      log(`${this.isHighLevel ? 'HighLevelConsumer' : 'Consumer'} onConnected!`);
      this.connected = true;
    } catch (err) {
      log(`${this.isHighLevel ? 'HighLevelProducer' : 'Producer'} connect failed!!!`);
      log(`failed err ==> ${JSON.stringify(err)}`);
      this.connecting = false;
      await this.onReconnecting();
    } finally {
      this.connecting = false;
      this.reconnecting = false;
    }
  }

  async onConnected() {
    if (this.connected && this.consumerInstance) {
      return this.consumerInstance;
    }
    this.connected = false;
    const KafkaConsumer = this.isHighLevel ? Kafka.HighLevelConsumer : Kafka.Consumer;
    const consumer = new KafkaConsumer(this.kafkaClient, this.options.topics, this.options.options);
    return consumer ? consumer : Promise.reject('consumer is null!!!');
  }

  async onReconnecting() {
    if (this.reconnecting) {
      log('reconnect request ignored. ConsumerClient is currently reconnecting!');
      return;
    }
    if (this.connected) {
      log('reconnect request ignored. ConsumerClient have been connected!');
      return;
    }
    this.reconnecting = true;
    if (this.autoReconnectCount <= 0) {
      return;
    }
    log('start reconnecting [ConsumerClient]!!!');
    this.autoReconnectCount -= 1;
    return this.connect();
  }

  async close(force: boolean = false) {
    if (this.closing) {
      return;
    }
    this.closing = true;
    try {
      const result = await this.onClosed(force);
      this.consumerInstance = null;
      this.connected = false;
      this.closing = false;
      return Promise.resolve(result);
    } catch (err) {
      this.closing = false;
      log(`close ConsumerClient failed; err => ${JSON.stringify(err)}`);
      return Promise.reject(err);
    }
  }

  async onClosed(force: boolean = false) {
    if (!this.connected || !this.consumerInstance) {
      return null;
    }
    return promiseFn(this.consumerInstance.close, this.consumerInstance)(force);
  }

  async pause() {
    if (!this.connected) {
      await this.connect();
    }
    return this.consumerInstance.pause();
  }

  async resume() {
    if (!this.connected) {
      await this.connect();
    }
    return this.consumerInstance.resume();
  }

  async commit(force: boolean = false) {
    if (!this.connected) {
      await this.connect();
    }
    return promiseFn(this.consumerInstance.commit, this.consumerInstance)(force);
    // return new Promise((resolve, reject) => {
    //   this.consumerInstance.commit(force, (err, data) => {
    //     if (err) {
    //       return reject(err);
    //     }
    //     return resolve(data);
    //   });
    // });
  }

  async addTopics(topics: string | string[], fromOffset: boolean = false) {
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
      }, fromOffset);
    });
  }

  async removeTopics(topics: string[]) {
    if (!this.connected || !this.consumerInstance) {
      log('ConsumerClient disConnected, not topics can be removed!');
      return null;
    }
    if (!topics.length) {
      log('ConsumerClient no topic removed due to no topics!');
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
    return promiseFn(this.consumerInstance.removeTopics, this.consumerInstance)(topics);
  }

  async setOffset(topic: string, partition: number = 0, offset: number = 0) {
    if (!this.connected) {
      await this.connect();
    }
    if (topic.trim()) {
      log('ConsumerClient no topic removed due to no topics!');
      throw new Error('topic can not be \' \'');
    }
    return this.consumerInstance.setOffset(topic, partition, offset);
  }

  async pauseTopic(topics: string[]) {
    if (!this.connected) {
      await this.connect();
    }
    return this.consumerInstance.pauseTopic(topics);
  }

  async resumeTopics(topics: string[]) {
    if (!this.connected) {
      await this.connect();
    }
    return this.consumerInstance.resumeTopics(topics);
  }

  async getTopicPayloads() {
    if (!this.connected) {
      await this.connect();
    }
    if (!this.isHighLevel) {
      log('topic payloads can not be obtained due to not HighLevelConsumer');
      return;
    }
    return this.consumerInstance.getTopicPayloads();
  }

  async consumeMessage(handler: MessageHandler) {
    if (!this.connected) {
      await this.connect();
    }
    return this.consumerInstance.on('message', (message) => this.createMessageHandler(handler));
  }

  async handleError(errorHandler: ErrorHandler) {
    if (!this.connected) {
      await this.connect();
    }
    return this.consumerInstance.on('error', (error) => this.createErrorHandler(errorHandler));
  }

  async consumeOffsetOutOfRange(offsetHandler: OffsetOutOfRangeHandler) {
    if (!this.connected) {
      await this.connect();
    }
    return this.consumerInstance.on('offsetOutOfRange', (topic) => this.createOffsetOutOfRange(topic));
  }

  private async createMessageHandler(handler: MessageHandler) {
    log('start to create messageHandler!');
    return (message) => this.onMessage(message, handler);
  }

  private async createErrorHandler(errorHandler: ErrorHandler) {
    log('start to create errorHandler!');
    return (error) => this.onError(error, errorHandler);
  }

  private async createOffsetOutOfRange(offsetHandler: OffsetOutOfRangeHandler) {
    log('start to create offsetOutOfRangeHandler!');
    return (topic) => this.onOffsetOutOfRange(topic, offsetHandler);
  }

  private onMessage(message: any, handler: MessageHandler) {
    log(`message is being handled! message=>${JSON.stringify(message)}`);
    if (!message) {
      return;
    }

    if (!isFunction(handler)) {
      log('message omitted due to no messageHandler!!!');
      return;
    }

    try {
      handler(message);
    } catch (err) {
      log(`handle message failed due to error => ${JSON.stringify(err)}`);
    }
  }

  private onError(error: any, handler: ErrorHandler) {
    log(`consumerClient error is being handled! error=>${JSON.stringify(error)}`);
    if (!error) {
      return;
    }

    if (!isFunction(handler)) {
      log('message omitted due to no errorHandler!!!');
      return;
    }

    try {
      handler(error);
    } catch (err) {
      log(`consumerClient handle onError failed due to err => ${JSON.stringify(err)}`);
    }
  }

  private onOffsetOutOfRange(topic: string, offsetHandler: OffsetOutOfRangeHandler) {
    if (!topic) {
      log('nothing to do due to no topic!!!');
      return;
    }
    if (!isFunction(offsetHandler)) {
      log('no topic to do due to no offsetHandler!!!');
      return;
    }
    try {
      offsetHandler(topic);
    } catch (err) {
      log(`handle offsetOutOfRange topic failed due to err => ${JSON.stringify(err)}`);
    }
  }
}
