import * as Kafka from 'kafka-node';
import * as Debug from 'debug';
import { isFunction } from 'lodash';
import { promiseFn } from './helper';
import { resolve } from 'dns';

const debug = Debug('LLS:testSpec');

export type MessageHandler = (message: any) => void;
export type ErrorHandler = (error: any) => void;
export type OffsetOutOfRangeHandler = (err: string) => void;

/****************************************************************
 *** HighLevelConsumer has been deprecated in the latest version
 *** of Kafka (0.10.1) and is likely to be removed in the future.
 *** Please use the ConsumerGroup instead
 ****************************************************************/
export class ConsumerClient {
  closing: boolean = false;
  connecting: boolean = false;
  connected: boolean = false;

  consumerInstance: any = null;
  options: any;
  autoReconnect: boolean;
  kafkaClient: any;

  private isHighLevel: boolean;

  constructor(options: any, isHighLevel?: boolean, autoReconnect?: boolean) {
    this.options = options;
    this.kafkaClient = options.client;
    this.isHighLevel = isHighLevel || false;
    this.autoReconnect = autoReconnect || false;

    // this.connect();
  }

  async connect() {
    if (this.connecting) {
      debug('connect request ignored. ConsumerClient is currently connecting!');
      return;
    }
    this.connecting = true;
    debug(`${this.isHighLevel ? 'HighLevelConsumer' : 'Consumer'} connecting...`);
    try {
      this.consumerInstance = await this.onConnected();
      debug(`${this.isHighLevel ? 'HighLevelConsumer' : 'Consumer'} onConnected!`);
      this.connected = true;
      this.connecting = false;
    } catch (err) {
      debug(`${this.isHighLevel ? 'HighLevelProducer' : 'Producer'} connect failed!!!`);
      debug(`failed err ==> ${JSON.stringify(err)}`);
      this.connecting = false;
      await this.onReconnecting();
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
    if (this.connecting) {
      debug('reconnect request ignored. ConsumerClient is currently reconnecting!');
      return;
    }
    if (this.connected) {
      debug('reconnect request ignored. ConsumerClient have been connected!');
      return;
    }
    if (!this.autoReconnect) {
      return;
    }
    debug('start reconnecting [ConsumerClient]!!!');
    return this.connect();
  }

  isConnected() {
    return this.connected;
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
      return result;
    } catch (err) {
      this.closing = false;
      debug(`close ConsumerClient failed; err => ${JSON.stringify(err)}`);
      throw err;
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
      debug('ConsumerClient no topic added due to no topics!');
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
      debug('ConsumerClient disConnected, not topics can be removed!');
      return null;
    }
    if (!topics.length) {
      debug('ConsumerClient no topic removed due to no topics!');
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
    if (!topic.trim()) {
      debug('ConsumerClient no topic removed due to no topics!');
      throw new Error('topic can not be \' \'');
    }
    return this.consumerInstance.setOffset(topic, partition, offset);
  }

  async pauseTopics(topics: string[]) {
    if (!this.connected) {
      await this.connect();
    }
    return this.consumerInstance.pauseTopics(topics);
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
      return this.consumerInstance.payloads;
    }
    return this.consumerInstance.getTopicPayloads();
  }

  async consumeMessage(handler?: MessageHandler) {
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
    return this.consumerInstance.on('offsetOutOfRange', (err) => this.createOffsetOutOfRange(err));
  }

  async getConsumer() {
    if (!this.connected) {
      await this.connect();
    }
    return this.consumerInstance;
  }

  private async createMessageHandler(handler: MessageHandler) {
    debug('start to create messageHandler!');
    return (message) => this.onMessage(message, handler);
  }

  private async createErrorHandler(errorHandler: ErrorHandler) {
    debug('start to create errorHandler!');
    return (error) => this.onError(error, errorHandler);
  }

  private async createOffsetOutOfRange(offsetHandler: OffsetOutOfRangeHandler) {
    debug('start to create offsetOutOfRangeHandler!');
    return (err) => this.onOffsetOutOfRange(err, offsetHandler);
  }

  private onMessage(message: any, handler: MessageHandler) {
    debug(`message is being handled! message=>${JSON.stringify(message)}`);
    if (!message) {
      return;
    }

    if (!isFunction(handler)) {
      debug('message omitted due to no messageHandler!!!');
      return;
    }

    try {
      handler(message);
    } catch (err) {
      debug(`handle message failed due to error => ${JSON.stringify(err)}`);
    }
  }

  private onError(error: any, handler: ErrorHandler) {
    debug(`consumerClient error is being handled! error=>${JSON.stringify(error)}`);
    if (!error) {
      return;
    }

    if (!isFunction(handler)) {
      debug('message omitted due to no errorHandler!!!');
      return;
    }

    try {
      handler(error);
    } catch (err) {
      debug(`consumerClient handle onError failed due to err => ${JSON.stringify(err)}`);
    }
  }

  private onOffsetOutOfRange(error: any, offsetHandler: OffsetOutOfRangeHandler) {
    if (!error) {
      debug('nothing to do due to no err!!!');
      return;
    }
    if (!isFunction(offsetHandler)) {
      debug('nothing to do due to no offsetHandler!!!');
      return;
    }
    try {
      offsetHandler(error);
    } catch (err) {
      debug(`handle offsetOutOfRange topic failed due to err => ${JSON.stringify(err)}`);
    }
  }
}
