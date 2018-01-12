import * as Kafka from 'kafka-node';
import * as Debug from 'debug';
import { isFunction } from 'lodash';
import { promiseFn, generateTrimStringArray } from './helper';

const debug = Debug('coupler:kafka-mq:ConsumerGroupStreamClient');

export class ConsumerGroupStreamClient {
  closing: boolean = false;
  connecting: boolean = false;
  connected: boolean = false;

  groupInstance: any = null;
  options: any;
  autoReconnectCount: number;
  kafkaClient: any;

  private topics: string[];
  constructor(options: any, topics: string | string[]) {
    this.init(options, topics);
  }

  async connect(): Promise<any> {
    if (this.connecting) {
      debug('connect request ignored. client is currently connecting...');
      return;
    }
    this.connecting = true;
    debug('client start to connect...');
    try {
      this.groupInstance = await this.onConnected();
      debug('client onConnected!!!');
      this.connected = true;
    } catch (err) {
      debug('client connect failed...');
      this.connecting = false;
      await this.onReconnecting();
    } finally {
      this.connecting = false;
    }
  }

  async close(): Promise<any> {
    if (this.closing) {
      debug('close request ignored. client is currently closing...');
      return;
    }
    this.closing = true;
    try {
      const result = await this.onClosed();
      this.closing = false;
      this.connected = false;
      this.connecting = false;
      this.groupInstance = null;
      return result;
    } catch (err) {
      debug(`client close failed err=${JSON.stringify(err)}`);
      this.closing = false;
      throw err;
    }
  }

  async getConsumerGroupStream() {
    if (!this.connected || !this.groupInstance) {
      await this.connect();
    }
    return this.groupInstance;
  }

  private async onConnected(): Promise<any> {
    if (this.connected && this.groupInstance) {
      return;
    }
    this.connected = false;
    this.groupInstance = null;
    const consumerGroupStream = new Kafka.ConsumerGroupStream(this.options, this.topics);
    return new Promise((resolve, reject) => {
      if (!consumerGroupStream || !consumerGroupStream.consumerGroup) {
        return reject('consumerGroupStream instance is null!');
      }
      consumerGroupStream.consumerGroup.on('connect', () => resolve(consumerGroupStream));
    });
  }

  private async onReconnecting(): Promise<any> {
    if (this.connecting) {
      debug('reconnect request ignored. client is currently reconnecting...');
      return;
    }
    if (this.connected) {
      debug('reconnect request ignored. client has been connected!');
      return;
    }
    if (this.autoReconnectCount <= 0) {
      debug('reconnect request refused. current reconnect count <= 0');
      return;
    }
    debug('start reconnecting !!!');
    this.autoReconnectCount -= 1;
    return this.connect();
  }

  private async init(options: any, topics: string | string[]): Promise<void> {
    this.options = options;
    this.autoReconnectCount =  options.autoReconnectCount || 0;
    const topicArr = generateTrimStringArray(topics);
    if (!topicArr || !topicArr.length) {
      throw new Error('topics must not be null, [], [" "] or ""...');
    }
    this.topics = topicArr;
    return this.connect();
  }

 private async onClosed(): Promise<any> {
    if (!this.closing) {
      debug('close request error, maybe onClosed...');
      return null;
    }
    if (!this.connected || !this.groupInstance) {
      debug('close request refused. client has been closed currently.');
      return null;
    }
    return new Promise((resolve, reject) => {
      this.groupInstance.close(resolve);
    });
  }

}
