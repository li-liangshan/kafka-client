import * as Kafka from 'kafka-node';
import * as Debug from 'debug';
import * as _ from 'lodash';
import { generateTrimStringArray } from './helper';

const debug = Debug('coupler:kafka-mq:ConsumerStreamClient');

export class ConsumerStreamClient {
  closing: boolean = false;
  connecting: boolean = false;
  connected: boolean = false;

  private streamInstance: any = null;
  private autoReconnectCount: number;
  private kafkaClient: any;
  private options: any;
  private topics: string[];
  constructor(client: any, topics: string | string[], options: any) {
    this.init(client, topics, options);
  }

  async connect(): Promise<any> {
    if (this.connecting) {
      debug('connect request ignored. ConsumerStreamClient is currently connecting...');
      return;
    }
    this.connecting = true;
    debug('ConsumerStreamClient start to connect...');
    try {
      this.streamInstance = await this.onConnected();
      debug('ConsumerStreamClient onConnected!!!');
      this.connected = true;
    } catch (err) {
      debug('ConsumerStreamClient connect failed...');
      this.connecting = false;
      await this.onReconnecting();
    } finally {
      this.connecting = false;
    }
  }

  async onConnected(): Promise<any> {
    if (this.connected && this.streamInstance) {
      return;
    }
    this.connected = false;
    this.streamInstance = null;
    const consumerStream = new Kafka.ConsumerStream(this.kafkaClient, this.topics, this.options);
    return new Promise((resolve, reject) => {
      if (!consumerStream) {
        return reject('consumerStream instance is null!');
      }
      consumerStream.on('readable', () => resolve(consumerStream));
    });
  }

  async onReconnecting(): Promise<any> {
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
  }

  async close(force: boolean = false) {
    if (this.closing) {
      debug('close request ignored. client is currently closing.');
      return;
    }
    this.closing = true;
    try {
      const result = await this.onClosed(force);
      this.connected = false;
      this.closing = false;
      this.connecting = false;
      this.streamInstance = null;
      debug('client close success...');
      return result;
    } catch (err) {
      this.closing = false;
      debug(`client close failed, err => ${JSON.stringify(err)}`);
      throw err;
    }
  }

  async onClosed(force: boolean = false) {
    if (!this.closing) {
      debug('close request error, maybe onClosed...');
      return null;
    }
    if (!this.connected || !this.streamInstance) {
      debug('close request refused. client has been closed currently.');
      return null;
    }
    return new Promise((resolve, reject) => {
      this.streamInstance.close(force, resolve);
    });
  }

  async createCommitStream(options) {
    if (!this.connected || !this.streamInstance) {
      this.connected = false;
      this.streamInstance = null;
      await this.connect();
    }
    debug('create commit stream...');
    return this.streamInstance.createCommitStream(options);
  }

  async consumeStreamMessage(handler) {
    if (!this.connected || !this.streamInstance) {
      await this.connect();
    }
    return this.streamInstance.on('message', (message) => this.createMessageHandler(handler));
  }

  private async createMessageHandler(handler) {
    debug('start to create messageHandler!');
    return (message) => this.onMessage(message, handler);
  }

  private onMessage(message: any, handler) {
    debug(`message is being handled! message=>${JSON.stringify(message)}`);
    if (!message) {
      return;
    }

    if (!_.isFunction(handler)) {
      debug('message omitted due to no messageHandler!!!');
      return;
    }

    try {
      handler(message);
    } catch (err) {
      debug(`handle message failed due to error => ${JSON.stringify(err)}`);
    }
  }

  private init(client: any, topics: string | string[], options: any): void {
    this.options = options;
    this.autoReconnectCount =  options.autoReconnectCount || 0;
    const topicArr = generateTrimStringArray(topics);
    if (!topicArr || !topicArr.length) {
      throw new Error('topics must not be null, [], [" "] or ""...');
    }
    this.topics = topicArr;

    if (!client) {
      throw new Error('kafka instance must not be null!');
    }

    this.kafkaClient = client;
  }
}
