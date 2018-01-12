import * as Kafka from 'kafka-node';
import * as Debug from 'debug';
import * as _ from 'lodash';
import { Promise } from 'es6-promise';

const debug = Debug('coupler:kafka-mq:AdminClient');

export class AdminClient {

  closing: boolean = false;
  connecting: boolean = false;
  connected: boolean = false;

  private adminInstance: any = null;
  private autoReconnectCount: number;
  private kafkaClient: any;
  private options: any;
  private topics: string[];
  constructor(client: any) {
    if (!client) {
      throw new Error('kafka-mq client is not exists...');
    }
    this.kafkaClient = client;
  }

  async connect(): Promise<any> {
    if (this.connecting) {
      debug('connect request ignored. AdminClient is currently connecting...');
      return;
    }
    this.connecting = true;
    debug('AdminClient start to connect...');
    try {
      this.adminInstance = await this.onConnected();
      debug('AdminClient onConnected!!!');
      this.connected = true;
    } catch (err) {
      debug('AdminClient connect failed...');
      this.connecting = false;
      await this.onReconnecting();
    } finally {
      this.connecting = false;
    }
  }

  async onConnected(): Promise<any> {
    if (this.connected && this.adminInstance) {
      return;
    }
    this.connected = false;
    this.adminInstance = null;
    const adminInstance = new Kafka.Admin(this.kafkaClient, this.topics, this.options);
    return new Promise((resolve, reject) => {
      if (!adminInstance) {
        return reject('adminInstance instance is null!');
      }
      adminInstance.on('readable', () => resolve(adminInstance));
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
      this.adminInstance = null;
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
    if (!this.connected || !this.adminInstance) {
      debug('close request refused. client has been closed currently.');
      return null;
    }
    return new Promise((resolve, reject) => {
      this.adminInstance.close(force, resolve);
    });
  }

  async listGroups() {
    if (!this.connected || !this.adminInstance) {
      this.connected = false;
      await this.connect();
    }
    return new Promise((resolve, reject) => {
      this.adminInstance.listGroups(resolve);
    });
  }

  async describeGroups(consumerGroups) {
    if (!this.connected || !this.adminInstance) {
      this.connected = false;
      await this.connect();
    }
    return new Promise((resolve, reject) => {
      this.adminInstance.describeGroups(consumerGroups, resolve);
    });
  }
}
