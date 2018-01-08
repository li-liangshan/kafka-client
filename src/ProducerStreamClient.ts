import * as Kafka from 'kafka-node';
import * as Debug from 'debug';
import * as _ from 'lodash';
import { Promise } from 'es6-promise';
import { resolve } from 'url';
import { fail } from 'assert';

const debug = Debug('coupler:kafka-mq:ProducerStreamClient');

export class ProducerStreamClient {
  closing: boolean = false;
  connecting: boolean = false;
  connected: boolean = false;

  streamInstance: any = null;
  autoReconnectCount: number;
  private options: any;
  constructor(options: any, autoReconnectCount: number = 0) {
    this.options = options;
    this.autoReconnectCount = autoReconnectCount;
  }

  async connect() {
    if (this.connecting) {
      debug('connect request ignored. ProducerStreamClient is currently connecting...');
      return;
    }
    this.connecting = true;
    try {
      this.streamInstance = await this.onConnected();
      debug('producerStreamClient onConnected!');
      this.connected = true;
    } catch (err) {
      debug('producerStreamClient connect failed!!!');
      debug(`failed err ==> ${JSON.stringify(err)}`);
      this.connecting = false;
      await this.onReconnecting();
    } finally {
      this.connecting = false;
    }
  }

  async onConnected() {
    if (this.connected && this.streamInstance) {
      return this.streamInstance;
    }
    this.connected = false;
    try {
      return new Kafka.ProducerStream(this.options);
    } catch (err) {
      throw err;
    }
  }

  async onReconnecting() {
    if (this.connecting) {
      debug('connect request ignored. ProducerStreamClient is currently connecting...');
      return;
    }

    if (this.connected && this.streamInstance) {
      debug('reconnect refused. ProducerStreamClient has been connected...');
      return;
    }

    if (this.autoReconnectCount <= 0) {
      debug('ProducerStreamClient can not reconnect...');
      return;
    }
    debug('start reconnecting [ProducerStreamClient]...');
    this.autoReconnectCount -= 1;
    return this.connect();
  }

  async close() {
    if (this.closing) {
      debug('close request ignored. ProducerStreamClient is currently closing...');
      return;
    }
    if (!this.connected || !this.streamInstance) {
      debug('close request refused. ProducerStreamClient has been closed...');
      return;
    }
    this.closing = true;
    try {
      const result = await this.onClosed();
      this.connected = false;
      this.closing = false;
      this.connecting = false;
      this.streamInstance = null;
      debug('ProducerStreamClient  onClosed...');
      return result;
    } catch (err) {
      debug(`ProducerStreamClient close err ==> ${JSON.stringify(err)}`);
      this.closing = false;
      throw err;
    }
  }

  async onClosed() {
    if (!this.connected || !this.streamInstance) {
      return null;
    }
    return new Promise((resolve, reject) => {
      this.streamInstance.close(resolve);
    });
  }

}
