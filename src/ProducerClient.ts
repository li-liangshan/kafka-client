import * as Kafka from 'kafka-node';
import debug from 'debug';
import * as _ from 'lodash';

const log = debug('coupler:kafka-mq:ProducerClient');

export class ProducerClient {
  private closing: boolean = false;
  private connecting: boolean = false;
  private connected: boolean = false;
  private reconnecting: boolean = false;

  private isHighLevel: boolean;
  private autoReconnectCount: number;
  private producerInstance: any;
  private options: any;
  private kafkaClient: any;

  constructor(options, isHighLevel?: boolean, autoReconnectCount?: number) {
    this.options = options;
    this.kafkaClient = options.client;
    this.isHighLevel = isHighLevel || false;
    this.autoReconnectCount = autoReconnectCount || 0;
  }

  async connect() {
    if (this.connecting) {
      log('connect request ignored. ProducerClient is currently connecting!');
      return;
    }
    this.connecting = true;
    log(`${this.isHighLevel ? 'HighLevelProducer' : 'Producer'} connecting...`);
    try {
      this.producerInstance = await this.onConnected();
      log(`${this.isHighLevel ? 'HighLevelProducer' : 'Producer'} onConnected!`);
      this.connected = true;
    } catch (err) {
      log(`${this.isHighLevel ? 'HighLevelProducer' : 'Producer'} connect failed!!!`);
      log(`failed err ==> ${JSON.stringify(err)}`);
      await this.onReconnecting();
    } finally {
      this.connecting = false;
      this.reconnecting = false;
    }
  }

  async onReconnecting() {
    if (this.reconnecting) {
      log('reconnect request ignored. ProducerClient is currently reconnecting!');
      return;
    }
    this.reconnecting = true;
    if (this.autoReconnectCount <= 0) {
      return;
    }
    log('start reconnecting [ProducerClient]!!!');
    this.autoReconnectCount -= 1;
    return this.connect();
  }

  async onConnected() {
    if (this.connected && this.producerInstance) {
      return this.producerInstance;
    }
    this.connected = false;
    const KafkaProducer = this.isHighLevel ? Kafka.HighLevelProducer : Kafka.Producer;
    const producer = new KafkaProducer(this.kafkaClient, this.options.producerOptions, this.options.partitioner);
    return new Promise((resolve, reject) => {
      producer.on('ready', () => {
        resolve(producer);
      });
      producer.on('error', (err) => {
        reject(err);
      });
    });
  }

  async close() {
    if (this.closing) {
      return;
    }
    this.closing = true;
    try {
      await this.onClosed();
      this.producerInstance = null;
      this.connected = false;
    } catch (err) {
      log(`close ProducerClient failed; err => ${JSON.stringify(err)}`);
    } finally {
      this.closing = false;
    }
  }

  async onClosed() {
    if (!this.connected || !this.producerInstance) {
      return null;
    }
    return new Promise((resolve, reject) => {
      this.producerInstance.close(resolve);
    });
  }

  async createTopics(topics: string[], async: boolean) {
    if (!this.connected) {
      await this.connect();
    }
    return new Promise((resolve, reject) => {
      this.producerInstance.createTopics(topics, async, (err, topicNames) => {
        if (err) {
          return reject(err);
        }
        return resolve(topicNames);
      });
    });
  }

  async send(payloads) {
    if (!this.connected) {
      await this.connect();
    }
    log(`producerClient is sending message = ${JSON.stringify(payloads)}`);
    return new Promise((resolve, reject) => {
      this.producerInstance.send(payloads, (err, data) => {
        if (err) {
          return reject(err);
        }
        return resolve(data);
      });
    });
  }
}
