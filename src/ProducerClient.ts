import * as Kafka from 'kafka-node';
import * as Debug from 'debug';
import * as _ from 'lodash';

const debug = Debug('coupler:kafka-mq:ProducerClient');

export class ProducerClient {
  private closing: boolean = false;
  private connecting: boolean = false;
  private connected: boolean = false;

  private isHighLevel: boolean;
  private autoReconnect: boolean;
  private producerInstance: any;
  private options: any;
  private kafkaClient: any;

  constructor(options, isHighLevel: boolean = false, autoReconnect: boolean = false) {
    this.init(options, isHighLevel, autoReconnect);
  }

  async connect() {
    if (this.connecting) {
      debug('connect request ignored. ProducerClient is currently connecting!');
      return;
    }
    this.connecting = true;
    debug(`${this.isHighLevel ? 'HighLevelProducer' : 'Producer'} connecting...`);
    try {
      this.producerInstance = await this.onConnected();
      debug(`${this.isHighLevel ? 'HighLevelProducer' : 'Producer'} onConnected!`);
      this.connected = true;
    } catch (err) {
      debug(`${this.isHighLevel ? 'HighLevelProducer' : 'Producer'} connect failed!`);
      debug(`failed err ==> ${JSON.stringify(err)}`);
      this.connecting = false;
      await this.onReconnecting();
    } finally {
      this.connecting = false;
    }
  }

  async onReconnecting() {
    if (!this.autoReconnect) {
      return;
    }
    if (this.connecting) {
      debug('reconnect request ignored. ProducerClient is currently reconnecting!');
      return;
    }
    debug('start reconnecting [ProducerClient]!!!');
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
        if (!producer) {
          return reject('producer instance not exits...');
        }
        resolve(producer);
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
      debug(`close ProducerClient failed; err => ${JSON.stringify(err)}`);
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

  async createTopics(topics: string[], async: boolean = true) {
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
    debug(`producerClient is sending message = ${JSON.stringify(payloads)}`);
    return new Promise((resolve, reject) => {
      this.producerInstance.send(payloads, (err, data) => {
        if (err) {
            return reject(err);
        }
        return resolve(data);
        });
      });
  }

  isConnected() {
    return this.connected;
  }

 private init(options, isHighLevel, autoReconnect) {
    this.options = options;
    this.kafkaClient = options.client;
    this.isHighLevel = isHighLevel;
    this.autoReconnect = autoReconnect;
  }
}
