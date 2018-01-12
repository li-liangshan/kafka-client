
import * as Kafka from 'kafka-node';
import * as Debug from 'debug';
import * as _ from 'lodash';
import { EventEmitter } from 'events';
import { Transform, Readable } from 'stream';
import { ProducerClient } from './ProducerClient';
import { ConsumerClient } from './ConsumerClient';
import { ConsumerGroupClient } from './ConsumerGroupClient';
import { ConsumerStreamClient } from './ConsumerStreamClient';
import { ConsumerGroupStreamClient } from './ConsumerGroupStreamClient';
import { ProducerStreamClient } from './ProducerStreamClient';
import { OffsetClient } from './OffsetClient';
import { AdminClient } from './AdminClient';

const debug = Debug('coupler:kafka-mq:client');

export interface IKafkaOptions {
  clientId?: string;
  sslOptions?: any;
  ssl?: boolean;

  kafkaHost?: string;
  connectTimeout?: number;
  requestTimeout?: number;
  idleConnection?: number;
  autoConnect?: boolean;
  versions?: {
    disabled?: boolean;
    requestTimeout?: boolean;
  };
  connectRetryOptions?: {
    retries?: number;
    factor?: number;
    minTimeout?: number;
    maxTimeout?: number;
    randomize?: boolean;
  };
  maxAsyncRequests?: number;
}

export interface ISendRequest {
  payloads?: any;
  requireAcks?: number;
  ackTimeoutMs?: number;
}

export interface IFetchRequest {
  consumer: any;
  payloads?: any;
  fetchMaxWaitMs?: number;
  fetchMinBytes?: number;
  maxTickMessages?: number;
}

export class Client extends EventEmitter {
  closing: boolean = false;
  connecting: boolean = false;
  connected: boolean = false;

  options: IKafkaOptions;
  private clientInstance: any;
  private producerClients: Map<string, any>;
  private consumerClients: Map<string, any>;
  private consumerGroupClients: Map<string, any>;
  private consumerStreamClients: Map<string, any>;
  private consumerGroupStreamClients: Map<string, any>;
  private producerStreamClients: Map<string, any>;
  private producerStreamTransforms: Map<string, Transform>;
  private consumerStreamTransforms: Map<string, Transform>;
  private consumerGroupStreamTransforms: Map<string, Transform>;
  private offsetClients: Map<string, any>;
  private adminClients: Map<string, any>;
  private clientId: string;

  constructor(options: IKafkaOptions) {
    super();
    this.init(options);
  }

  async connect() {
    if (this.connecting) {
      debug('connect request ignored. client is currently connecting!');
      return;
    }
    this.connecting = true;
    debug('client start to connect...');
    try {
      this.clientInstance = await this.onConnected();
      debug('client onConnected!!!');
      this.connected = true;
    } catch (err) {
      debug('client connect failed...');
      this.clientInstance = null;
      this.connected = false;
    } finally {
      this.connecting = false;
    }
  }

  async onConnected(): Promise<any> {
    if (this.connected && this.clientInstance) {
      return this.clientInstance;
    }
    this.connected = false;
    this.clientInstance = null;
    const client = new Kafka.Client(this.options);
    return new Promise((resolve, reject) => {
      if (!client) {
        return reject('kafka instance create failed...');
      }
      client.on('ready', () => resolve(client));
    });
  }

  async close() {
    if (this.closing) {
      debug('close request ignored. client has been closed currently.');
      return;
    }
    this.closing = true;
    try {
      const result = await this.onClosed();
      this.connected = false;
      this.clientInstance = null;
      this.closing = false;
      this.connecting = false;
      debug('client close success...');
      return result;
    } catch (err) {
      this.closing = false;
      debug(`client close failed, err => ${JSON.stringify(err)}`);
      throw err;
    }
  }
  async onClosed(instance?: any) {
    if (!this.closing) {
      debug('close request ignored, closing status must be true...');
      return null;
    }
    if (!this.clientInstance) {
      this.connected = false;
      debug('close request refused. client has been closed currently.');
      return null;
    }
    return new Promise((resolve, reject) => {
      this.clientInstance.close(resolve);
    });
  }

  async checkOrConnect(funName: string) {
    debug(`check the client Connected`);
    if (!this.connected) {
      debug(`KafkaClient is not connected [${funName}]`);
      debug('KafkaClient start connecting ....');
      await this.connect();
    }
    debug(`KafkaClient connected before ${funName}`);
  }

  async init(options) {
    this.options = options;
    this.clientId = options.clientId;

    this.producerClients = new Map<string, any>();
    this.consumerClients = new Map<string, any>();
    this.consumerGroupClients = new Map<string, any>();
    this.consumerStreamClients = new Map<string, any>();
    this.consumerGroupStreamClients = new Map<string, any>();
    this.producerStreamClients = new Map<string, any>();
    this.offsetClients = new Map<string, any>();
    this.adminClients = new Map<string, any>();

    this.producerStreamTransforms = new Map<string, Transform>();
    this.consumerStreamTransforms = new Map<string, Transform>();
    this.consumerGroupStreamTransforms = new Map<string, Transform>();
  }

  async addProducerClient(producerId: string, options, isHighLevel: boolean = false, autoConnectCount: number = 0) {
    await this.checkExists(this.producerClients, producerId, 'producer');
    const producerClient = new ProducerClient(options, isHighLevel, autoConnectCount);
    this.producerClients.set(producerId, producerClient);
  }

  async getProducerClient(producerId) {
    return this.getMapItem(this.producerClients, producerId);
  }

  async getProducerClientMap() {
    return this.producerClients;
  }

  async addConsumerClient(consumerId, options, isHighLevel, autoReconnectCount) {
    await this.checkExists(this.consumerClients, consumerId, 'consumer');
    const consumerClient = new ConsumerClient(options, isHighLevel, autoReconnectCount);
    this.consumerClients.set(consumerId, consumerClient);
  }

  async getConsumerClient(consumerId) {
    return this.getMapItem(this.consumerClients, consumerId);
  }

  async getConsumerClientMap() {
    return this.consumerClients;
  }

  async addConsumerGroupClient(groupId, options, topics) {
    await this.checkExists(this.consumerGroupClients, groupId, 'ConsumerGroup');
    const consumerGroupClient = new ConsumerGroupClient(options, topics);
    this.consumerGroupClients.set(groupId, consumerGroupClient);
  }

  async getConsumerGroupClient(groupId) {
    return this.getMapItem(this.consumerGroupClients, groupId);
  }

  async getConsumerGroupClientMap() {
    return this.consumerGroupClients;
  }

  async addConsumerStreamClient(consumerStreamId, options, topics) {
    await this.checkExists(this.consumerStreamClients, consumerStreamId, 'ConsumerStream');
    const consumerStreamClient = new ConsumerStreamClient(this.clientInstance, topics, options);
    this.consumerStreamClients.set(consumerStreamId, consumerStreamClient);
  }

  async getConsumerStreamClient(consumerStreamId) {
    return this.getMapItem(this.consumerStreamClients, consumerStreamId);
  }

  async getConsumerStreamClientMap() {
    return this.consumerGroupClients;
  }

  async addConsumerGroupStreamClients(groupStreamId, options, topics) {
    await this.checkExists(this.consumerGroupStreamClients, groupStreamId, 'ConsumerGroupStream');
    const consumerGroupStreamClient = new ConsumerGroupStreamClient(options, topics);
    this.consumerGroupStreamClients.set(groupStreamId, consumerGroupStreamClient);
  }

  async getConsumerGroupStreamClient(groupStreamId) {
    return this.getMapItem(this.consumerGroupStreamClients, groupStreamId);
  }

  async getConsumerGroupStreamClientMap() {
    return this.consumerGroupStreamClients;
  }

  async addProducerStreamClient(producerStreamId, options, autoReconnectCount) {
    await this.checkExists(this.producerStreamClients, producerStreamId, 'ProducerStream');
    const producerStreamClient = new ProducerStreamClient(options, autoReconnectCount);
    this.producerStreamClients.set(producerStreamId, producerStreamClient);
  }

  async getProducerStreamClient(producerStreamId) {
    return this.getMapItem(this.producerStreamClients, producerStreamId);
  }

  async getProducerStreamClientMap() {
    return this.producerStreamClients;
  }

  async addOffsetClient(offsetId, options) {
    await this.checkExists(this.offsetClients, offsetId, 'Offset');
    const offsetClient = new OffsetClient(options);
    this.offsetClients.set(offsetId, offsetClient);
  }

  async getOffsetClient(offsetId) {
    return this.getMapItem(this.offsetClients, offsetId);
  }

  async getOffsetClientMap() {
    return this.offsetClients;
  }

  async addAdminClient(adminId, options) {
    await this.checkExists(this.adminClients, adminId, 'Admin');
    const adminClient = new AdminClient(this.clientInstance);
    this.adminClients.set(adminId, adminClient);
  }

  async getAdminClient(adminId) {
    return this.getMapItem(this.adminClients, adminId);
  }

  async getAdminClientMap() {
    return this.adminClients;
  }

  // producer method as follows:
  async sendProducerMessage(producerId, payloads) {
    const producerClient = await this.getProducerClient(producerId);
    if (!producerClient || !(producerClient instanceof ProducerClient)) {
      throw new Error('producer not exists. you must add a producer...');
    }
    return producerClient.send(payloads);
  }

  async createProducerTopics(producerId, topics, async) {
    const producerClient = await this.getProducerClient(producerId);
    if (!producerClient || !(producerClient instanceof ProducerClient)) {
      throw new Error('producer not exists. you must add a producer...');
    }
    return producerClient.createTopics(topics, async);
  }

  // consumer method as follows:
  async pauseConsumer(consumerId) {
    const consumerClient = await this.getConsumerClientInstance(consumerId);
    return consumerClient.pause();
  }

  async closeConsumer(consumerId, force) {
    const consumerClient = await this.getConsumerClientInstance(consumerId);
    return consumerClient.close(force);
  }

  async resumeConsumer(consumerId) {
    const consumerClient = await this.getConsumerClientInstance(consumerId);
    return consumerClient.resume();
  }

  async commitConsumer(consumerId, force) {
    const consumerClient = await this.getConsumerClientInstance(consumerId);
    return consumerClient.commit(force);
  }

  async addConsumerTopics(consumerId, topics, fromOffset) {
    const consumerClient = await this.getConsumerClientInstance(consumerId);
    return consumerClient.addTopics(topics, fromOffset);
  }

  async removeConsumerTopics(consumerId, topics) {
    const consumerClient = await this.getConsumerClientInstance(consumerId);
    return consumerClient.removeTopics(topics);
  }

  async setConsumerOffset(consumerId, topic, partition, offset) {
    const consumerClient = await this.getConsumerClientInstance(consumerId);
    return consumerClient.setOffset(topic, partition, offset);
  }

  async pauseConsumerTopic(consumerId, topics) {
    const consumerClient = await this.getConsumerClientInstance(consumerId);
    return consumerClient.pauseTopic(topics);
  }

  async resumeConsumerTopics(consumerId, topics) {
    const consumerClient = await this.getConsumerClientInstance(consumerId);
    return consumerClient.resumeTopics(topics);
  }

  async consumeMessage(consumerId, handler) {
    const consumerClient = await this.getConsumerClientInstance(consumerId);
    return consumerClient.consumeMessage(handler);
  }

  async consumeOffsetOutOfRange(consumerId, offsetHandler) {
    const consumerClient = await this.getConsumerClientInstance(consumerId);
    return consumerClient.consumeOffsetOutOfRange(offsetHandler);
  }

  async handleConsumerError(consumerId, errorHandler) {
    const consumerClient = await this.getConsumerClientInstance(consumerId);
    return consumerClient.handleError(errorHandler);
  }

  // consumerGroup method as follows:
  async sendConsumerGroupOffsetCommitRequest(groupId, commits) {
    const consumerGroupClient = await this.getConsumerGroupStreamClient(groupId);
    return consumerGroupClient.sendOffsetCommitRequest(commits);
  }

  async addConsumerGroupTopics(groupId, topics) {
    const consumerGroupClient = await this.getConsumerGroupStreamClient(groupId);
    return consumerGroupClient.addTopics(topics);
  }

  async scheduleConsumerGroupReconnect(timeout: number) {
    const consumerGroupClient = await this.getConsumerGroupStreamClient(groupId);
    return consumerGroupClient.scheduleReconnect(timeout);
  }

  // producerStream method as follows:
  async setProducerTransform(transformName, transFormOptions) {
    await this.checkExists(this.producerStreamTransforms, transformName, 'ProducerStreamTransform');
    const transform = new Transform(transFormOptions);
    this.producerStreamTransforms.set(transformName, transform);
  }

  async updateProducerTransform(transformName, transFormOptions) {
    const transform = new Transform(transFormOptions);
    this.producerStreamTransforms.set(transformName, transform);
  }

  async deleteProducerTransform(transformName) {
    return this.deleteMapItem(this.producerStreamTransforms, transformName);
  }

  async sendStream(streamSource: Readable, producerId, transformName = producerId) {
    if (!streamSource) {
      throw new Error('streamSource not exists...');
    }
    const transform = await this.getMapItem(this.producerStreamTransforms, transformName);
    const producerStreamClient = await this.getProducerStreamClient(producerId);
    const producerStream = await producerStreamClient.getProducerStream();
    if (!producerStream) {
      throw new Error(`[producerStream:${producerId} not exists.`);
    }
    if (!transform) {
      throw new Error(`[producerStream:${transformName}] transform exists.`);
    }
    return streamSource.pipe(transform).pipe(producerStream);
  }

  // consumerStream method as follows:
  async createConsumerCommitStream(consumerStreamId, options) {
    const consumerStreamClient = await this.getMapItem(this.consumerStreamClients, consumerStreamId);
    return consumerStreamClient.createCommitStream(options);
  }

  async consumeStreamMessage(consumerStreamId, handler) {
    const consumerStreamClient = await this.getMapItem(this.consumerStreamClients, consumerStreamId);
    return consumerStreamClient.consumeStreamMessage(handler);
  }
  // consumerGroupStream method as follows:
  async consumeGroupStream(groupStreamId, destStream, transformName = groupStreamId) {
    if (!destStream) {
      throw new Error('destStream not exists...');
    }
    const transform = await this.getMapItem(this.consumerGroupStreamTransforms, transformName);
    const consumerGroupStreamClient = await this.getConsumerGroupStreamClient(groupStreamId);
    const consumerGroupStream = await consumerGroupStreamClient.getConsumerGroupStream();
    if (!consumerGroupStream) {
      throw new Error(`[consumerGroupStream:${groupStreamId} not exists.`);
    }
    if (!transform) {
      throw new Error(`[consumerGroupStream transform:${transformName}] not exists.`);
    }
    return consumerGroupStream.pipe(transform).pipe(destStream);
  }

  // offset method as follows:
  async offsetClose(offsetId) {
    const offsetClient = await this.getOffsetClient(offsetId);
    return offsetClient.close();
  }

  async offsetFetch(offsetId, payloads: any) {
    const offsetClient = await this.getOffsetClient(offsetId);
    return offsetClient.fetch(payloads);
  }

  async offsetCommit(offsetId, groupId, payloads) {
    const offsetClient = await this.getOffsetClient(offsetId);
    return offsetClient.commit(groupId, payloads);
  }

  async offsetFetchCommits(offsetId, groupId, payloads) {
    const offsetClient = await this.getOffsetClient(offsetId);
    return offsetClient.fetchCommits(groupId, payloads);
  }

  async fetchLatestOffsets(offsetId, topics) {
    const offsetClient = await this.getOffsetClient(offsetId);
    return offsetClient.fetchLatestOffsets(topics);
  }

  async fetchEarliestOffsets(offsetId, topics) {
    const offsetClient = await this.getOffsetClient(offsetId);
    return offsetClient.fetchEarliestOffsets(topics);
  }

  // Admin method as follows:
  async listGroups(adminId) {
    const adminClient = await this.getAdminClient(adminId);
    return adminClient.listGroups();
  }

  async describeGroups(adminId, consumerGroups) {
    const adminClient = await this.getAdminClient(adminId);
    return adminClient.describeGroups(consumerGroups);
  }

  // private functions as follows:
  private async getConsumerClientInstance(consumerId): Promise<any> {
    const consumerClient = await this.getConsumerClient(consumerId);
    if (!consumerClient || !(consumerClient instanceof ConsumerClient)) {
      throw new Error('consumer not exists. you must add a consumer...');
    }
    return consumerClient;
  }

  private async checkExists(mapInstance, primaryId, entityType) {
    await this.checkMapInstanceExists(mapInstance);
    if (mapInstance.has(primaryId)) {
      debug(`add ${entityType} request refused. ${entityType}: ${primaryId} exists...`);
      throw new Error(`${entityType}: ${primaryId} exists...`);
    }
    return;
  }

  private async getMapItem(mapInstance, primaryId) {
    await this.checkMapInstanceExists(mapInstance);
    if (mapInstance.has(primaryId)) {
      return mapInstance.get(primaryId);
    }
    return null;
  }

  private async deleteMapItem(mapInstance, primaryId) {
    if (mapInstance.has(primaryId)) {
      return mapInstance.delete(primaryId);
    }
    return false;
  }

  private async checkMapInstanceExists(mapInstance) {
    if (!mapInstance && mapInstance instanceof Map) {
      throw new Error('mapInstance must be map instance...');
    }
    return;
  }

}
