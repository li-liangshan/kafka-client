import test from 'ava';
import * as Kafka from 'kafka-node';
import * as Debug from 'debug';
import * as _ from 'lodash';
import { ConsumerStreamClient } from '../src/ConsumerStreamClient';
import { ProducerClient } from '../src/ProducerClient';
import { Transform } from 'stream';

const debug = Debug('LLS:testSpec:ConsumerStreamClient');

const url = 'localhost:2181/';
const existTopics = ['topic1'];
const numberOfMessages = 100;
const options = { groupId: 'groupId', fetchMaxBytes: 512 };

test.beforeEach(t => {
  debug('ava consumerStreamClient test start');
});

test.afterEach(t => {
  debug('ava consumerStreamClient test end');
});

test('consumerStreamClient status on connected | closed', async (t) => {
  const client = new Kafka.Client(url);
  const consumerStreamClient = new ConsumerStreamClient(client, existTopics, options);
  await consumerStreamClient.connect();
  t.true(consumerStreamClient.isConnected());
  await consumerStreamClient.close();
  t.false(consumerStreamClient.isConnected());
});

test('consumerStreamClient should emit a message', async (t) => {
  const client = new Kafka.Client(url);
  const producerClient = new ProducerClient({ client });
  await producerClient.connect();
  const payloads = [
    { topic: 'topic-node', message: 'Iyyyy', partition: 0 },
  ];
  try {
    await producerClient.createTopics(['topic-node'], true);
    const data = await producerClient.send(payloads);
    debug(`data ===========================> ${JSON.stringify(data)}`);
    const consumerStreamClient = new ConsumerStreamClient(client, ['topic-node'], options);
    await consumerStreamClient.connect();
    debug(`cms ======= ${consumerStreamClient.isConnected()}`);
    const consumerStream = await consumerStreamClient.getConsumerStream();
    const consumeHandler = (message) => {
      debug(`consume-stream-message ===>=> ${JSON.stringify(message)}`);
    };
    await consumerStreamClient.createCommitStream();
    const message = await consumerStreamClient.consumeStreamMessage(consumeHandler);
    debug(`consume-stream-message ====> ${JSON.stringify(message)}`);
    t.pass();
  } catch (err) {
    debug(`error ======> ${JSON.stringify(err)}`);
    t.fail();
  }
});
