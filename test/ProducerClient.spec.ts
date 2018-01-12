import test from 'ava';
import { ProducerClient } from '../src/ProducerClient';
import * as Kafka from 'kafka-node';
import * as Debug from 'debug';

const debug = Debug('LLS:testSpec');

const url = 'localhost:2181/';
test.beforeEach(t => {
  debug('ava test start');
});

test.afterEach(t => {
  debug('ava test end');
});
test('producerClient status on connected', async (t) => {
  const client = new Kafka.Client(url);
  const producerClient = new ProducerClient({ client });
  await producerClient.connect();
  t.true(producerClient.isConnected());
  await producerClient.close();
  t.false(producerClient.isConnected());
});

test('producerClient send message topic different and partition same...', async (t) => {
  const client = new Kafka.Client(url);
  const producerClient = new ProducerClient({ client });
  const payloads = [
    { topic: 'topic1', message: 'I am topic one...', partitions: 0},
    { topic: 'topic2', message: 'I am topic two...', partitions: 0},
  ];
  try {
    debug(`payloads => ${JSON.stringify(payloads)}`);
    const data = await producerClient.send(payloads);
    debug(`data => ${JSON.stringify(data)}`);
    t.pass();
  } catch (err) {
    debug(err);
    t.fail();
  }
});

test('producerClient send message topic same and partition same...', async (t) => {
  const client = new Kafka.Client(url);
  const producerClient = new ProducerClient({ client });
  const payloads = [
    { topic: 'topic1', message: 'I am topic one...', partitions: 0},
    { topic: 'topic1', message: 'I am topic two...', partitions: 0},
  ];

  try {
    debug(`payloads => ${JSON.stringify(payloads)}`);
    const data = await producerClient.send(payloads);
    debug(`data => ${JSON.stringify(data)}`);
    t.pass();
  } catch (err) {
    debug(err);
    t.fail();
  }
});

test('producerClient send message topic same and partition different...', async (t) => {
  const client = new Kafka.Client(url);
  const producerClient = new ProducerClient({ client, requireAcks: 0 }, true);
  const payloads = [
    { topic: 'topic1', message: 'I am topic one...', partitions: 0},
    { topic: 'topic1', message: 'I am topic two...', partitions: 1},
  ];

  try {
    debug(`payloads => ${JSON.stringify(payloads)}`);
    const data = await producerClient.send(payloads);
    debug(`data => ${JSON.stringify(data)}`);
    t.pass();
  } catch (err) {
    debug(`err => ${JSON.stringify(err)}`);
    t.fail();
  }
});

test('producerClient create topics', async (t) => {
  const client = new Kafka.Client(url);
  const producerClient = new ProducerClient( { client });
  try {
    const topicName1 = await producerClient.createTopics(['yong', 'named'], true);
    debug(`topicsName1 ==> ${JSON.stringify(topicName1)}`);
    const topicName2 = await producerClient.createTopics(['yong', 'manand'], true);
    debug(`topicsName2 ==> ${JSON.stringify(topicName2)}`);
    t.pass();
  } catch (err) {
    t.fail();
  }
});
