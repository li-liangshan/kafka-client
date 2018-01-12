import test from 'ava';
import { ConsumerClient } from '../src/ConsumerClient';
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

test('consumerClient status on connected with different topics, same partitions', async (t) => {
  const client = new Kafka.Client(url);
  const consumerClient = new ConsumerClient({ client, topics: [
    {
      topic: 'topic1',
      partitions: 0,
    },
    {
      topic: 'topic2',
      partitions: 0,
    },
  ], options: null});
  await consumerClient.connect();
  t.true(consumerClient.isConnected());
  await consumerClient.close();
  t.false(consumerClient.isConnected());
});

test('consumerClient status on connected with different topics, different partitions', async (t) => {
  const client = new Kafka.Client(url);
  const consumerClient = new ConsumerClient({ client, topics: [
    {
      topic: 'topic1',
      partitions: 0,
    },
    {
      topic: 'topic2',
      partitions: 1,
    },
  ], options: null});
  await consumerClient.connect();
  t.true(consumerClient.isConnected());
  await consumerClient.close();
  t.false(consumerClient.isConnected());
});

test('consumerClient status on connected with same topics, different partitions', async (t) => {
  const client = new Kafka.Client(url);
  const consumerClient = new ConsumerClient({ client, topics: [
    {
      topic: 'topic1',
      partitions: 0,
    },
    {
      topic: 'topic1',
      partitions: 1,
    },
  ], options: null});
  await consumerClient.connect();
  t.true(consumerClient.isConnected());
  await consumerClient.close();
  t.false(consumerClient.isConnected());
});

test('consumerClient status on connected with different topics, same partitions', async (t) => {
  const client = new Kafka.Client(url);
  const consumerClient = new ConsumerClient({ client, topics: [
    {
      topic: 'topic1',
      partitions: 0,
    },
    {
      topic: 'topic2',
      partitions: 0,
    },
  ], options: null});
  await consumerClient.connect();
  t.true(consumerClient.isConnected());
  await consumerClient.close();
  t.false(consumerClient.isConnected());
});

test('consumerClient status pause and resume', async (t) => {
  const client = new Kafka.Client(url);
  const consumerClient = new ConsumerClient({ client, topics: [
    {
      topic: 'topic1',
      partitions: 0,
    },
    {
      topic: 'topic2',
      partitions: 0,
    },
  ], options: null});
  await consumerClient.connect();
  // const message = await consumerClient.consumeMessage((message) => message);
  // debug(`message1234==>${JSON.stringify(message)}`);
  t.true(consumerClient.isConnected());
  await consumerClient.pause();
  const message1 = await consumerClient.consumeMessage((message) => debug(`message==>${message}`));
  debug(`message12345==>${JSON.stringify(message1)}`);
  await consumerClient.resume();
  const message2 = await consumerClient.consumeMessage((message) => debug(`message==>${message}`));
  debug(`message12345d6==>${JSON.stringify(message2)}`);
  await consumerClient.close();
  t.false(consumerClient.isConnected());
});

test('consumerClient commit', async (t) => {
  const client = new Kafka.Client(url);
  const consumerClient = new ConsumerClient({ client, topics: [
    {
      topic: 'topic1',
      partitions: 0,
    },
    {
      topic: 'topic2',
      partitions: 0,
    },
  ], options: {autoCommit: false}});
  await consumerClient.connect();
  t.true(consumerClient.isConnected());
  const commit = await consumerClient.commit();
  debug(`commit1==>${JSON.stringify(commit)}`);
  await consumerClient.close();
  t.false(consumerClient.isConnected());
});

test('consumerClient addTopics topic3 not exists', async (t) => {
  const client = new Kafka.Client(url);
  const consumerClient = new ConsumerClient({ client, topics: [
    {
      topic: 'topic1',
      partitions: 0,
    },
    {
      topic: 'topic2',
      partitions: 0,
    },
  ], options: {autoCommit: false}});
  try {
    await consumerClient.connect();
    t.true(consumerClient.isConnected());
    const addTopics = await consumerClient.addTopics('topic3');
    debug(`addTopics==>${JSON.stringify(addTopics)}`);
    await consumerClient.close();
    t.false(consumerClient.isConnected());
  } catch (err) {
    debug(`addTopics err => ${JSON.stringify(err)}`);
    t.pass();
  }
});

test('consumerClient addTopics topic1 exists', async (t) => {
  const client = new Kafka.Client(url);
  const consumerClient = new ConsumerClient({ client, topics: [
    {
      topic: 'topic1',
      partitions: 0,
    },
    {
      topic: 'topic2',
      partitions: 0,
    },
  ], options: {autoCommit: false}});
  try {
    await consumerClient.connect();
    t.true(consumerClient.isConnected());
    const addTopics = await consumerClient.addTopics('topic1');
    debug(`addTopics==>${JSON.stringify(addTopics)}`);
    await consumerClient.close();
    t.false(consumerClient.isConnected());
  } catch (err) {
    debug(`addTopics err => ${JSON.stringify(err)}`);
    t.pass();
  }
});

test('consumerClient removeTopics topic1 successful', async (t) => {
  const client = new Kafka.Client(url);
  const consumerClient = new ConsumerClient({ client, topics: [
    {
      topic: 'topic1',
      partitions: 0,
    },
    {
      topic: 'topic2',
      partitions: 0,
    },
  ], options: {autoCommit: false}});
  try {
    await consumerClient.connect();
    t.true(consumerClient.isConnected());
    const removeTopics = await consumerClient.removeTopics(['topic1']);
    debug(`removeTopics==>${JSON.stringify(removeTopics)}`);
    await consumerClient.close();
    t.false(consumerClient.isConnected());
  } catch (err) {
    debug(`removeTopics err => ${JSON.stringify(err)}`);
    t.fail();
  }
});

test('consumerClient setOffset successful', async (t) => {
  const client = new Kafka.Client(url);
  const consumerClient = new ConsumerClient({ client, topics: [
    {
      topic: 'topic1',
      partitions: 0,
    },
    {
      topic: 'topic2',
      partitions: 0,
    },
  ], options: {autoCommit: false}});
  try {
    await consumerClient.connect();
    t.true(consumerClient.isConnected());
    await consumerClient.setOffset('topic1', 0, 14);
    debug(`setOffset==>${JSON.stringify('setOffset')}`);
    await consumerClient.close();
    t.false(consumerClient.isConnected());
  } catch (err) {
    debug(`setOffset err => ${JSON.stringify(err)}`);
    t.fail();
  }
});
