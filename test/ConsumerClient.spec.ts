import test from 'ava';
import { ConsumerClient } from '../src/ConsumerClient';
import * as Kafka from 'kafka-node';
import * as Debug from 'debug';
import { ProducerClient } from '../src/ProducerClient';

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
    { topic: 'topic1', partition: 0 },
    { topic: 'topic2', partition: 0 },
  ], options: null});
  await consumerClient.connect();
  t.true(consumerClient.isConnected());
  await consumerClient.close();
  t.false(consumerClient.isConnected());
});

test('consumerClient status on connected with different topics, different partitions', async (t) => {
  const client = new Kafka.Client(url);
  const consumerClient = new ConsumerClient({ client, topics: [
    { topic: 'topic1', partition: 0 },
    { topic: 'topic2', partition: 1 },
  ], options: null});
  await consumerClient.connect();
  t.true(consumerClient.isConnected());
  await consumerClient.close();
  t.false(consumerClient.isConnected());
});

test('consumerClient status on connected with same topics, different partitions', async (t) => {
  const client = new Kafka.Client(url);
  const consumerClient = new ConsumerClient({ client, topics: [
    { topic: 'topic1', partition: 0 },
    { topic: 'topic1', partition: 1 },
  ], options: null});
  await consumerClient.connect();
  t.true(consumerClient.isConnected());
  await consumerClient.close();
  t.false(consumerClient.isConnected());
});

test('consumerClient status on connected with different topics, same partitions', async (t) => {
  const client = new Kafka.Client(url);
  const consumerClient = new ConsumerClient({ client, topics: [
    { topic: 'topic1', partition: 0 },
    { topic: 'topic2', partition: 0 },
  ], options: null});
  await consumerClient.connect();
  t.true(consumerClient.isConnected());
  await consumerClient.close();
  t.false(consumerClient.isConnected());
});

test('consumerClient status pause and resume', async (t) => {
  const client = new Kafka.Client(url);
  const consumerClient = new ConsumerClient({ client, topics: [
    { topic: 'topic1', partition: 0 },
    { topic: 'topic2', partition: 0 },
  ], options: null});
  await consumerClient.connect();
  t.true(consumerClient.isConnected());
  await consumerClient.pause();
  await consumerClient.consumeMessage((message) => debug(`message==>${message}`));
  await consumerClient.resume();
  await consumerClient.consumeMessage((message) => debug(`message==>${message}`));
  await consumerClient.close();
  t.false(consumerClient.isConnected());
});

test('consumerClient commit', async (t) => {
  const client = new Kafka.Client(url);
  const consumerClient = new ConsumerClient({ client, topics: [
    { topic: 'topic1', partition: 0 },
    { topic: 'topic2', partition: 0 },
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
    { topic: 'topic1', partition: 0 },
    { topic: 'topic2', partition: 0 },
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
    { topic: 'topic1', partition: 0 },
    { topic: 'topic2', partition: 0 },
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
    { topic: 'topic1', partition: 0 },
    { topic: 'topic2', partition: 0 },
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
    { topic: 'topic1', partition: 0, offset: 0 },
    { topic: 'topic2', partition: 0, offset: 0 },
  ], options: {autoCommit: false}});

  await consumerClient.connect();
  t.true(consumerClient.isConnected());
  const consumer = await consumerClient.getConsumer();
  await consumerClient.setOffset('topic1', 0, 14);
  t.is(consumer.payloads[0].offset, 14);
  await consumerClient.setOffset('topic2', 0, 13);
  t.is(consumer.payloads[1].offset, 13);
  await consumerClient.close();
  t.false(consumerClient.isConnected());
});

test('consumerClient pauseTopic | resumeTopics successful', async (t) => {
  const client = new Kafka.Client(url);
  const consumerClient = new ConsumerClient({client, topics: [
    { topic: 'topic1', partition: 0 },
    { topic: 'topic2', partition: 0 },
  ], options: {} }); // groupId 数据格式注意
  await consumerClient.connect();
  t.true(consumerClient.isConnected());
  const consumer = await consumerClient.getConsumer();
  debug(`payloads pause before ===> ${JSON.stringify(consumer.payloads)}`);
  await consumerClient.pauseTopics(['topic1']);
  debug(`payloads pause after ===> ${JSON.stringify(consumer.payloads)}`);
  await consumerClient.resumeTopics(['topic1']);
  debug(`payloads resume ===> ${JSON.stringify(consumer.payloads)}`);
  await consumerClient.close();
  t.false(consumerClient.isConnected());
});

test('consumerClient getTopicPayloads successful', async (t) => {
  const client = new Kafka.Client(url);
  const consumerClient = new ConsumerClient({ client, topics: [
    { topic: 'topic1', partition: 0 },
    { topic: 'topic2', partition: 0 },
  ], options: {} }, false);
  await consumerClient.connect();
  t.true(consumerClient.isConnected());
  const beforeTopicPayloads = await consumerClient.getTopicPayloads();
  debug(`topicPayloads pause before ===> ${JSON.stringify(beforeTopicPayloads)}`);
  await consumerClient.pauseTopics(['topic1']);
  const afterTopicPayloads = await consumerClient.getTopicPayloads();
  debug(`topicPayloads pause after ===> ${JSON.stringify(afterTopicPayloads)}`);
  await consumerClient.resumeTopics(['topic1']);
  const resumeTopicPayloads = await consumerClient.getTopicPayloads();
  debug(`topicPayloads resume ===> ${JSON.stringify(resumeTopicPayloads)}`);
  await consumerClient.close();
  t.false(consumerClient.isConnected());
});

test('consumerClient consumeMessage successful', async (t) => {
  const client = new Kafka.Client(url);
  const consumerClient = new ConsumerClient({ client, topics: [
    { topic: 'topic1', partition: 0 },
    { topic: 'topic2', partition: 0 },
  ], options: {}});
  await consumerClient.connect();
  t.true(consumerClient.isConnected());
  const handler = (message) => {
    debug(`consume message =>${JSON.stringify(message)}`);
  };
  await consumerClient.consumeMessage(handler);
  await consumerClient.close();
  t.false(consumerClient.isConnected());
});

test('consumerClient consumeOffsetOutOfRange successful', async (t) => {
  const client = new Kafka.Client(url);
  const consumerClient = new ConsumerClient({ client, topics: [
    { topic: 'topic1', partition: 0 },
    { topic: 'topic2', partition: 0 },
  ], options: {}});
  await consumerClient.connect();
  t.true(consumerClient.isConnected());
  const offsetHandler = (error) => {
    debug(`consumeOffsetOutOfRange err =>${JSON.stringify(error)}`);
  };
  await consumerClient.consumeOffsetOutOfRange(offsetHandler);
  await consumerClient.close();
  t.false(consumerClient.isConnected());
});
