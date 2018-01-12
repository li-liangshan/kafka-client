import test from 'ava';
import * as Kafka from 'kafka-node';
import * as Debug from 'debug';
import * as _ from 'lodash';
import { ProducerStreamClient } from '../src/ProducerStreamClient';
import { Transform } from 'stream';

const debug = Debug('LLS:testSpec');

const url = 'localhost:2181/';
test.beforeEach(t => {
  debug('ava producerStreamClient test start');
});

test.afterEach(t => {
  debug('ava producerStreamClient test end');
});

test('producerStreamClient status on connected | closed', async (t) => {
  const producerStreamClient = new ProducerStreamClient({ });
  await producerStreamClient.connect();
  t.true(producerStreamClient.isConnected());
  await producerStreamClient.close();
  t.false(producerStreamClient.isConnected());
});

test('producerStreamClient send stream successful', async (t) => {
  const producerStreamClient = new ProducerStreamClient();
  await producerStreamClient.connect();
  t.true(producerStreamClient.isConnected());
  const stdinTransform = new Transform({
    objectMode: true,
    decodeStrings: true,
    transform(msg, encoding, callback) {
      const text = _.trim(msg.toString());
      debug(`pushing message ${text} to ExampleTopic`);
      callback(null, {
        topic: 'ExampleTopic',
        messages: text,
      });
      t.pass();
    },
  });
  const consumerOptions = {
    host: '127.0.0.1:2181',
    groupId: 'ExampleTestGroup',
    sessionTimeout: 15000,
    protocol: ['roundrobin'],
    fromOffset: 'latest', // equivalent of auto.offset.reset valid values are 'none', 'latest', 'earliest'
  };
  const consumerGroup = new Kafka.ConsumerGroup(consumerOptions, ['topic-named']);
  await new Promise((resolve, reject) => {
    consumerGroup.on('connect', () => {
      resolve();
    });
  });

  const message = await new Promise((resolve, reject) => {
    consumerGroup.on('message', (message) => {
      resolve(message);
    });
  });
  debug(`consumerGroup message => ${JSON.stringify(message)}`);

  const producerStream = await producerStreamClient.getProducerStream();
  const wrote = producerStream.write({
    topic: 'topic-named',
    message: 'world',
  });
  debug(`producerStream wrote => ${wrote}`);
  t.true(wrote);
  t.pass();
});
