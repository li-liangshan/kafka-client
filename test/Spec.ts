import * as Kafka from 'kafka-node';
import * as Debug from 'debug';
import * as _ from 'lodash';
import { ConsumerStreamClient } from '../src/ConsumerStreamClient';
import { Transform } from 'stream';

const debug = Debug('LLS:testSpec');

const url = 'localhost:2181/';
const existTopics = ['topic1', 'topic2'];
const numberOfMessages = 100;
const client = new Kafka.Client(url);
const producer = new Kafka.Producer(client);
const options = { autoCommit: false, groupId: 'groupId', fetchMaxBytes: 512 };

const testRun = async () => {
  const consumerStreamClient = new ConsumerStreamClient(client, existTopics, options);
  await consumerStreamClient.connect();
  consumerStreamClient.isConnected();
  new Promise((resolve, reject) => {
    producer.on('ready', () => {
      producer.createTopics(existTopics, () => {
        const messages = [];
        for (let i = 1; i <= numberOfMessages; i++) {
          messages.push('stream message ' + i);
        }
        producer.send([
          { topic: existTopics[0], messages },
          { topic: existTopics[1], messages: messages[numberOfMessages - 1] },
         ], (err, data) => {
          // if (err) {
          //   debug(`error ===> ${JSON.stringify(err)}`);
          //   return reject(err);
          // }
          const topics = [{topic: existTopics[0]}];
          resolve(data);
         });
      });
    });
  }).then((msg) => {
    debug(`send data ==>${JSON.stringify(msg)}`);
  }).catch((err) => {
    debug(`send data failed==>${JSON.stringify(err)}`);
  });
};

testRun();
