# hyperkafka

![stability-experimental](https://img.shields.io/badge/stability-experimental-orange.svg?style=flat-square)
[![NPM Version](https://img.shields.io/npm/v/hyperkafka.svg?style=flat-square)](https://www.npmjs.com/package/hyperkafka)
[![JavaScript Style Guide](https://img.shields.io/badge/code%20style-standard-brightgreen.svg?style=flat-square)](http://standardjs.com/)

A decentralized messaging platform.

`npm i hyperkafka`

## Usage

```js
// producer
var drive = hyperdrive(memdb())
var archive = drive.createArchive()

var producer = hyperkafka.Producer(archive)
producer.write('topic', 'hello')


// somewhere, another machine
var archive2 = drive.createArchive(archive.key)
var consumer = hyperkafka.Consumer(archive2)
consumer.get('topic', 0, function (err, msg) {
  // msg === {offset: 0, ts: timestamp, payload: 'hello'}
})

// create a readable stream
var rs = consumer.createReadStream('topic')
rs.on('data', msg => { })
```

## API

### Producer

#### `p = new Producer(archive)`

Create a new `Producer` with a [hyperdrive](https://github.com/mafintosh/hyperdrive) archive.

#### `p.write(topic, message, [timestamp])`

Append a message to a topic. `message` can be `Buffer`, `String`, or object.

You can specify timestamp for the message. The timestamp should be a `uint64be` buffer because there's no int64 support in javascript. By default it will be current time (`Date.now()`).

### Consumer

#### `c = new Consumer(archive)`

Create a new `Consumer` with a [hyperdrive](https://github.com/mafintosh/hyperdrive) archive.

#### `c.get(topic, offset, cb(err, message))`

Get a message on a topic with specified offset. Returns error if there's no message with given offset.

#### `c.createReadStream(topic, start)`

Get a readable stream with all messages in a topic after given offset.


## FAQ

#### This is just append-only log! Why not just use hypercore?

Overhead, batching, and topic.

If we simply push every single message as a seperated block into hypercore. The overhead is big: 144 bytes per message for 100000 messages. Therefore we need batching.

Also we need random-accessable message within a topic. We combine hyperdrive's built-in index and kafka storage format to achieve it.

#### This is basic messaging! How about MQTT/RabbitMQ/ZeroMQ...or any other message queue?

Message queues usually don't preserve message history. I want a messaging platform which can automatically archive all message in a decentralized environment.

#### Should I replace my apache kafka cluster with hyperkafka?

No. They have different goal and use cases. hyperkafka use the name because its internal storage is inspired by apache kafka, also it sounds cool.

If you want a private, highly-scalable messaging platform, use apache kafka.

If you want to store and share public realtime data, try hyperkafka. You might like it.

## License

The MIT License
