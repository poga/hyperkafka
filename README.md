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
  // msg === {offset: 0, ts: timestamp, payload: buffer('hello')}
})

// create a readable stream
var rs = consumer.createReadStream('topic')
rs.on('data', msg => { })
```

## API

### Producer

#### `p = new Producer(archive, opts)`

Create a new `Producer` with a [hyperdrive](https://github.com/mafintosh/hyperdrive) archive.

Option `opts` includes:

* segmentSize: the file size limit of a single segment.
* linger: the delay before flushing buffer into hyperdrive

#### `p.write(topic, message, [timestamp])`

Append a message to a topic. `message` can be `Buffer`, `String`, or object.

You can specify timestamp for the message. The timestamp should be a `uint64be` buffer because there's no int64 support in javascript. By default it will be current time (`Date.now()`).

#### `p.topics(cb(err, topics))`

Returns an array of topics

#### `p.on('flush', cb(segmentSize, flushedTopci))`

Producer will emit `flush` event when a segment is flushed into hyperdrive.

### Consumer

#### `c = new Consumer(archive)`

Create a new `Consumer` with a [hyperdrive](https://github.com/mafintosh/hyperdrive) archive.

#### `c.get(topic, offset, cb(err, message))`

Get a message on a topic with specified offset. Returns error if there's no message with given offset.

#### `c.createReadStream(topic, start)`

Get a readable stream with all messages in a topic after given offset.

#### `c.topics(cb(err, topics))`

Returns an array of topics


## FAQ

#### Why?

Event-sourcing is a popular pattern for building large-scale high-performance system. It's also a simple and extensible software architecture.

By utilizing the well-designed Kafka's API, we can make developing P2P app simpler and easier to reason.

#### This is just append-only log! Why not just use hypercore?

Hyperdrive's indexing allows us to query and insert data efficiently.

Also, If we simply push every single message as a seperated block into hypercore. The overhead is big: 144 bytes per message for 100000 messages. Therefore we need batching.

#### This is basic messaging! How about MQTT/RabbitMQ/ZeroMQ...or any other message queue?

Message queues usually don't preserve message history. I want a messaging platform which can automatically archive all message in a decentralized environment.

#### Should I replace my apache kafka cluster with hyperkafka?

No. They have different goal and use cases. hyperkafka use the name because its internal storage is inspired by apache kafka, also it sounds cool.

If you want a private, highly-scalable messaging platform, use apache kafka.

If you want to store and share public realtime data, try hyperkafka. You might like it.

## License

The MIT License
