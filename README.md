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

#### `c = new Consumer(archive)`

Create a new `Consumer` with a [hyperdrive](https://github.com/mafintosh/hyperdrive) archive.

#### `c.get(topic, offset, cb(err, message))`

Get a message on a topic with specified offset. Returns error if there's no message with given offset.

#### `c.createReadStream(topic, start)`

Get a readable stream with all messages in a topic after given offset.


## License

The MIT License
