# hyperkafka

![stability-experimental](https://img.shields.io/badge/stability-experimental-orange.svg?style=flat-square)

A decentralized messaging platform.

## Usage

```js
// producer
var drive = hyperdrive(memdb())
var archive = drive.createArchive()
swarm(archive)

var producer = hyperkafka.Producer(archive)
producer.write('topic', 'key', 'value')


// somewhere, another machine
var archive2 = drive.createArchive(archive.key)
swarm(archive2)
var consumer = hyperkafka.Consumer(archive2)
consumer.get('topic', 0, function (err, msg) {
  // msg === {offset: 0, ts: timestamp, payload: {k: 'key', v: 'value'}}
})
```

## License

The MIT License
