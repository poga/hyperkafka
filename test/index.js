const hyperdrive = require('hyperdrive')
const memdb = require('memdb')
const tape = require('tape')
const hk = require('..')
const collect = require('collect-stream')
const fs = require('fs')
const protobuf = require('protocol-buffers')
const uint64be = require('uint64be')

const messages = protobuf(fs.readFileSync('index.proto'))

tape('write object message', function (t) {
  var drive = hyperdrive(memdb())
  var archive = drive.createArchive()
  var producer = hk.Producer(archive)

  producer.write('topic', {k: 'foo', v: 'bar'})

  producer.on('flush', function (size) {
    t.same(size, 37)

    archive.list((err, entries) => {
      t.error(err)
      t.same(entries.map(e => e.name), ['/topic/0.index', '/topic/0.log'])
      collect(archive.createFileReadStream('/topic/0.index'), (err, data) => {
        t.error(err)
        t.same(messages.Index.decode(data), {offset: 0, position: 0, size: 37})

        collect(archive.createFileReadStream('/topic/0.log'), (err, data) => {
          t.error(err)
          var message = messages.Message.decode(data)
          t.same(message.offset, 0)
          t.same(JSON.parse(message.payload), {k: 'foo', v: 'bar'})
        })

        t.end()
      })
    })
  })
})

tape('write string message', function (t) {
  var drive = hyperdrive(memdb())
  var archive = drive.createArchive()
  var producer = hk.Producer(archive)

  producer.write('topic', 'foo')

  producer.on('flush', function (size) {
    t.same(size, 19)

    archive.list((err, entries) => {
      t.error(err)
      t.same(entries.map(e => e.name), ['/topic/0.index', '/topic/0.log'])
      collect(archive.createFileReadStream('/topic/0.index'), (err, data) => {
        t.error(err)
        t.same(messages.Index.decode(data), {offset: 0, position: 0, size: 19})

        collect(archive.createFileReadStream('/topic/0.log'), (err, data) => {
          t.error(err)
          var message = messages.Message.decode(data)
          t.same(message.offset, 0)
          t.same(message.payload.toString(), 'foo')
        })

        t.end()
      })
    })
  })
})

tape('write buffer message', function (t) {
  var drive = hyperdrive(memdb())
  var archive = drive.createArchive()
  var producer = hk.Producer(archive)

  producer.write('topic', new Buffer('foo'))

  producer.on('flush', function (size) {
    t.same(size, 19)

    archive.list((err, entries) => {
      t.error(err)
      t.same(entries.map(e => e.name), ['/topic/0.index', '/topic/0.log'])
      collect(archive.createFileReadStream('/topic/0.index'), (err, data) => {
        t.error(err)
        t.same(messages.Index.decode(data), {offset: 0, position: 0, size: 19})

        collect(archive.createFileReadStream('/topic/0.log'), (err, data) => {
          t.error(err)
          var message = messages.Message.decode(data)
          t.same(message.offset, 0)
          t.same(message.payload.toString(), 'foo')
        })

        t.end()
      })
    })
  })
})

tape('write same segment multiple times', function (t) {
  var drive = hyperdrive(memdb())
  var archive = drive.createArchive()
  var producer = hk.Producer(archive)

  producer.write('topic', 'foo')
  // wait one lingering interval (10ms)
  setTimeout(() => {
    producer.write('topic', 'bar')
  }, 100)

  var firstFlush = true

  // should flush twice
  producer.on('flush', function (flushed) {
    if (firstFlush) {
      t.same(flushed, 19)
      firstFlush = false
      return
    }

    t.same(flushed, 38)
    archive.list((err, entries) => {
      t.error(err)
      t.same(Array.from(new Set(entries.map(e => e.name))), ['/topic/0.index', '/topic/0.log'])
      var consumer = hk.Consumer(archive)
      consumer.get('topic', 0, (err, msg) => {
        t.error(err)
        t.equal(msg.offset, 0)
        t.same(msg.payload.toString(), 'foo')
        consumer.get('topic', 1, (err, msg) => {
          t.error(err)
          t.equal(msg.offset, 1)
          t.same(msg.payload.toString(), 'bar')
          t.end()
        })
      })
    })
  })
})

tape('next segment', function (t) {
  var drive = hyperdrive(memdb())
  var archive = drive.createArchive()
  var producer = hk.Producer(archive, {segmentSize: 0})

  producer.write('topic', 'foo')
  producer.once('flush', function (flushed) {
    t.same(flushed, 19)

    producer.write('topic', 'bar')

    producer.once('flush', function (flushed) {
      t.same(flushed, 19)

      archive.list((err, entries) => {
        t.error(err)
        t.same(entries.map(e => e.name), ['/topic/0.index', '/topic/0.log', '/topic/1.index', '/topic/1.log'])
        collect(archive.createFileReadStream('/topic/1.index'), (err, data) => {
          t.error(err)
          t.same(messages.Index.decode(data), {offset: 1, position: 0, size: 19})

          collect(archive.createFileReadStream('/topic/1.log'), (err, data) => {
            t.error(err)
            var message = messages.Message.decode(data)
            t.same(message.offset, 1)
            t.same(message.payload.toString(), 'bar')
            t.end()
          })
        })
      })
    })
  })
})

tape('subscribe', function (t) {
  var drive = hyperdrive(memdb())
  var archive = drive.createArchive()
  var producer = hk.Producer(archive)

  producer.write('topic', 'foo')

  var consumer = hk.Consumer(archive)
  var i = 0
  var check = ['foo', 'bar']
  consumer.subscribe('topic', 0, (err, msg) => {
    t.error(err)
    t.same(msg.payload.toString(), check[i])
    i += 1

    if (i === 2) t.end()
  })
  setTimeout(() => {
    producer.write('topic', 'bar')
  }, 200)

  producer.once('flush', (flushed) => {
    t.equal(flushed, 19)
  })
})

tape('subscribe future offset', function (t) {
  var drive = hyperdrive(memdb())
  var archive = drive.createArchive()
  var producer = hk.Producer(archive)

  producer.write('topic', 'foo')

  var consumer = hk.Consumer(archive)
  consumer.subscribe('topic', 2, (err, msg) => {
    t.error(err)
    t.same(msg.payload.toString(), 'baz')
    t.end()
  })
  producer.write('topic', 'bar')
  producer.write('topic', 'baz')
})

tape('multi-topic topic not exists', function (t) {
  var drive = hyperdrive(memdb())
  var archive = drive.createArchive()
  var producer = hk.Producer(archive)

  producer.write('topic', 'foo')

  var consumer = hk.Consumer(archive)
  producer.on('flush', () => {
    consumer.get('topic2', 0, (err, msg) => {
      t.same(err.message, 'offset not found')
      t.end()
    })
  })
})

tape('multi-topic producer', function (t) {
  var drive = hyperdrive(memdb())
  var archive = drive.createArchive()
  var producer = hk.Producer(archive)

  producer.write('topic', 'foo')
  producer.write('topic2', 'bar')

  var checks = {
    topic: false,
    topic2: false
  }
  producer.on('flush', (flushed, topic) => {
    if (checks[topic]) return t.fail(`${topic} should not flush twice`)
    checks[topic] = true
    t.same(flushed, 19)

    if (checks.topic && checks.topic2) t.end()
  })
})

tape('multi-topic consumer', function (t) {
  var drive = hyperdrive(memdb())
  var archive = drive.createArchive()
  var producer = hk.Producer(archive)

  producer.write('topic', 'foo')
  producer.write('topic2', 'bar')

  // wait topic2 flushed
  producer.on('flush', (flushed, topic) => {
    if (topic !== 'topic2') return

    var consumer = hk.Consumer(archive)
    consumer.get('topic', 0, (err, msg) => {
      t.error(err)
      t.same(msg.offset, 0)
      t.same(msg.payload.toString(), 'foo')
      consumer.get('topic2', 0, (err, msg) => {
        t.error(err)
        t.same(msg.offset, 0)
        t.same(msg.payload.toString(), 'bar')
        t.end()
      })
    })
  })
})

tape('createReadStream', function (t) {
  var drive = hyperdrive(memdb())
  var archive = drive.createArchive()
  var producer = hk.Producer(archive)

  var consumer = hk.Consumer(archive)
  var rs = consumer.createReadStream('topic', 0)

  var check = ['foo', 'bar']
  rs.on('data', msg => {
    t.same(msg.payload.toString(), check[msg.offset])
    if (msg.offset === 1) t.end()
  })
  producer.write('topic', 'foo')
  producer.write('topic', 'bar')
})

tape('timestamp', function (t) {
  var drive = hyperdrive(memdb())
  var archive = drive.createArchive()
  var producer = hk.Producer(archive)

  var consumer = hk.Consumer(archive)
  var rs = consumer.createReadStream('topic', 0)

  var ts = Date.now()
  rs.once('data', msg => {
    t.same(msg.payload.toString(), 'foo')
    t.same(uint64be.decode(msg.timestamp), ts)
    t.end()
  })
  producer.write('topic', 'foo', uint64be.encode(ts))
})
