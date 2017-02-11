const hyperdrive = require('hyperdrive')
const memdb = require('memdb')
const tape = require('tape')
const hk = require('..')
const collect = require('collect-stream')
const fs = require('fs')
const protobuf = require('protocol-buffers')

const messages = protobuf(fs.readFileSync('index.proto'))

tape('write', function (t) {
  var drive = hyperdrive(memdb())
  var archive = drive.createArchive()
  var producer = hk.Producer(archive)

  producer.write('topic', 'foo', 'bar')

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

tape('write same segment multiple times', function (t) {
  var drive = hyperdrive(memdb())
  var archive = drive.createArchive()
  var producer = hk.Producer(archive)

  producer.write('topic', 'foo', 'bar')
  // wait one lingering interval (10ms)
  setTimeout(() => {
    producer.write('topic', 'foo', 'bar2')
  }, 100)

  var firstFlush = true

  // should flush twice
  producer.on('flush', function (flushed) {
    if (firstFlush) {
      t.same(flushed, 37)
      firstFlush = false
      return
    }

    t.same(flushed, 75)
    archive.list((err, entries) => {
      t.error(err)
      t.same(Array.from(new Set(entries.map(e => e.name))), ['/topic/0.index', '/topic/0.log'])
      var consumer = hk.Consumer(archive)
      consumer.get('topic', 0, (err, msg) => {
        t.error(err)
        t.equal(msg.offset, 0)
        t.same(JSON.parse(msg.payload), {k: 'foo', v: 'bar'})
        consumer.get('topic', 1, (err, msg) => {
          t.error(err)
          t.equal(msg.offset, 1)
          t.same(JSON.parse(msg.payload), {k: 'foo', v: 'bar2'})
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

  producer.write('topic', 'foo', 'bar')
  producer.once('flush', function (flushed) {
    t.same(flushed, 37)

    producer.write('topic', 'foo', 'baz')

    producer.once('flush', function (flushed) {
      t.same(flushed, 37)

      archive.list((err, entries) => {
        t.error(err)
        t.same(entries.map(e => e.name), ['/topic/0.index', '/topic/0.log', '/topic/1.index', '/topic/1.log'])
        collect(archive.createFileReadStream('/topic/1.index'), (err, data) => {
          t.error(err)
          t.same(messages.Index.decode(data), {offset: 1, position: 0, size: 37})

          collect(archive.createFileReadStream('/topic/1.log'), (err, data) => {
            t.error(err)
            var message = messages.Message.decode(data)
            t.same(message.offset, 1)
            t.same(JSON.parse(message.payload), {k: 'foo', v: 'baz'})
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

  producer.write('topic', 'foo', 'bar')

  var consumer = hk.Consumer(archive)
  var i = 0
  var check = [{k: 'foo', v: 'bar'}, {k: 'foo', v: 'baz'}]
  consumer.subscribe('topic', 0, (err, msg) => {
    t.error(err)
    t.same(JSON.parse(msg.payload), check[i])
    i += 1

    if (i === 2) t.end()
  })
  setTimeout(() => {
    producer.write('topic', 'foo', 'baz')
  }, 200)

  producer.once('flush', (flushed) => {
    t.equal(flushed, 37)
  })
})

tape('subscribe future offset', function (t) {
  var drive = hyperdrive(memdb())
  var archive = drive.createArchive()
  var producer = hk.Producer(archive)

  producer.write('topic', 'foo', 'bar')

  var consumer = hk.Consumer(archive)
  consumer.subscribe('topic', 2, (err, msg) => {
    t.error(err)
    t.same(JSON.parse(msg.payload), {k: 'foo', v: 'bar3'})
    t.end()
  })
  producer.write('topic', 'foo', 'bar2')
  producer.write('topic', 'foo', 'bar3')
})
