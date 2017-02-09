const hyperdrive = require('hyperdrive')
const memdb = require('memdb')
const tape = require('tape')
const hk = require('..')
const collect = require('collect-stream')

tape('write', function (t) {
  var drive = hyperdrive(memdb())
  var archive = drive.createArchive()
  var producer = hk.Producer(archive)

  producer.write('topic', 'foo', 'bar')

  producer.on('flush', function (size) {
    t.same(size, 64)

    archive.list((err, entries) => {
      t.error(err)
      t.same(entries.map(e => e.name), ['/topic/0.index', '/topic/0.log'])
      collect(archive.createFileReadStream('/topic/0.index'), (err, data) => {
        t.error(err)
        t.same(JSON.parse(data), {offset: 0, pos: 0})

        collect(archive.createFileReadStream('/topic/0.log'), (err, data) => {
          t.error(err)
          var message = JSON.parse(data)
          t.same(message.offset, 0)
          t.same(message.payload, {k: 'foo', v: 'bar'})
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
    producer.write('topic', 'foo', 'baz')
  }, 100)

  var firstFlush = true

  // should flush twice
  producer.on('flush', function (flushed) {
    if (firstFlush) {
      t.same(flushed, 64)
      firstFlush = false
      return
    }

    t.same(flushed, 128)
    archive.list((err, entries) => {
      t.error(err)
      t.same(Array.from(new Set(entries.map(e => e.name))), ['/topic/0.index', '/topic/0.log'])
      var consumer = hk.Consumer(archive)
      consumer.get('topic', 0, (err, msg) => {
        t.error(err)
        t.equal(msg.offset, 0)
        t.same(msg.payload, {k: 'foo', v: 'bar'})
        consumer.get('topic', 1, (err, msg) => {
          t.error(err)
          t.equal(msg.offset, 1)
          t.same(msg.payload, {k: 'foo', v: 'baz'})
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
    t.same(flushed, 64)

    producer.write('topic', 'foo', 'baz')

    producer.once('flush', function (flushed) {
      t.same(flushed, 64)

      archive.list((err, entries) => {
        t.error(err)
        t.same(entries.map(e => e.name), ['/topic/0.index', '/topic/0.log', '/topic/1.index', '/topic/1.log'])
        collect(archive.createFileReadStream('/topic/1.index'), (err, data) => {
          t.error(err)
          t.same(JSON.parse(data), {offset: 1, pos: 0})

          collect(archive.createFileReadStream('/topic/1.log'), (err, data) => {
            t.error(err)
            var message = JSON.parse(data)
            t.same(message.offset, 1)
            t.same(message.payload, {k: 'foo', v: 'baz'})
          })

          t.end()
        })
      })
    })
  })
})

tape('get', function (t) {
  var drive = hyperdrive(memdb())
  var archive = drive.createArchive()
  var producer = hk.Producer(archive)

  producer.write('topic', 'foo', 'bar')
  producer._writeSegment('topic', (err) => {
    t.error(err)
    producer._nextSegment('topic')
    producer.write('topic', 'foo', 'baz')

    producer._writeSegment('topic', (err) => {
      t.error(err)

      var consumer = hk.Consumer(archive)
      consumer.get('topic', 0, (err, msg) => {
        t.error(err)
        t.equal(msg.offset, 0)
        t.same(msg.payload, {k: 'foo', v: 'bar'})
        consumer.get('topic', 1, (err, msg) => {
          t.error(err)
          t.equal(msg.offset, 1)
          t.same(msg.payload, {k: 'foo', v: 'baz'})
          t.end()
        })
      })
    })
  })
})
