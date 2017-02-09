const hyperdrive = require('hyperdrive')
const memdb = require('memdb')
const tape = require('tape')
const hk = require('..')
const collect = require('collect-stream')

tape('write', function (t) {
  var drive = hyperdrive(memdb())
  var archive = drive.createArchive()
  var broker = hk.Broker(archive)

  broker.write('topic', 'foo', 'bar')
  broker._writeSegment('topic', (err) => {
    t.error(err)

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

tape('next segment', function (t) {
  var drive = hyperdrive(memdb())
  var archive = drive.createArchive()
  var broker = hk.Broker(archive)

  broker.write('topic', 'foo', 'bar')
  broker._writeSegment('topic', (err) => {
    t.error(err)
    broker._nextSegment('topic')
    broker.write('topic', 'foo', 'baz')

    broker._writeSegment('topic', (err) => {
      t.error(err)

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
  var broker = hk.Broker(archive)

  broker.write('topic', 'foo', 'bar')
  broker._writeSegment('topic', (err) => {
    t.error(err)
    broker._nextSegment('topic')
    broker.write('topic', 'foo', 'baz')

    broker._writeSegment('topic', (err) => {
      t.error(err)

      broker.get('topic', 0, (err, msg) => {
        t.error(err)
        t.equal(msg.offset, 0)
        t.same(msg.payload, {k: 'foo', v: 'bar'})
        broker.get('topic', 1, (err, msg) => {
          t.error(err)
          t.equal(msg.offset, 1)
          t.same(msg.payload, {k: 'foo', v: 'baz'})
          t.end()
        })
      })
    })
  })
})
