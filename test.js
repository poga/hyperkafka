const hyperdrive = require('hyperdrive')
const memdb = require('memdb')
const tape = require('tape')
const hk = require('.')
const collect = require('collect-stream')

tape('test', function (t) {
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
