const hyperdrive = require('hyperdrive')
const memdb = require('memdb')
const tape = require('tape')
const hk = require('..')
const swarm = require('hyperdiscovery')

tape('replicate', function (t) {
  var drive = hyperdrive(memdb())
  var archive = drive.createArchive()
  var sw1 = swarm(archive)
  var producer = hk.Producer(archive)

  producer.write('topic', 'foo', 'bar')
  producer._writeSegment('topic', (err) => {
    t.error(err)

    var drive2 = hyperdrive(memdb())
    var archive2 = drive2.createArchive(archive.key)
    var sw2 = swarm(archive2)

    var consumer = hk.Consumer(archive2)
    consumer.get('topic', 0, (err, msg) => {
      t.error(err)
      t.equal(msg.offset, 0)
      t.same(JSON.parse(msg.payload), {k: 'foo', v: 'bar'})

      sw1.close(function () {
        t.ok(1, 'sw1 closed')
        sw2.close(function () {
          t.ok(1, 'sw2 closed')
          t.end()
        })
      })
    })
  })
})

