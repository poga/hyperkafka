const hyperdrive = require('hyperdrive')
const memdb = require('memdb')
const tape = require('tape')
const hk = require('..')
const swarm = require('hyperdiscovery')

tape('replicate', function (t) {
  var drive = hyperdrive(memdb())
  var archive = drive.createArchive()
  var sw1 = swarm(archive)
  var broker = hk.Broker(archive)

  broker.write('topic', 'foo', 'bar')
  broker._writeSegment('topic', (err) => {
    t.error(err)

    var drive2 = hyperdrive(memdb())
    var archive2 = drive2.createArchive(archive.key)
    var sw2 = swarm(archive2)

    var broker2 = hk.Broker(archive2)
    broker2.get('topic', 0, (err, msg) => {
      t.error(err)
      t.equal(msg.offset, 0)
      t.same(msg.payload, {k: 'foo', v: 'bar'})
      sw1.close()
      sw2.close()
      t.end()
    })
  })
})
