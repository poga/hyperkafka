const hyperdrive = require('hyperdrive')
const memdb = require('memdb')
const tape = require('tape')
const hk = require('..')

tape('list topics', function (t) {
  var drive = hyperdrive(memdb())
  var archive = drive.createArchive()
  var producer = hk.Producer(archive)
  var consumer = hk.Consumer(archive)

  producer.write('topic', {k: 'foo', v: 'bar'})
  producer.write('topic2', {k: 'foo', v: 'bar'})

  producer.once('flush', function () {
    producer.topics((err, topics) => {
      t.error(err)
      t.same(topics, ['topic', 'topic2'])

      consumer.topics((err, toics) => {
        t.error(err)
        t.same(topics, ['topic', 'topic2'])
        t.end()
      })
    })
  })
})
