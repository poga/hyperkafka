const path = require('path')
const collect = require('collect-stream')
const protobuf = require('protocol-buffers')
const fs = require('fs')
const read = require('hyperdrive-read')
const _ = require('lodash')

const messages = protobuf(fs.readFileSync('index.proto'))

const INDEX_ITEM_SIZE = 15

module.exports = Consumer

function Consumer (archive) {
  if (!(this instanceof Consumer)) return new Consumer(archive)

  this._archive = archive

  // manage offset
  this._offset = 0
  this._currentSegmentOffset = 0
  this._currentPosition = 0
}

Consumer.prototype.get = function (topic, offset, cb) {
  this._archive.list((err, entries) => {
    if (err) return cb(err)

    var index
    var indexOffset = 0
    for (var i = 0; i < entries.length; i++) {
      var e = entries[i]
      if (e.name.endsWith('.index')) {
        let entryOffset = +path.basename(e.name, '.index')
        if (entryOffset <= offset && (e.length / INDEX_ITEM_SIZE) >= (offset - entryOffset + 1)) {
          index = e
          indexOffset = entryOffset
          break
        }
      }
    }
    if (!index) return cb(new Error('offset not found'))

    // read index
    var start = (offset - indexOffset) * INDEX_ITEM_SIZE
    var buf = new Buffer(INDEX_ITEM_SIZE)
    read(this._archive, index, buf, 0, INDEX_ITEM_SIZE, start, (err, bytesRead, buffer) => {
      if (err) return cb(err)

      var indexItem = messages.Index.decode(buf)

      // read message
      var msgBuf = new Buffer(indexItem.size) // size is the corresponding message sze
      read(this._archive, `/${topic}/${indexOffset}.log`, msgBuf, 0, indexItem.size, indexItem.position, (err, bytesRead, buffer) => {
        if (err) return cb(err)

        cb(null, messages.Message.decode(buffer))
      })
    })
  })
}

// should do random accessable format first so we can know how many message is in a segment
// without reading it. (via the size of index). so we know whether the offset we want is in the segment
Consumer.prototype.subscribe = function (topic, start, cb) {
  var currentOffset = start
  var self = this

  // 1. offset is monotonlically increse
  // 2. we can find the first segment to read with offset 0
  // 3. when we received a new segment whos init offset is next of our current offset, we switch to that segment
  // 4. when we received a segment update on our current segment, we reload segment to get new data and update our current offset
  var list
  function _read () {
    self.get(topic, currentOffset, (err, msg) => {
      if (err && err.message === 'offset not found') {
        if (list) return
        // topic is exhausted. wait new data and try again
        return _readLive()
      }
      if (err) return cb(err)
      cb(null, msg)

      currentOffset += 1
      process.nextTick(_read)
    })
  }

  function _readLive () {
    list = self._archive.list()
    list.on('data', x => {
      // wait log entry to make sure index and msg are both saved
      if (x.name.endsWith('.log')) {
        _read()
      }
    })
  }

  _read()
}
