const path = require('path')
const collect = require('collect-stream')
const protobuf = require('protocol-buffers')
const fs = require('fs')
const read = require('hyperdrive-read')

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

