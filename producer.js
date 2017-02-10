const pump = require('pump')
const events = require('events')
const inherits = require('inherits')
const BufStream = require('stream-buffers').ReadableStreamBuffer
const _ = require('lodash')
const protobuf = require('protocol-buffers')
const fs = require('fs')
const uint64be = require('uint64be')

const messages = protobuf(fs.readFileSync('index.proto'))

module.exports = Producer

function Producer (archive, opts) {
  if (!(this instanceof Producer)) return new Producer(archive, opts)
  events.EventEmitter.call(this)

  this._opts = Object.assign({}, {linger: 10, segmentSize: 1024 * 1024}, opts)
  this._archive = archive

  // lingering
  this._writeSegment = _.debounce(this._writeSegmentNow, this._opts.linger)

  this.topics = {}
  // manage offset
  this._offset = 0
  this._currentSegmentOffset = 0
  this._currentPosition = 0
}

inherits(Producer, events.EventEmitter)

Producer.prototype.write = function (topic, key, value) {
  if (!this.topics[topic]) this._initTopic(topic)

  // prepare message
  var timestamp = Date.now()
  var payload = {k: key, v: value}
  var buf = messages.Message.encode({
    offset: this._offset,
    timestamp: uint64be.encode(timestamp),
    payload: new Buffer(JSON.stringify(payload))
  })
  this.topics[topic].log = Buffer.concat([this.topics[topic].log, buf])

  // prepare index
  var idxBuf = messages.Index.encode({
    offset: this._offset,
    position: this._currentPosition,
    size: buf.length
  })
  this.topics[topic].index = Buffer.concat([this.topics[topic].index, idxBuf])

  this._offset += 1
  this._currentPosition += buf.length

  this._writeSegment(topic)
}

// really write segment files into hyperdrive
Producer.prototype._writeSegmentNow = function (topic, cb) {
  var self = this
  writeIndex()

  function writeIndex () {
    var source = new BufStream()
    source.put(self.topics[topic].index)
    source.stop()
    var out = self._archive.createFileWriteStream(`/${topic}/${self._currentSegmentOffset}.index`)
    pump(source, out, err => {
      if (err) return cb(err)

      writeLog()
    })
  }

  function writeLog () {
    var source = new BufStream()
    source.put(self.topics[topic].log)
    source.stop()
    var out = self._archive.createFileWriteStream(`/${topic}/${self._currentSegmentOffset}.log`)
    pump(source, out, err => {
      if (err) return cb(err)

      var flushed = self.topics[topic].log.length
      if (self.topics[topic].log.length >= self._opts.segmentSize) {
        self._nextSegment(topic)
      }

      self.emit('flush', flushed)

      if (cb) return cb()
    })
  }
}

Producer.prototype._nextSegment = function (topic) {
  this._currentSegmentOffset = this._offset
  this._currentPosition = 0

  // reset topic buffers
  this._initTopic(topic)
}

Producer.prototype._initTopic = function (topic) {
  this.topics[topic] = { log: new Buffer(0), index: new Buffer(0) }
}
