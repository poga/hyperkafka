const pump = require('pump')
const events = require('events')
const inherits = require('inherits')
const BufStream = require('stream-buffers').ReadableStreamBuffer
const _ = require('lodash')
const protobuf = require('protocol-buffers')
const fs = require('fs')
const uint64be = require('uint64be')
const path = require('path')
const {listTopic} = require('./topic')

const messages = protobuf(fs.readFileSync(path.join(__dirname, 'index.proto')))

module.exports = Producer

const DEFAULT = {
  segmentSize: 5 * 1024 * 1024, // 5 MB
  linger: 10 // milliseconds
}

function Producer (archive, opts) {
  if (!(this instanceof Producer)) return new Producer(archive, opts)
  events.EventEmitter.call(this)

  this._opts = Object.assign({}, DEFAULT, opts)
  this._archive = archive

  this._topics = {}
  // manage offset
}

inherits(Producer, events.EventEmitter)

Producer.prototype.write = function (topic, msg, timestamp) {
  if (!this._topics[topic]) this._initTopic(topic)

  // prepare message
  var ts = timestamp || uint64be.encode(Date.now())
  var payload
  if (Buffer.isBuffer(msg)) {
    payload = msg
  } else if (typeof msg === 'string') {
    payload = new Buffer(msg)
  } else {
    payload = new Buffer(JSON.stringify(msg))
  }
  var buf = messages.Message.encode({
    offset: this._topics[topic].offset,
    timestamp: ts,
    payload: payload
  })
  this._topics[topic].log = Buffer.concat([this._topics[topic].log, buf])

  // prepare index
  var idxBuf = messages.Index.encode({
    offset: this._topics[topic].offset,
    position: this._topics[topic].currentPosition,
    size: buf.length
  })
  this._topics[topic].index = Buffer.concat([this._topics[topic].index, idxBuf])

  this._topics[topic].offset += 1
  this._topics[topic].currentPosition += buf.length

  this._topics[topic].write()
}

// really write segment files into hyperdrive
Producer.prototype._writer = function (topic) {
  var self = this
  return () => {
    writeIndex()

    function writeIndex () {
      var source = new BufStream()
      source.put(self._topics[topic].index)
      source.stop()
      var out = self._archive.createFileWriteStream(`/${topic}/${self._topics[topic].currentSegmentOffset}.index`)
      pump(source, out, err => {
        if (err) return self.emit('error', err)

        writeLog()
      })
    }

    function writeLog () {
      var source = new BufStream()
      source.put(self._topics[topic].log)
      source.stop()
      var out = self._archive.createFileWriteStream(`/${topic}/${self._topics[topic].currentSegmentOffset}.log`)
      pump(source, out, err => {
        if (err) return self.emit('error', err)

        var flushed = self._topics[topic].log.length
        if (self._topics[topic].log.length >= self._opts.segmentSize) {
          self._nextSegment(topic)
        }

        self.emit('flush', flushed, topic)
      })
    }
  }
}

Producer.prototype._nextSegment = function (topic) {
  this._topics[topic].currentSegmentOffset = this._topics[topic].offset
  this._topics[topic].currentPosition = 0

  // reset topic buffers and index, but preserve current offset
  this._topics[topic].log = new Buffer(0)
  this._topics[topic].index = new Buffer(0)
}

Producer.prototype._initTopic = function (topic) {
  this._topics[topic] = {
    log: new Buffer(0),
    index: new Buffer(0),
    write: _.debounce(this._writer(topic), this._opts.linger),
    offset: 0,
    currentSegmentOffset: 0,
    currentPosition: 0
  }
}

Producer.prototype.topics = function (cb) {
  listTopic(this._archive, cb)
}
