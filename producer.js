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

const DEFAULT = {
  segmentSize: 5 * 1024 * 1024, // 5 MB
  linger: 10 // milliseconds
}

function Producer (archive, opts) {
  if (!(this instanceof Producer)) return new Producer(archive, opts)
  events.EventEmitter.call(this)

  this._opts = Object.assign({}, DEFAULT, opts)
  this._archive = archive

  this.topics = {}
  // manage offset
}

inherits(Producer, events.EventEmitter)

Producer.prototype.write = function (topic, msg, timestamp) {
  if (!this.topics[topic]) this._initTopic(topic)

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
    offset: this.topics[topic].offset,
    timestamp: ts,
    payload: payload
  })
  this.topics[topic].log = Buffer.concat([this.topics[topic].log, buf])

  // prepare index
  var idxBuf = messages.Index.encode({
    offset: this.topics[topic].offset,
    position: this.topics[topic].currentPosition,
    size: buf.length
  })
  this.topics[topic].index = Buffer.concat([this.topics[topic].index, idxBuf])

  this.topics[topic].offset += 1
  this.topics[topic].currentPosition += buf.length

  this.topics[topic].write()
}

// really write segment files into hyperdrive
Producer.prototype._writer = function (topic) {
  var self = this
  return () => {
    writeIndex()

    function writeIndex () {
      var source = new BufStream()
      source.put(self.topics[topic].index)
      source.stop()
      var out = self._archive.createFileWriteStream(`/${topic}/${self.topics[topic].currentSegmentOffset}.index`)
      pump(source, out, err => {
        if (err) return self.emit('error', err)

        writeLog()
      })
    }

    function writeLog () {
      var source = new BufStream()
      source.put(self.topics[topic].log)
      source.stop()
      var out = self._archive.createFileWriteStream(`/${topic}/${self.topics[topic].currentSegmentOffset}.log`)
      pump(source, out, err => {
        if (err) return self.emit('error', err)

        var flushed = self.topics[topic].log.length
        if (self.topics[topic].log.length >= self._opts.segmentSize) {
          self._nextSegment(topic)
        }

        self.emit('flush', flushed, topic)
      })
    }
  }
}

Producer.prototype._nextSegment = function (topic) {
  this.topics[topic].currentSegmentOffset = this.topics[topic].offset
  this.topics[topic].currentPosition = 0

  // reset topic buffers and index, but preserve current offset
  this.topics[topic].log = new Buffer(0)
  this.topics[topic].index = new Buffer(0)
}

Producer.prototype._initTopic = function (topic) {
  this.topics[topic] = {
    log: new Buffer(0),
    index: new Buffer(0),
    write: _.debounce(this._writer(topic), this._opts.linger),
    offset: 0,
    currentSegmentOffset: 0,
    currentPosition: 0
  }
}
