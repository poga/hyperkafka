const pump = require('pump')
const events = require('events')
const inherits = require('inherits')
const BufStream = require('stream-buffers').ReadableStreamBuffer
const ndjson = require('ndjson')
const _ = require('lodash')

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
  if (!this.topics[topic]) {
    this.topics[topic] = { log: new Buffer(0), index: [] }
  }

  // TODO use random accessable format
  var payload = {k: key, v: value}
  var timestamp = Date.now()
  var buf = new Buffer(JSON.stringify({offset: this._offset, ts: timestamp, payload}) + '\n')
  this.topics[topic].log = Buffer.concat([this.topics[topic].log, buf])

  // TODO use random accessable format
  var idx = {offset: this._offset, pos: this._currentPosition}
  this.topics[topic].index.push(idx)

  this._offset += 1
  this._currentPosition += buf.length

  this._writeSegment(topic)
}

// really write segment files into hyperdrive
Producer.prototype._writeSegmentNow = function (topic, cb) {
  var self = this
  writeIndex()

  function writeIndex () {
    var serializer = ndjson.serialize()
    self.topics[topic].index.forEach(i => serializer.write(i))
    serializer.end()
    var idx = self._archive.createFileWriteStream(`/${topic}/${self._currentSegmentOffset}.index`)
    pump(serializer, idx, err => {
      if (err) return cb(err)

      writeLog()
    })
  }

  function writeLog () {
    var source = new BufStream()
    source.put(self.topics[topic].log)
    source.stop()
    var idx = self._archive.createFileWriteStream(`/${topic}/${self._currentSegmentOffset}.log`)
    pump(source, idx, err => {
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
  this.topics[topic] = { log: new Buffer(0), index: [] }
}
