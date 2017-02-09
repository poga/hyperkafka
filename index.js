const pump = require('pump')
const BufStream = require('stream-buffers').ReadableStreamBuffer
const path = require('path')
const collect = require('collect-stream')

module.exports = {Broker}

function Broker (archive) {
  if (!(this instanceof Broker)) return new Broker(archive)

  this._archive = archive

  // buffering, lingering
  this.topics = {}
  // manage offset
  this._offset = 0
  this._currentSegmentOffset = 0
  this._currentPosition = 0
}

Broker.prototype.write = function (topic, key, value) {
  if (!this.topics[topic]) {
    this.topics[topic] = { log: new Buffer(0), index: new Buffer(0) }
  }

  // TODO better serializer
  var payload = {k: key, v: value}
  var timestamp = Date.now()
  var buf = new Buffer(JSON.stringify({offset: this._offset, ts: timestamp, payload}))
  this.topics[topic].log = Buffer.concat([this.topics[topic].log, buf])
  // FIXME: use ndjson
  var idx = new Buffer(JSON.stringify({offset: this._offset, pos: this._currentPosition}))
  this.topics[topic].index = Buffer.concat([this.topics[topic].index, idx])

  this._offset += 1
  this._currentPosition += buf.length
}

// really write segment files into hyperdrive
Broker.prototype._writeSegment = function (topic, cb) {
  console.log(this.topics)
  var self = this
  writeIndex()

  function writeIndex () {
    var source = new BufStream()
    source.put(self.topics[topic].index)
    source.stop()
    var idx = self._archive.createFileWriteStream(`/${topic}/${self._currentSegmentOffset}.index`)
    pump(source, idx, err => {
      console.log('index')
      if (err) return cb(err)

      writeLog()
    })
  }

  function writeLog () {
    console.log('log')
    var source = new BufStream()
    source.put(self.topics[topic].log)
    source.stop()
    var idx = self._archive.createFileWriteStream(`/${topic}/${self._currentSegmentOffset}.log`)
    pump(source, idx, err => {
      if (err) return cb(err)

      cb()
    })
  }
}

Broker.prototype._nextSegment = function (topic) {
  this._currentSegmentOffset = this._offset
  this._currentPosition = 0

  // reset topic buffers
  this.topics[topic] = { log: new Buffer(0), index: new Buffer(0) }
}

Broker.prototype.get = function (topic, offset, cb) {
  this._archive.list((err, entries) => {
    if (err) return cb(err)

    var segment = 0
    entries.forEach(e => {
      if (e.name.endsWith('.index')) {
        var segmentOffset = +path.basename(e.name, '.index')
        if (segmentOffset < offset && segmentOffset >= segment) {
          segment = segmentOffset
        }
      }
    })

    collect(archive.createFileReadStream(`/${topic}/${segment}.index`), (err, data) => {
      if (err) return cb(err)


    })
  })
}
