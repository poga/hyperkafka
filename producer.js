const events = require('events')
const inherits = require('inherits')
const _ = require('lodash')
const path = require('path')

const hypercore = require('hypercore')

module.exports = Producer

const DEFAULT = {
  linger: 10 // milliseconds
}

function Producer (storage, opts) {
  if (!(this instanceof Producer)) return new Producer(storage, opts)
  events.EventEmitter.call(this)
  var feed = hypercore(path.join(storage, 'root'))

  this._opts = Object.assign({}, DEFAULT, opts)
  this._feed = feed
  this._storage = storage
  this._topic = {}
}

inherits(Producer, events.EventEmitter)

Producer.prototype.produce = function (message) {
  var topic = message.topic
  if (!this._topics[topic]) this._initTopic(topic)

  // determine timestamp type
  if (message.timestamp) {
    message.timestamptype = 'CreateTime'
  } else {
    message.timestamp = Date.now()
    message.timestamptype = 'LogAppendTime'
  }

  // remove message.topic from payload
  var payload = {
    o: this._topic[topic].offset,
    k: message.key,
    v: message.value,
    ts: message.timestamp,
    tstype: message.timestamptype
  }

  this._topics[topic].buf = this._topics[topic].buf.append(payload)
  this._topics[topic].offset += 1

  this._topics[topic].write()
}

Producer.prototype._writer = function (topic) {
  var self = this
  return () => {
    self._topics[topic].feed.append(JSON.stringify(self._topics[topic].buf), function (err) {
      if (err) return self.emit('error', err)

      var flushed = self._topics[topic].buf.length
      self._topics[topic].buf = []
      self.emit('flush', topic, flushed)
    })
  }
}

Producer.prototype._initTopic = function (topic) {
  this._topics[topic] = {
    buf: [],
    write: _.debounce(this._writer(topic), this._opts.linger),
    offset: 0,
    feed: hypercore(path.join(this._storage, topic))
  }
}

Producer.prototype.topics = function (cb) {
}
