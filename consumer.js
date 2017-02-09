const path = require('path')
const ndjson = require('ndjson')

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

    var segment = 0
    entries.forEach(e => {
      // FIXME because we don't have random-accessable format yet. we don't use index here
      if (e.name.endsWith('.log')) {
        var segmentOffset = +path.basename(e.name, '.log')
        if (segmentOffset <= offset && segmentOffset >= segment) {
          segment = segmentOffset
        }
      }
    })

    this._archive.createFileReadStream(`/${topic}/${segment}.log`)
      .pipe(ndjson.parse())
      .on('data', d => {
        if (d.offset === offset) {
          cb(null, d)
        }
      })
  })
}
