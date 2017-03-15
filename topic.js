function listTopic (archive, cb) {
  console.log('list')
  archive.list((err, entries) => {
    if (err) return cb(err)
    var topics = {}
    entries.forEach(e => {
      if (e.name.endsWith('.index')) topics[e.name.match(/^\/(.+)\//)[1]] = true
    })

    cb(null, Object.keys(topics).sort())
  })
}

module.exports = {listTopic}
