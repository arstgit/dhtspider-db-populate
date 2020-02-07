const assert = require('assert')
const util = require('util')
const DHT = require('bittorrent-dht/client')
const torrentStream = require('torrent-stream')
const mongodb = require('mongodb')
const MongoClient = mongodb.MongoClient
const Binary = mongodb.Binary

const url = 'mongodb://mongo:27017'
const dbName = 'dhtspider'
const dhtPort = '7999'

const dht = createDHT(dhtPort)
const engineArr = []
const MAX_ENGINE_CNT = 10
const ENGINE_TIMEOUT = 40 * 1000
const TRAVERSAL_INTERVAL = 0.5 * 1000

let populatedCnt = 0
let failedPopulatedCnt = 0
let traversing = false

let cursor
let collection
// Use connect method to connect to the server
MongoClient.connect(url, { useUnifiedTopology: true }, function(err, client) {
  assert.equal(null, err)
  console.log('Connected successfully to server')

  const db = client.db(dbName)
  collection = db.collection('hash')
  cursor = collection
    .find({
      name: { $exists: false },
      lasttrytime: { $exists: false }
    })
    .batchSize(500)
    .addCursorFlag('noCursorTimeout', true)

  setInterval(traversal, TRAVERSAL_INTERVAL)
  setInterval(() => {
    process.stdout.write(
      'populated: ' +
        populatedCnt +
        ' failedPopulated: ' +
        failedPopulatedCnt +
        '\n'
    )
  }, 5 * 1000)
})

function createDHT(port, opts) {
  const dht = new DHT(opts)
  dht.listen(port)
  return dht
}

function processOneCb(err, result) {
  if (err !== null) {
    console.error(err)
    return
  }
  if (result === null) {
    console.log('no document to be populated!')
    return
  }

  let obj = {}
  let hash = result._id.buffer.toString('hex')

  let engine = torrentStream(hash)
  //let engine = torrentStream(hash, { dht: dht })
  engine.on('ready', function() {
    let metadata = engine.torrent.infoBuffer
    let name = engine.torrent.name

    obj.active = false

    engine.remove(false, err => {
      engine.destroy(err => {})
    })

    collection.updateOne(
      {
        _id: new Binary(result._id.buffer, '00')
      },
      {
        $set: {
          name: name,
          metadata: new Binary(metadata, '00'),
          mtime: Date.now()
        }
      },
      {
        upsert: false
      },
      function(err, result) {
        if (err) {
          console.log(err)
          return
        }

        assert.equal(1, result.result.n)

        populatedCnt++
      }
    )
  })

  obj.hash = hash
  obj.engine = engine
  obj.active = true
  obj.createdTime = Date.now()
  engineArr.push(obj)
}

async function initOne() {
  try {
    let result = await util.promisify(cursor.next).call(cursor)
    processOneCb(null, result)
  } catch (err) {
    console.log(err)
  }
}

async function traversal() {
  if (traversing === true) return
  traversing = true
  for (let i = 0; i < engineArr.length; i++) {
    let obj = engineArr[i]

    // populated success
    if (obj.active === false) {
      engineArr.splice(i, 1)
      continue
    }

    // populated failed, timeout
    if (Date.now() - obj.createdTime > ENGINE_TIMEOUT) {
      engineArr.splice(i, 1)

      let engine = obj.engine
      engine.remove(false, err => {
        engine.destroy(err => {})
      })

      let tmpBuffer = Buffer.from(obj.hash, 'hex')
      collection.updateOne(
        {
          _id: new Binary(tmpBuffer, '00')
        },
        {
          $set: {
            lasttrytime: Date.now()
          }
        },
        {
          upsert: false
        },
        function(err, result) {
          if (err) {
            console.log(err)
            return
          }

          assert.equal(1, result.result.n)

          failedPopulatedCnt++
        }
      )
      continue
    }
  }

  let needed = MAX_ENGINE_CNT - engineArr.length
  for (let i = 0; i < needed; i++) {
    await initOne()
  }
  traversing = false
}

console.log('dhtspider-db-populate started!')
