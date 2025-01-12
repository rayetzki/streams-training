'use strict'

const fs = require('fs')
const { pipeline, finished } = require('stream/promises')
const path = require('path')
const saxophonist = require('saxophonist')
const { createBrotliDecompress } = require('zlib')
const pLimit = require('p-limit')

const files = process.argv.splice(2)
const pagesFilename = path.resolve(__dirname, 'pages.csv')

let total = 0

const chunkToRow = (chunk) => {
  const title = chunk.children[0].text
  const id = chunk.children[2].text
  return `${id},${title}\n`
}

async function parseAsync(file, writable) {
  console.time(file)
  const readable = fs.createReadStream(file)
  const brotli = createBrotliDecompress()
  const parseStream = saxophonist('page', {
    highWaterMark: 1024 * 1024 * 1024 * 32
  })

  async function * transform (stream) {
    for await (const chunk of stream) {
      total++
      yield chunkToRow(chunk)
    }
    // TODO
    // 1. iterate over each chunk
    // 2. increment the total
    // 3. transform the chunk using chunkToRow
    // 4. yield the transformed chunk
  }

  // TODO
  // 1. create a pipeline from readable -> brotli -> parseStream -> transform -> writable
  // 2. do not end the writable stream, so that it can be reused for the next parsed file
  await pipeline(
    readable,
    brotli,
    parseStream,
    transform,
    writable,
    { end: false }
  )
  // TODO: Extra
  // 1. call `writable.write()` directly inside the transform function
  // 2. remove the yield and async generator function
  // 3. handle backpressure by using the `events.once` utility and the 'drain' event

  console.timeEnd(file)
}

async function start () {
  console.log('starting')
  console.time('parsing time')
  console.log('Reading files: ', files)

  // TODO
  // 1. Create the write stream for pagesFilename
  // 2. write the header to the file "id, title"
  // 3. Iterate over each file and call parseAsync in *series*
  // 3. End the writable stream and then wait on `finished(writeable)`
  const pagesStream = fs.createWriteStream(pagesFilename)
  pagesStream.write('id,title\n')
  
  for (const file of files) {
    await parseAsync(file, pagesStream)
  }

  // TODO: Extra - parallelise
  // 1. Map over each file and call parseAsync
  // 2. Promise.all that array of promises
  // 3. Use `p-limit` to limit the concurrency

  console.log('total: ', total)
  console.timeEnd('parsing time')

  pagesStream.end()
  await finished(pagesStream)
}

start()
