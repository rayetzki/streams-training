'use strict'

const fs = require('fs')
const { pipeline, finished } = require('stream/promises')
const path = require('path')
const { once } = require('events')
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

  async function transform (stream) {
    for await (const chunk of stream) {
      const waitDrain = !writable.write(chunkToRow(chunk))
      total++
      if (waitDrain) {
        await once(writable, 'drain')
      }
    }
  }

  await pipeline(
    readable,
    brotli,
    parseStream,
    transform,
  )

  console.timeEnd(file)
}

async function start () {
  console.log('starting')
  console.time('parsing time')
  console.log('Reading files: ', files)

  const pagesStream = fs.createWriteStream(pagesFilename, {
    highWaterMark: 1024 * 1,
  })
  pagesStream.write('id,title\n')

  const limit = pLimit(2);
  
  const fileStream = files.map(file => limit(() => parseAsync(file, pagesStream)));
  await Promise.all(fileStream);

  console.log('total: ', total)
  console.timeEnd('parsing time')

  pagesStream.end()
  await finished(pagesStream)
}

start()
