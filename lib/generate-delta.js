const fs = require('fs')
const stream = require('stream')
const QueryStream = require('pg-query-stream')
const Transformer = require('./Transformer')
const { snakeCase } = require('lodash')

module.exports = async function generateDelta (options) {
  const progressCallback = options.progressCallback ? options.progressCallback : () => {}
  const deltaFileWriteStream = createOutputStream(options.outputFilepath, options.dryrun)

  const info = {
    totalCount: 0
  }

  try {
    for (const model of Object.keys(options.csvExtracts)) {
      if ((model === '-HEADER-') || (model === '-FOOTER-')) {
        writeHeaderLine(info, model, options, deltaFileWriteStream)
        continue
      }

      await extractModel(info, model, options, deltaFileWriteStream)

      await processDeletes(info, model, options, deltaFileWriteStream)
    }
  } finally {
    progressCallback(info, true)
    await new Promise((resolve, reject) => deltaFileWriteStream.end(err => err ? reject(err) : resolve()))
  }

  return info
} // generateDelta

function createOutputStream (outputFilepath, isDryrun) {
  if (isDryrun) {
    return nullStream()
  }
  return fs.createWriteStream(outputFilepath, { defaultEncoding: 'utf8' })
}

function nullStream () {
  const writableStream = new stream.Writable()
  writableStream._write = (chunk, encoding, next) => next()
  return writableStream
}

function writeHeaderLine (info, model, options, outStream) {
  const csvTransformer = new Transformer(info, model, options)
  csvTransformer.pipe(outStream, { end: false })
  csvTransformer.write(options.headerData || 'DUMMY')
} // writeHeaderLine

function extractModel (info, model, options, outStream) {
  const progressCallback = options.progressCallback ? options.progressCallback : () => {}
  const modified = options.modifiedColumnName || '_modified'

  const table = tableName(options.namespace, model)

  const sql = `select * from ${table} where ${modified} >= $1`
  const csvTransform = (sql, values, client) => {
    const dbStream = client.query(new QueryStream(sql, values))
    const csvTransformer = new Transformer(info, model, options)
    dbStream.pipe(csvTransformer).pipe(outStream, { end: false })

    return new Promise((resolve, reject) => {
      dbStream.on('error', reject)
      dbStream.on('end', () => { progressCallback(info, false); resolve() })
    })
  }
  return options.client.run([{ sql, params: [options.since], action: csvTransform }])
} // extractModel

function tableName (namespace, model) {
  const separator = model.indexOf('.')

  return (separator === -1) ? `${snakeCase(namespace)}.${model}` : model
} // tableName

function processDeletes (info, model, options, outStream) {
  if (!options.deletesFunction) { return }
  const csvTransformer = new Transformer(info, model, options, true)
  csvTransformer.pipe(outStream, { end: false })

  const since = new Date(options.since)
  return options.deletesFunction(options.namespace, model, since, csvTransformer)
}
