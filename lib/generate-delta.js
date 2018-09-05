const fs = require('fs')
const QueryStream = require('pg-query-stream')
const Transformer = require('./Transformer')

module.exports = async function generateDelta (options) {
  const deltaFileWriteStream = fs.createWriteStream(options.outputFilepath, { defaultEncoding: 'utf8' })
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
    }
  } finally {
    await new Promise((resolve, reject) => deltaFileWriteStream.end(err => err ? reject(err) : resolve()))
  }

  return info
} // generateDelta

function writeHeaderLine (info, model, options, outStream) {
  const csvTransformer = new Transformer(info, model, options)
  csvTransformer.pipe(outStream, { end: false })
  csvTransformer.write('DUMMY')
} // writeHeaderLine

function extractModel (info, model, options, outStream) {
  const modified = options.modifiedColumnName || '_modified'

  const table = tableName(options.namespace, model)

  const sql = `select * from ${table} where ${modified} >= $1`
  const csvTransform = (sql, values, client) => {
    const dbStream = client.query(new QueryStream(sql, values))
    const csvTransformer = new Transformer(info, model, options)
    dbStream.pipe(csvTransformer).pipe(outStream, { end: false })
  }
  return options.client.run([{ sql: sql, params: [options.since], action: csvTransform }])
} // extractModel

function tableName (namespace, model) {
  const separator = model.indexOf('.')

  return (separator === -1) ? `${namespace}.${model}` : model
} // tableName
