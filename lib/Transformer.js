const Transform = require('stream').Transform
const csvEncoder = require('./simple-csv-encoder')
const DateTime = require('luxon').DateTime

class Transformer extends Transform {
  constructor (info, model, options) {
    super({objectMode: true})
    this.info = info
    this.transformerFn = options.transformFunction ? options.transformFunction : identityTransform

    const transformers = []

    options.csvExtracts[model].forEach(
      function (csvColumnSource) {
        switch (csvColumnSource[0]) {
          case '$':
            const functionName = csvColumnSource.slice(1)
            switch (functionName) {
              case 'ROW_NUM':
                transformers.push((row, info) => info.totalCount)
                break
              case 'ACTION':
                transformers.push(row => {
                  // TODO: handle deleted action
                  const createdCol = options.createdColumnName || '_created'
                  const modifiedCol = options.modifiedColumnName || '_modified'
                  const created = new Date(row[createdCol])
                  const modified = new Date(row[modifiedCol])
                  const since = new Date(options.since)

                  if (modified >= since && created >= since) {
                    return options.actionAliases.insert
                  }
                  if (modified >= since && created <= since) {
                    return options.actionAliases.update
                  }
                })
                break
              case 'TIMESTAMP':
                transformers.push(() => DateTime.local().toLocaleString(DateTime.TIME_24_WITH_SECONDS))
                break
              case 'DATESTAMP':
                transformers.push(() => DateTime.local().toISODate())
                break
              case 'DATETIMESTAMP':
                transformers.push(() => DateTime.local().toISO())
                break
              default:
                transformers.push(() => `Unknown fn $${functionName}`)
            }
            break
          case '@':
            const columnName = csvColumnSource.slice(1)
            transformers.push(row => row[columnName])
            break
          default:
            transformers.push(() => csvColumnSource)
        }
      }
    )
    this.transformers = transformers
  }

  _transform (sourceRow, encoding, callback) {
    this.info.totalCount++

    const outputValues = this.transformers
      .map(fn => fn(sourceRow, this.info))

    this.transformerFn(
      outputValues,
      (err, transformedValues) => callback(err, csvEncoder(transformedValues))
    )
  }
}

function identityTransform (row, callback) {
  callback(null, row)
}

module.exports = Transformer
