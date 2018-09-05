const Transform = require('stream').Transform
const csvEncoder = require('./simple-csv-encoder')
const DateTime = require('luxon').DateTime

class Transformer extends Transform {
  constructor (info, model, options) {
    super({ objectMode: true })
    this.info = info
    this.transformerFn = options.transformFunction ? options.transformFunction : identityTransform
    this.filterFn = options.filterFunction ? options.filterFunction : () => true

    this.transformers = options.csvExtracts[model].map(csvColumnSource => {
      switch (csvColumnSource[0]) {
        case '$':
          const functionName = csvColumnSource.slice(1)
          switch (functionName) {
            case 'ROW_NUM':
              return (row, info) => info.totalCount
            case 'ACTION':
              return row => {
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
              }
            case 'TIMESTAMP':
              return () => DateTime.local().toLocaleString(DateTime.TIME_24_WITH_SECONDS)
            case 'DATESTAMP':
              return () => DateTime.local().toISODate()
            case 'DATETIMESTAMP':
              return () => DateTime.local().toISO()
            default:
              return () => `Unknown fn $${functionName}`
          }
        case '@':
          const columnName = csvColumnSource.slice(1)
          return row => row[columnName]
        default:
          return () => csvColumnSource
      }
    })
  } // constructor

  _transform (sourceRow, encoding, callback) {
    this.info.totalCount++

    const outputValues = this.transformers
      .map(fn => fn(sourceRow, this.info))

    if (!this.filterFn(outputValues)) {
      this.info.totalCount-- // I'm not a super-fan of having to diddle this back, but there we are
      return callback(null, null)
    }

    this.transformerFn(
      outputValues,
      (err, transformedValues) => callback(err, csvEncoder(transformedValues))
    )
  } // _transform
}

function identityTransform (row, callback) {
  callback(null, row)
}

module.exports = Transformer
