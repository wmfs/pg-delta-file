const Transform = require('stream').Transform
const csvEncoder = require('./simple-csv-encoder')
const DateTime = require('luxon').DateTime
const DEFAULT_PROGRESS_FREQ = 1000

class Transformer extends Transform {
  constructor (info, model, options, processingDeletes) {
    super({ objectMode: true })
    this.info = info
    this.modelInfo = this.info[model] || { totalCount: 0 }
    this.info[model] = this.modelInfo

    const transformers = options.transformFunction ? options.transformFunction : identityTransform
    const transformFns = Array.isArray(transformers) ? transformers : [transformers]

    this.transformFn = (outputValues, cb) => {
      const transformerCascade = (err, values, index = 0) => {
        if (index !== transformFns.length && !err) {
          transformFns[index](values, (e, transformedValues) => transformerCascade(e, transformedValues, index + 1))
        } else {
          cb(err, values)
        }
      }

      transformerCascade(null, outputValues)
    }

    const filters = options.filterFunction ? options.filterFunction : () => true
    this.filterFns = Array.isArray(filters) ? filters : [filters]

    this.progressCallback = options.progressCallback
    this.progressFrequency = options.progressFrequency || DEFAULT_PROGRESS_FREQ

    const actionTransformer = !processingDeletes ? insertOrUpdateAction(options) : deleteAction(options)

    this.transformers = options.csvExtracts[model].map(csvColumnSource => {
      switch (csvColumnSource[0]) {
        case '$': {
          const functionName = csvColumnSource.slice(1)
          switch (functionName) {
            case 'ROW_NUM':
              return (row, info) => info.totalCount
            case 'ACTION':
              return actionTransformer
            case 'TIMESTAMP':
              return () => DateTime.local().toLocaleString(DateTime.TIME_24_WITH_SECONDS)
            case 'DATESTAMP':
              return () => DateTime.local().toFormat('yyyy-LL-dd')
            case 'DATETIMESTAMP':
              return () => DateTime.local().toISO()
            default:
              return () => `Unknown fn $${functionName}`
          }
        }
        case '@': {
          const columnName = csvColumnSource.slice(1)
          return row => row[columnName]
        }
        default:
          return () => csvColumnSource
      }
    })
  } // constructor

  _transform (sourceRow, encoding, callback) {
    this.bumpRowCount()

    const outputValues = this.transformers
      .map(fn => fn(sourceRow, this.info))

    if (!this.filterFns.every(f => f(outputValues))) {
      this.filterRowCount()
      return callback(null, null)
    }

    this.transformFn(
      outputValues,
      (err, transformedValues) => callback(err, csvEncoder(transformedValues))
    )

    this.progress()
  } // _transform

  progress () {
    if (!this.progressCallback) return

    // report every N rows
    const c = this.modelInfo.totalCount
    if (Math.trunc(c / this.progressFrequency) === (c / this.progressFrequency)) {
      this.progressCallback(this.info, false)
    }
  }

  bumpRowCount () {
    this.info.totalCount++
    this.modelInfo.totalCount++
  }

  filterRowCount () {
    this.info.totalCount-- // I'm not a super-fan of having to diddle this back, but there we are
    this.modelInfo.totalCount--

    if (!this.modelInfo.filteredCount) {
      this.modelInfo.filteredCount = 0
    }

    this.modelInfo.filteredCount++
  }
}

function identityTransform (row, callback) {
  callback(null, row)
}

function insertOrUpdateAction (options) {
  return row => {
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
}

function deleteAction (options) {
  return () => options.actionAliases.delete
}

module.exports = Transformer
