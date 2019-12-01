/* eslint-env mocha */

'use strict'

const HlPgClient = require('@wmfs/hl-pg-client')
const chai = require('chai')
chai.use(require('dirty-chai'))
const expect = chai.expect
const path = require('path')
const generateDelta = require('../lib')
const process = require('process')
const fs = require('fs')

describe('Run the basic usage example', function () {
  this.timeout(process.env.TIMEOUT || 5000)

  let client

  before(function () {
    if (process.env.PG_CONNECTION_STRING && !/^postgres:\/\/[^:]+:[^@]+@(?:localhost|127\.0\.0\.1).*$/.test(process.env.PG_CONNECTION_STRING)) {
      console.log(`Skipping tests due to unsafe PG_CONNECTION_STRING value (${process.env.PG_CONNECTION_STRING})`)
      this.skip()
    }
  })

  describe('setup', () => {
    it('setup database connection', (done) => {
      client = new HlPgClient(process.env.PG_CONNECTION_STRING)
      done()
    })

    it('install test schemas', () => {
      return client.runFile(path.resolve(__dirname, 'fixtures', 'install-test-schemas.sql'))
    })
  })

  const tests = [
    {
      name: 'single delta',
      file: 'single-delta.csv',
      count: 5,
      info: {
        totalCount: 5,
        people: {
          totalCount: 5
        }
      },
      delta: {
        namespace: 'springfield',
        since: '2016-06-03 15:02:38.000000 GMT',
        actionAliases: {
          insert: 'i',
          update: 'u',
          delete: 'd'
        },
        csvExtracts: {
          people: [
            73,
            '$ACTION',
            '$ROW_NUM',
            '@social_security_id',
            '@first_name',
            '@last_name',
            '@age'
          ]
        }
      }
    },
    {
      name: 'delta with header',
      file: 'with-header.csv',
      count: 7,
      info: {
        totalCount: 7,
        '-HEADER-': {
          totalCount: 1
        },
        people: {
          totalCount: 5
        },
        '-FOOTER-': {
          totalCount: 1
        }
      },
      delta: {
        namespace: 'springfield',
        since: '2016-06-03 15:02:38.000000 GMT',
        actionAliases: {
          insert: 'i',
          update: 'u',
          delete: 'd'
        },
        csvExtracts: {
          '-HEADER-': [
            'a string',
            '$ROW_NUM',
            'another string',
            999
          ],
          people: [
            73,
            '$ACTION',
            '$ROW_NUM',
            '@social_security_id',
            '@first_name',
            '@last_name',
            '@age'
          ],
          '-FOOTER-': [
            'all done',
            '$ROW_NUM',
            0,
            0
          ]
        }
      }
    },
    {
      name: 'delta for both tables',
      file: 'multiple-delta.csv',
      count: 6,
      info: {
        totalCount: 6,
        homes: {
          totalCount: 3
        },
        people: {
          totalCount: 3
        }
      },
      delta: {
        namespace: 'springfield', // to be inferred
        since: '2017-06-02 15:02:38.000000 GMT',
        actionAliases: {
          insert: 'i',
          update: 'u',
          delete: 'd'
        },
        csvExtracts: {
          homes: [
            74,
            '$ACTION',
            '@address',
            '@owner_id'
          ],
          people: [
            73,
            '$ACTION',
            '@social_security_id',
            '@first_name',
            '@last_name',
            '@age'
          ]
        }
      }
    },
    {
      name: 'delta with transformer',
      file: 'upper-cased.csv',
      count: 5,
      info: {
        totalCount: 5,
        people: {
          totalCount: 5
        }
      },
      delta: {
        namespace: 'springfield',
        since: '2016-06-03 15:02:38.000000 GMT',
        actionAliases: {
          insert: 'i',
          update: 'u',
          delete: 'd'
        },
        transformFunction: function (row, callback) {
          row[4] = row[4].toUpperCase()
          row[5] = row[5].toUpperCase()
          callback(null, row)
        },
        csvExtracts: {
          people: [
            73,
            '$ACTION',
            '$ROW_NUM',
            '@social_security_id',
            '@first_name',
            '@last_name',
            '@age'
          ]
        }
      }
    },
    {
      name: 'delta with filter',
      file: 'filtered.csv',
      count: 3,
      info: {
        totalCount: 3,
        people: {
          totalCount: 3,
          filteredCount: 2
        }
      },
      delta: {
        namespace: 'springfield',
        since: '2016-06-03 15:02:38.000000 GMT',
        actionAliases: {
          insert: 'i',
          update: 'u',
          delete: 'd'
        },
        filterFunction: function (row) {
          return +row[6] < 60
        },
        csvExtracts: {
          people: [
            73,
            '$ACTION',
            '$ROW_NUM',
            '@social_security_id',
            '@first_name',
            '@last_name',
            '@age'
          ]
        }
      }
    },
    {
      name: 'delta with filter and transform',
      file: 'filtered-upper-cased.csv',
      count: 3,
      info: {
        totalCount: 3,
        people: {
          totalCount: 3,
          filteredCount: 2
        }
      },
      delta: {
        namespace: 'springfield',
        since: '2016-06-03 15:02:38.000000 GMT',
        actionAliases: {
          insert: 'i',
          update: 'u',
          delete: 'd'
        },
        filterFunction: function (row) {
          return +row[6] < 60
        },
        transformFunction: function (row, callback) {
          row[4] = row[4].toUpperCase()
          row[5] = row[5].toUpperCase()
          callback(null, row)
        },
        csvExtracts: {
          people: [
            73,
            '$ACTION',
            '$ROW_NUM',
            '@social_security_id',
            '@first_name',
            '@last_name',
            '@age'
          ]
        }
      }
    }
  ]

  describe('delta-file generation', () => {
    for (const test of tests) {
      it(test.name, async () => {
        const outputFile = path.resolve(__dirname, 'output', test.file)
        const expectedFile = path.resolve(__dirname, 'fixtures', 'expected', test.file)

        let callbackInfo = null
        const callbackProgress = []

        test.delta.client = client
        test.delta.outputFilepath = outputFile
        test.delta.dryrun = false
        test.delta.progressCallback = (info, complete) => {
          callbackInfo = info
          callbackProgress.push(complete)
        }

        const info = await generateDelta(
          test.delta
        )

        expect(info.totalCount).to.eql(test.count)
        expect(info).to.eql(test.info)
        expect(info).to.eql(callbackInfo)

        const expectedProgress = callbackProgress.map(() => false)
        expectedProgress[callbackProgress.length - 1] = true

        expect(expectedProgress).to.eql(callbackProgress)

        const output = readRecords(outputFile)
        const expected = readRecords(expectedFile)

        expect(output).to.eql(expected)
      })
    }
  })

  describe('delta dry-run', () => {
    for (const test of tests) {
      it(test.name, async () => {
        const outputFile = path.resolve(__dirname, 'output', test.file)
        fs.unlinkSync(outputFile)

        test.delta.client = client
        test.delta.outputFilepath = outputFile
        test.delta.dryrun = true

        const info = await generateDelta(
          test.delta
        )

        expect(info.totalCount).to.eql(test.count)
        expect(info).to.eql(test.info)

        expect(fs.existsSync(outputFile)).to.be.false()
      })
    }
  })

  describe('cleanup', () => {
    it('uninstall test schemas', () => {
      return client.runFile(path.resolve(__dirname, 'fixtures', 'uninstall-test-schemas.sql'))
    })

    it('close database connections', function (done) {
      client.end()
      done()
    })
  })
})

function readRecords (fileName) {
  const file = fs.readFileSync(fileName, { encoding: 'utf8' })
  const rows = file.split('\n')
  return rows
}
