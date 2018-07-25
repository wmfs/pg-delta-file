/* eslint-env mocha */

'use strict'

const HlPgClient = require('@wmfs/hl-pg-client')
const chai = require('chai')
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

  describe('pg-delta-file', () => {
    it('generate the delta file', async () => {
      const outputFile = path.resolve(__dirname, 'output', 'single-delta.csv')
      const expectedFile = path.resolve(__dirname, 'fixtures', 'expected', 'single-delta.csv')

      const info = await generateDelta(
        {
          namespace: 'springfield',
          client: client,
          since: '2016-06-03 15:02:38.000000 GMT',
          outputFilepath: outputFile,
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
      )

      expect(info.totalCount).to.eql(5)

      const output = readRecords(outputFile)
      const expected = readRecords(expectedFile)

      expect(output).to.eql(expected)
    })

    it('should generate delta file for both tables', async () => {
      const outputFile = path.resolve(__dirname, 'output', 'multiple-delta.csv')
      const expectedFile = path.resolve(__dirname, 'fixtures', 'expected', 'multiple-delta.csv')

      const info = await generateDelta(
        {
          namespace: 'springfield', // to be inferred
          client: client,
          since: '2017-06-02 15:02:38.000000 GMT',
          outputFilepath: path.resolve(__dirname, './output', './multiple-delta.csv'),
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
      )
      expect(info.totalCount).to.eql(6)

      const output = readRecords(outputFile)
      const expected = readRecords(expectedFile)

      expect(output).to.eql(expected)
    })

    it('generate the delta file with transformer', async () => {
      const outputFile = path.resolve(__dirname, 'output', 'upper-cased.csv')
      const expectedFile = path.resolve(__dirname, 'fixtures', 'expected', 'upper-cased.csv')

      const info = await generateDelta(
        {
          namespace: 'springfield',
          client: client,
          since: '2016-06-03 15:02:38.000000 GMT',
          outputFilepath: outputFile,
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
      )

      expect(info.totalCount).to.eql(5)

      const output = readRecords(outputFile)
      const expected = readRecords(expectedFile)

      expect(output).to.eql(expected)
    })
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
  const file = fs.readFileSync(fileName, {encoding: 'utf8'})
  const rows = file.split('\n')
  return rows
}
