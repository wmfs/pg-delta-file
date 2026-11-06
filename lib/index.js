const { mkdir } = require('node:fs/promises')
const getDirName = require('path').dirname
const generateDelta = require('./generate-delta')

module.exports = async function setup (options, callback) {
  await createDirectory(options.outputFilepath, options.dryrun)

  const theExport = generateDelta(options)

  if (!callback) return theExport

  theExport
    .then(info => callback(null, info))
    .catch(err => callback(err))
}

function createDirectory (outputFilepath, isDryrun) {
  if (isDryrun) return

  const outputDirPath = getDirName(outputFilepath)
  return mkdir(outputDirPath, { recursive: true })
}
