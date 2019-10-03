# pg-delta-file
[![Tymly Package](https://img.shields.io/badge/tymly-package-blue.svg)](https://tymly.io/)
[![npm (scoped)](https://img.shields.io/npm/v/@wmfs/pg-delta-file.svg)](https://www.npmjs.com/package/@wmfs/pg-delta-file)
[![CircleCI](https://circleci.com/gh/wmfs/pg-delta-file.svg?style=svg)](https://circleci.com/gh/wmfs/pg-delta-file)
[![codecov](https://codecov.io/gh/wmfs/pg-delta-file/branch/master/graph/badge.svg)](https://codecov.io/gh/wmfs/pg-delta-file)
[![CodeFactor](https://www.codefactor.io/repository/github/wmfs/pg-delta-file/badge)](https://www.codefactor.io/repository/github/wmfs/pg-delta-file)
[![Dependabot badge](https://img.shields.io/badge/Dependabot-active-brightgreen.svg)](https://dependabot.com/)
[![Commitizen friendly](https://img.shields.io/badge/commitizen-friendly-brightgreen.svg)](http://commitizen.github.io/cz-cli/)
[![JavaScript Style Guide](https://img.shields.io/badge/code_style-standard-brightgreen.svg)](https://standardjs.com)
[![license](https://img.shields.io/github/license/mashape/apistatus.svg)](https://github.com/wmfs/tymly/blob/master/packages/pg-concat/LICENSE)




> Outputs change-only-update CSV files (or “delta” files) that contain all the necessary actions required to re-synchronize rows in a cloned table.

## Usage

```
const generateDeltaFile = require('pg-delta-file')

const deltaInfo = await generateDeltaFiles(
  since: '2017-07-16T20:37:26.847Z',
  outputFilepath: '/some/temp/dir/people-delta.csv',
  actionAliases: {
    insert: 'u',
    update: 'u',
    delete: 'd'
  },
  createdColumnName: '_created',
  modifiedColumnName: '_modified',
  transformFunction: (row, callback) => { ... } // optional data transformation
  filterFunction: (row) => { ... } // option filter predicate
  dryrun: true // optional flag, set true to return info without generating output file 
  csvExtracts: {
    '[schema.]people': [
      'PERSON', // Just output a literal
      '$ACTION', // Will output 'u' or 'd'
      '$ROW_NUM', // Row counter
      '@social_security_id', /// Column data
      '@first_name',
      '@last_name',
      '@age'
      '$DATESTAMP',
      '$TIMESTAMP',
      '$DATETIMESTAMP',
    ],
    '[schema2.]address': [
      ...
    ]
  }
)

/*
{
  "totalCount": 5,
  "people": {
    "totalCount": 5
  },
  "address": {
    "totalCount": 3
    "filteredCount": 2
  }
} 
*/
```

## <a name="install"></a>Install
```bash
$ npm install pg-delta-file --save
```

## <a name="license"></a>License
[MIT](https://github.com/wmfs/pg-delta-file/blob/master/LICENSE)
