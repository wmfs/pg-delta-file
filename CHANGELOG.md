# [1.11.0](https://github.com/wmfs/pg-delta-file/compare/v1.10.0...v1.11.0) (2018-10-03)


### 🛠 Builds

* **deps:** update pg requirement from 7.4.3 to 7.5.0 ([a23b7bc](https://github.com/wmfs/pg-delta-file/commit/a23b7bc))

# [1.10.0](https://github.com/wmfs/pg-delta-file/compare/v1.9.0...v1.10.0) (2018-10-01)


### 🛠 Builds

* **deps:** update luxon requirement from 1.4.1 to 1.4.2 ([9708daf](https://github.com/wmfs/pg-delta-file/commit/9708daf))

# [1.9.0](https://github.com/wmfs/pg-delta-file/compare/v1.8.0...v1.9.0) (2018-09-28)


### 🛠 Builds

* **deps:** update luxon requirement from 1.4.0 to 1.4.1 ([cb9f087](https://github.com/wmfs/pg-delta-file/commit/cb9f087))

# [1.8.0](https://github.com/wmfs/pg-delta-file/compare/v1.7.0...v1.8.0) (2018-09-28)


### 🛠 Builds

* **deps:** update luxon requirement from 1.3.3 to 1.4.0 ([9c3d023](https://github.com/wmfs/pg-delta-file/commit/9c3d023))
* **deps-dev:** update chai requirement from 4.1.2 to 4.2.0 ([3389ec0](https://github.com/wmfs/pg-delta-file/commit/3389ec0))
* **deps-dev:** update semantic-release requirement ([1ad8acc](https://github.com/wmfs/pg-delta-file/commit/1ad8acc))

# [1.7.0](https://github.com/wmfs/pg-delta-file/compare/v1.6.0...v1.7.0) (2018-09-12)


### 🛠 Builds

* **deps:** update lodash requirement from 4.17.10 to 4.17.11 ([5d11673](https://github.com/wmfs/pg-delta-file/commit/5d11673))
* **deps-dev:** update [@semantic-release](https://github.com/semantic-release)/git requirement ([866c9ae](https://github.com/wmfs/pg-delta-file/commit/866c9ae))
* **deps-dev:** update [@wmfs](https://github.com/wmfs)/hl-pg-client requirement ([34f882d](https://github.com/wmfs/pg-delta-file/commit/34f882d))
* **deps-dev:** update semantic-release requirement ([1b76f5c](https://github.com/wmfs/pg-delta-file/commit/1b76f5c))

# [1.6.0](https://github.com/wmfs/pg-delta-file/compare/v1.5.0...v1.6.0) (2018-09-11)


### 🛠 Builds

* **deps:** correct luxon version ([2e4ddb0](https://github.com/wmfs/pg-delta-file/commit/2e4ddb0))
* **deps-dev:** update [@wmfs](https://github.com/wmfs)/hl-pg-client requirement ([9bdc90a](https://github.com/wmfs/pg-delta-file/commit/9bdc90a))
* **deps-dev:** update [@wmfs](https://github.com/wmfs)/hl-pg-client requirement ([9e21af9](https://github.com/wmfs/pg-delta-file/commit/9e21af9))
* **deps-dev:** update [@wmfs](https://github.com/wmfs)/hl-pg-client requirement ([516e60d](https://github.com/wmfs/pg-delta-file/commit/516e60d))
* **deps-dev:** update semantic-release requirement ([2a960f9](https://github.com/wmfs/pg-delta-file/commit/2a960f9))
* **deps-dev:** update semantic-release requirement ([3a9ea85](https://github.com/wmfs/pg-delta-file/commit/3a9ea85))

# [1.5.0](https://github.com/wmfs/pg-delta-file/compare/v1.4.0...v1.5.0) (2018-09-05)


### ✨ Features

* pg-delta-file takes option filterFunction, which can be used to strip out unwanted rows ([9671498](https://github.com/wmfs/pg-delta-file/commit/9671498))


### 🛠 Builds

* **deps-dev:** update [@semantic-release](https://github.com/semantic-release)/git requirement from 7.0.1 to 7.0.2 ([e3bc519](https://github.com/wmfs/pg-delta-file/commit/e3bc519))
* **deps-dev:** update [@semantic-release](https://github.com/semantic-release)/git requirement from 7.0.2 to 7.0.3 ([df5d0fe](https://github.com/wmfs/pg-delta-file/commit/df5d0fe))
* **deps-dev:** update codecov requirement from 3.0.4 to 3.1.0 ([19066d5](https://github.com/wmfs/pg-delta-file/commit/19066d5))
* **deps-dev:** update nyc requirement from 12.0.2 to 13.0.1 ([83fcdf7](https://github.com/wmfs/pg-delta-file/commit/83fcdf7))
* **deps-dev:** update semantic-release requirement from 15.9.11 to 15.9.12 ([887b2b7](https://github.com/wmfs/pg-delta-file/commit/887b2b7))
* **deps-dev:** update semantic-release requirement from 15.9.8 to 15.9.9 ([d54d837](https://github.com/wmfs/pg-delta-file/commit/d54d837))
* **deps-dev:** update semantic-release requirement from 15.9.9 to 15.9.11 ([3f56985](https://github.com/wmfs/pg-delta-file/commit/3f56985))
* **dev-deps:** Move to standard 12.0.1 ([0d92228](https://github.com/wmfs/pg-delta-file/commit/0d92228))


### 🚨 Tests

* New (failing) test for filterFunction ([a3aa474](https://github.com/wmfs/pg-delta-file/commit/a3aa474))
* table driven tests ([15f9230](https://github.com/wmfs/pg-delta-file/commit/15f9230))

# [1.4.0](https://github.com/wmfs/pg-delta-file/compare/v1.3.0...v1.4.0) (2018-08-14)


### ✨ Features

* Added $DATESTAMP, $TIMESTAMP, $DATETIMESTAMP output functions ([e4d2ed7](https://github.com/wmfs/pg-delta-file/commit/e4d2ed7))


### 📦 Code Refactoring

* Rework transformer. Instead of dynamically generating function source code, use an array o ([3d26316](https://github.com/wmfs/pg-delta-file/commit/3d26316))

# [1.3.0](https://github.com/wmfs/pg-delta-file/compare/v1.2.0...v1.3.0) (2018-08-14)


### ✨ Features

* Added support for fixed header and footer rows ([0345970](https://github.com/wmfs/pg-delta-file/commit/0345970))


### 🛠 Builds

* **deps-dev:** update [@semantic-release](https://github.com/semantic-release)/changelog requirement to 3.0.0 ([bd0ca70](https://github.com/wmfs/pg-delta-file/commit/bd0ca70))
* **deps-dev:** update [@semantic-release](https://github.com/semantic-release)/git requirement to 7.0.1 ([af28ffb](https://github.com/wmfs/pg-delta-file/commit/af28ffb))
* **deps-dev:** update [@wmfs](https://github.com/wmfs)/hl-pg-client requirement to 1.1.3 ([3f44b03](https://github.com/wmfs/pg-delta-file/commit/3f44b03))
* **deps-dev:** update semantic-release requirement from 15.9.3 to 15.9.5 ([26de657](https://github.com/wmfs/pg-delta-file/commit/26de657))
* **deps-dev:** update semantic-release requirement from 15.9.5 to 15.9.6 ([ebcd1f2](https://github.com/wmfs/pg-delta-file/commit/ebcd1f2))
* **deps-dev:** update semantic-release requirement from 15.9.6 to 15.9.7 ([334f97a](https://github.com/wmfs/pg-delta-file/commit/334f97a))
* **deps-dev:** update semantic-release requirement from 15.9.7 to 15.9.8 ([51e5785](https://github.com/wmfs/pg-delta-file/commit/51e5785))
* **deps-dev:** update semantic-release requirement to 15.9.1 ([2515f54](https://github.com/wmfs/pg-delta-file/commit/2515f54))
* **deps-dev:** update semantic-release requirement to 15.9.2 ([f1ed3a5](https://github.com/wmfs/pg-delta-file/commit/f1ed3a5))
* **deps-dev:** update semantic-release requirement to 15.9.3 ([0057108](https://github.com/wmfs/pg-delta-file/commit/0057108))


### ♻️ Chores

* codecov 3.0.2 -> 3.0.4 ([a4dc745](https://github.com/wmfs/pg-delta-file/commit/a4dc745))

# [1.2.0](https://github.com/wmfs/pg-delta-file/compare/v1.1.0...v1.2.0) (2018-07-25)


### ✨ Features

* Added optional transformFunction options ([650525a](https://github.com/wmfs/pg-delta-file/commit/650525a))


### 🐛 Bug Fixes

* Correct callback function ([172cb9f](https://github.com/wmfs/pg-delta-file/commit/172cb9f))
* Wait for file to finish writing before returning from generateDelta ([4a0d564](https://github.com/wmfs/pg-delta-file/commit/4a0d564))


### 🛠 Builds

* **package:** update commitizen config path ([506d283](https://github.com/wmfs/pg-delta-file/commit/506d283))


### 📦 Code Refactoring

* **.gitignore:** remove and ignore package-lock.json ([6390da9](https://github.com/wmfs/pg-delta-file/commit/6390da9))
* Added describe sections in test ([fbbb848](https://github.com/wmfs/pg-delta-file/commit/fbbb848))


### ⚙️ Continuous Integrations

* **.travis.yml:** add travis_retry function ([99c6888](https://github.com/wmfs/pg-delta-file/commit/99c6888))
* **.travis.yml:** update pg config ([fcfe4ec](https://github.com/wmfs/pg-delta-file/commit/fcfe4ec))
* **package:** commit analyzer plugin config ([ca1be2d](https://github.com/wmfs/pg-delta-file/commit/ca1be2d))
* **semantic-release:** update devDependancy scope after change in Dependabot ([7fdfcac](https://github.com/wmfs/pg-delta-file/commit/7fdfcac))
* remove deps-dev scoping release ([453c13a](https://github.com/wmfs/pg-delta-file/commit/453c13a))


### 💎 Styles

* Standards fixes ([bf0e8c8](https://github.com/wmfs/pg-delta-file/commit/bf0e8c8))
