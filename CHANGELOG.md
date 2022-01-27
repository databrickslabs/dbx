# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

**NOTE:** For CLI interfaces, we support SemVer approach. However, for API components we don't use SemVer as of now. This may lead to instability when using dbx API methods directly.

[Please read through the Keep a Changelog (~5min)](https://keepachangelog.com/en/1.0.0/).

----

## [Unreleased] - YYYY-MM-DD

### Added
- Recognition of `conf/deployment.yml` file from conf directory as a default parameter
- Remove unnecessary references of `conf/deployment.yml` in CI pipelines

### Changed

- Upgraded minimal `mlflow` version to 1.23
- Upgraded minimal `databricks-cli` version to 0.16.2

### Fixed
- Provided bugfix for emoji-based messages in certain shell environments
- Provided bugfix for cases when not all jobs are listed due to usage of Jobs API 2.1
- Provided bugfix for cases when file names are reused multiple times
- Provided bugfix for cases when `policy_name` argument needs to be applied on the tasks level

----
> Unreleased changes must be tracked above this line.
> When releasing, Copy the changelog to below this line, with proper version and date.
> And empty the **[Unreleased]** section above.
----

## [0.3.0] - 2022-01-04
### Added
- Add support for named property of the driver instance pool name
- Add support for built-in templates and project initialization via :code:`dbx init`

### Fixed
- Provided bugfix for named property resolution in multitask-based jobs



## [0.2.2] - 2021-12-03
### Changed
- Update the contribution docs with CLA
- Update documentation about environment variables

### Added
- Add support for named job properties
- Add support for `spark_jar_task` in Azure Data Factory reflector

### Fixed
- Provide bugfix for strict path resolving in the execute command
- Provide bugfix for Azure Datafactory when using `existing_cluster_id`

## [0.2.1] - 2021-11-04
### Changed
- Update `databricks-cli` dependency to 0.16.2
- Improved code coverage

### Added
- Added support for environment variables in deployment files

### Fixed
- Fixed minor bug in exception text
- Provide a bugfix for execute issue

## [0.2.0] - 2021-09-12
### Changed
- Removed pydash from package dependencies, as it is not used. Still need it as a dev-requirement.

### Added
- Added support for [multitask jobs](https://docs.databricks.com/data-engineering/jobs/index.html).
- Added more explanations around DATABRICKS_HOST exception during API client initialization
- Add strict path adjustment policy and FUSE-based path adjustment





## [0.1.6] - 2021-08-26
### Fixed
- Fix issue which stripped non-pyspark libraries from a requirements file during deploys.
- Fix issue which didn't update local package during remote execution.


## [0.1.5] - 2021-08-12
### Added
- Support for [yaml-based deployment files](https://github.com/databrickslabs/dbx/issues/39).
### Changed
- Now dbx finds the git branch name from any subdirectory in the repository.
- Minor alterations in the documentation.
- Altered the Changelog based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/)
- Changed(for contributors): Makefile now requires pyenv.
- Changed(for contributors): Makefile is more self describing and self-sufficient.
  - `make clean install` will set you up with all that is needed.
  - `make help` to see all available commands.


## [0.1.4]
### Fixed
- Fix issue with execute parameters passing
- Fix issue with multi-version package upload


## [0.1.3]
### Added
- Add explicit exception for artifact location change
- Add experimental support for fixed properties' propagation from cluster policies


## [0.1.2]
### Added
- Added Run Submit API support.


## [0.1.1]
### Fixed
- Fixed the issue with pywin32 installation for Azure imports on win platforms.


## [0.1.0]
### Added
- Integration with Azure Data Factory.
### Fixed
- Some small internal behaviour fixes.
### Changed
- Changed the behaviour of `dbx deploy --write-specs-to-file`, to make the structure of specs file compatible with environment structure.


## [0.0.14]
### Added
- Added integrated permission management, please refer to documentation for details.


## [0.0.13]
### Added
- Added `--write-specs-to-file` option for `dbx deploy` command.


## [0.0.12]
### Fixed
- HotFix for execute command.


## [0.0.11]
### Changed
- Made Internal refactorings after code coverage analysis.


## [0.0.10]
### Fixed
- Fixed issue with job spec adjustment.


## [0.0.9]
### Changed
- Finalized the CI setup for the project.
- No code changes were done.
- Release is required to start correct numeration in pypi.


## [0.0.8]
### Added
- Initial public release version.
