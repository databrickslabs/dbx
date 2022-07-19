# Changelog
All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

**NOTE:** For CLI interfaces, we support SemVer approach. However, for API components we don't use SemVer as of now. This may lead to instability when using dbx API methods directly.

[Please read through the Keep a Changelog (~5min)](https://keepachangelog.com/en/1.0.0/).

## [X.Y.Z] - YYYY-MM-DD

----
> Unreleased changes must be tracked above this line.
> When releasing, Copy the changelog to below this line, with proper version and date.
> And empty the **[Unreleased]** section above.
----

## [0.6.5] - 2022-07-19

## Fixed

- Local build command now produces only one file in the `dist` folder

## Added

- Add `dist` directory cleanup before core package build

## Changed

- Separate `unit-requirements.txt` file has been deleted from the template

## [0.6.4] - 2022-07-01

## Fixed

- `RunSubmit` based launch when cloud storage is used as an artifact location


## [0.6.3] - 2022-06-28

### Added

- Module-based interface for launching commands in Azure Pipelines

### Changed

- All invocations in Azure Pipelines template are now module-based (`python -m ...`)


## [0.6.2] - 2022-06-24

- Fix auth ordering (now env-variables based auth has priority across any other auth methods)


## [0.6.1] - 2022-06-22

- Fix import issues in `dbx.api.storage` package

## [0.6.0] - 2022-06-22

### Added

- Added dev container config for VSCode and GitHub CodeSpaces
- tests are now parallel (x2 less time spent per each CI pipeline launch)
- url-strip behaviour for old-format workspace host names (which was unsupported in Mlflow API and caused a lot of hardly explainable errors)

### Changed

- Docs fixed in terms of allowed versions
- Non-strict path adjustment policy has been deleted from code and docs
- Dropped support for environment variables in plain JSON/YAML files
- Refactored code for reading configurations
- Drop support for `ruamel.yaml` in favor of standard `pyyaml`
- All tests are now based on pytest
- Full support for env variables in Jinja-based deployment configs
- Documentation improvements for Jinja-based templates
- Now package builds are performed with `pip` by default


### Fixed

- Parsing of `requirements.txt` has been improved to properly handle comments in requirements files
- Recognition of `--branch-name` argument for `dbx launch`
- Path resolution for Jinja2 templates

## [0.5.0] - 2022-06-01

### Added

- YAML Example for deploying multi-task Python job
- YAML Example for deploying multi-task Scala job
- Support including jinja templates from subpaths of the current working directory
- Add `--path` and `--checkout` options to the `dbx init`
- Change the format of the `python_basic` to use pytest
- Add `sync repo` and `sync dbfs` commands for syncing local files to Databricks and watching for changes.

### Changed

- Refactor the configuration code
- Refactor the JSON-related code

## [0.4.1] - 2022-03-01

## Fixed

- Jinja2-based file recognition behaviour

## [0.4.0] - 2022-02-28

### Added

- Documentation, examples and support for Jobs API 2.1
- Support for Jinja2-based templates inside deployment configuration
- Added new `--job` argument to deploy command for a single-job deploy and convenience

### Fixed

- Issue with empty paths in non-strict path adjustment logic
- Issues with `--no-package` argument for multi-task jobs
- Issues with named properties propagation for Jobs API 2.1


## [0.3.3] - 2022-02-08

### Fixed

- path resolution on win platforms
- Provided bugfix for non-DBFS based mlflow artifact locations

### Added

- CI pipeline on win platform

## [0.3.2] - 2022-01-31

### Fixed

- Provided bugfix for job/task name references in the deployment configuration

## [0.3.1] - 2022-01-30

### Added
- Recognition of `conf/deployment.yml` file from conf directory as a default parameter
- Remove unnecessary references of `conf/deployment.yml` in CI pipelines

### Changed

- Upgraded minimal `mlflow` version to 1.23
- Upgraded minimal `databricks-cli` version to 0.16.2
- Upgraded minimal requirements for Azure Data Factory dependent libraries

### Fixed
- Provided bugfix for emoji-based messages in certain shell environments
- Provided bugfix for cases when not all jobs are listed due to usage of Jobs API 2.1
- Provided bugfix for cases when file names are reused multiple times
- Provided bugfix for cases when `policy_name` argument needs to be applied on the tasks level
- Provided bugfix for ADF integration that deleted pipeline-level properties

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
