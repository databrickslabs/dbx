# Changelog

All notable changes to this project will be documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/),
and this project adheres to [Semantic Versioning](https://semver.org/spec/v2.0.0.html).

**NOTE:** For CLI interfaces, we support SemVer approach. However, for API components we don't use SemVer as of now. This may lead to instability when using dbx API methods directly.

[Please read through the Keep a Changelog (~5min)](https://keepachangelog.com/en/1.0.0/).

### Added

- Add `sync workspace` subcommand for syncing local files to Databricks and watching for changes.

### Fixed

- Recursively upload required files when a directory is passed as a task parameter with `dbx execute` and `--upload-via-context`.

## [0.8.10] - 2023-03-21


### Added
- ✨ support for files bigger than 1MB in sync

## [0.8.9] - 2022-03-06

### Fixed

- 🔨 fix `dbx deploy --no-package` when `--no-rebuild` is not specified
- 🔗 broken links in the docs
- 📝 fix deployment file structure in the docs

### Changed

- 📌 switch from using `retry` to `tenacity`

## [0.8.8] - 2022-02-22

# Fixed

- 👔 Docs: deleted lgtm badges from readme
- 👔 Docs: added footer navigation,  deleted the version layover
- 🩹 Reload config after build in case if there are any dynamic components dependent on it
- 🩹 Check if target repo exists before syncing and produce more clear error message if it does not.
- 🩹 Type recognition of `named_parameters` in `python_wheel_task`
- 🩹 Update default dbx version in the template
- 🩹 Fix the bug with repetitive launch ops
- 🔨 Add support for extras for cloud file operations
- 🔨 `warehouse_id` field in dbt task is optional now.
- 🔨 `dbx deploy --no-package` won't build wheel package.

## [0.8.7] - 2022-11-14

## Added

- 📖 Documentation on how to use custom templates
- 🦺 Add explicit Python file extension validation for `spark_python_task`

## Fixed

- 🩹 Build logic in case when `no_build` is specified


## [0.8.6] - 2022-11-09

### Changed

- ♻️ Allow `init_scripts` in DLT pipelines
- 🔇 Hide the rst version overlay from read the docs


## [0.8.5] - 2022-11-09

### Changed

- ⬆️ Bump typer to 0.7.0
- 👔 improve docs and add landing page


## [0.8.4] - 2022-11-07

### Fixed

- 🩹 Argument parsing logic in `dbx execute` without any arguments


## [0.8.3] - 2022-11-06

### Fixed

- 🩹 Wheel dependency for setup has been removed
- 🩹 Add host cleanup logic to `dbx sync` commands
- 🩹 Return auto-add functionality from `dist` folder
- 🩹 Make `pause_status` property optional
- 🚨 Make traversal process fail-safe for dictionaries

### Changed

- ⚡️ Use improved method for job search

## [0.8.2] - 2022-11-02

### Fixed

- 🩹 Deletion logic in the workflow eraser


## [0.8.1] - 2022-11-02

### Changed

- 📖 Reference documentation for deployment file
- ♻️ Add extensive caching for job listing


## [0.8.0] - 2022-11-02

### Changed

- ♻️ Introduce model matching for workflow object
- ♻️ Heavily refactor parameter passing logic
- ♻️ Heavily refactor the models used by `dbx` internal APIs
- ♻️ Make empty `workflows` list a noop instead of error
- ♻️ Handle `pytest` exit code in cookiecutter project integration test entrypoint

### Added

- 🔥 Delta Live Tables support
- 📖 Documentation on the differences between `dbx execute` and `dbx launch`
- 📖 Documentation on how to use parameter passing in various cases
- 📖 Documentation on how to enable Photon
- 📖 Documentation on artifact storage
- 🪄 Functionality to automatically enable context-based upload
- 🪄 Automatic conversion from `wasbs://` to `abfss://` references when using ADLS as artifact storage.
- ♻️ New init scripts append logic in case when `cluster-policy://` resolution is used.

### Fixed

- 🐛 Message with rich markup [] is properly displayed now
- 📖 Broken link in the generated README.md in Python template


## [0.7.6] - 2022-10-05

### Changed

- ✨ Empty list of workflows is now a noop instead of throwing an error
- 🩹 Disable the soft-wrap for printed out text

### Fixed

- 🐛 Rollback to the failsafe behaviour for assets-based property preprocessing

## [0.7.5] - 2022-09-15

### Added

- 📖 documentation on the dependency management
- ✨ failsafe switch for assets-based shared job clusters

### Fixed

- 🎨 404 page in docs is now rendered correctly
- ✏️ Small typos in the docs
- ✏️ Reference structures for `libraries` section
- 🔗 Broken links in the docs

## [0.7.4] - 2022-08-31

### Added

- 📖 documentation on the integration tests

### Changed

- ♻️ refactored poetry build logic

### Fixed

- 📖 indents in quickstart doc
- 📝 add integration tests to the quickstart structure

## [0.7.3] - 2022-08-29

### Added

- ✨ add pip install extras option
- 🎨 Nice spinners for long-running processes (e.g. cluster start and run tracing)
- 🧪 Add convenient integration tests interface example

### Changed

- 📖 Small typos in Jinja docs
- 📖 Formatting issues in cluster types doc

## [0.7.2] - 2022-08-28

### Fixed

- 🐛 bug with context provisioning for `dbx execute`

## [0.7.1] - 2022-08-28

### Added

- ⚡️`dbx destroy` command
- ☁️ failsafe behaviour for shared clusters when assets-based launch is used
- 📖 Documentation with cluster types guidance
- 📖 Documentation with scheduling and orchestration links
- 📖 Documentation for mixed-mode projects DevOps

### Changed

- ✨Add `.dbx/sync` folder to template gitignore
- ✨Changed the dependencies from the `mlflow` to a more lightweight `mlflow-skinny` option
- ✨Added suppression for too verbose `click` stacktraces
- ⚡️added `execute_shell_command` fixture, improving tests performance x2
- ⚡️added failsafe check for `get_experiment_by_name` call

## [0.7.0] - 2022-08-24

### Added

- 🎨Switch all the CLI interfaces to `typer`
- ✨Add `workflow-name` argument to `dbx deploy`, `dbx launch` and `dbx execute`
- ✨Add `--workflows` argument to `dbx deploy`
- ✨Add `--assets-only` and `--from-assets` as a clearer replacement for old arguments
- ⚡️Add support for `--environment` parameter for `dbx sync` commands
- ✨Add flexible parameter overriding logic for `dbx execute` via new `--parameters` option
- ✨Add flexible parameter overriding logic for `dbx launch` via new `--parameters` option (RunNow API)
- ✨Add flexible parameter overriding logic for `dbx launch` via new `--parameters` option (RunSubmit API)
- ✨Add inplace Jinja support for YAML and JSON files, can be configured via `dbx configure --enable-inplace-jinja-support`
- ✨Add build logic options for `pip`, `poetry` and `flit`
- ✨Add build logic customization with `build.commands` section
- ✨Add support for custom Python functions in Jinja templates

### Changed

- ✨Arguments `--allow-delete-unmatched`/`--disallow-delete-unmatched` were **replaced** with `--unmatched-behaviour` option.
- 🏷️Deprecate `jobs` section and rename it to `workflows`
- 🏷️Deprecate `job` and `jobs` options and rename it to `workflow` argument
- ✨Refactored all cluster-relevant methods into a separate `ClusterController`
- ✨Refactored model-related components for `.dbx/project.json` file
- ✨Refactored `launch`-related API-level code
- ⚡️Deleted `autouse` of `temp_project` fixture to speedup the tests
- 🚩Deprecate `--files-only` and `--as-run-submit` options
- 🚩Deprecate `--files-only` and `--as-run-submit` options
- 🚩Delete the Azure Data Factory-related functionality.
    Unfortunately we're unable to make this integration stable and secure due to resource lack and lack of RunNow API.
- 💎Documentation framework changed from `sphinx` to `mkdocs`
- 💎Documentation has been heavily re-worked and improved

### Fixed

- 🐛`dbx sync` now takes into account `HTTP(S)_PROXY` env variables
- 🐛empty task parameters are now supported
- 🐛ACLs are now properly updated for Jobs API 2.1

## [0.6.12] - 2022-08-15

### Added

- `--jinja-variables-file` for `dbx execute`

### Fixed

- Support `jobs_api_version` values provided by config in `ApiClient` construction
- References and wording in the Python template

## [0.6.11] - 2022-08-09

### Fixed

- Callback issue in `--jinja-variables-file` for `dbx deploy`

## [0.6.10] - 2022-08-04

### Added

- Added support for `python_wheel_task` in `dbx execute`

### Fixed

- Error in case when `.dbx/project.json` is non-existent
- Error in case when `environment` is not provided in the project file
- Path usage when `--upload-via-context` on win platform

## [0.6.9] - 2022-08-03

### Added

- Additional `sync` command options (`--no-use-gitignore`, `--force-include`, etc.) for more control over what is synced.
- Additional `init` command option `--template` was added to allow using dbx templates distributed as part of python packages.
- Refactored the `--deployment-file` option for better modularity of the code
- Add upload via context for `dbx execute`

## [0.6.8] - 2022-07-21

### Fixed

- Tasks naming in tests imports for Python template

## [0.6.7] - 2022-07-21

### Fixed

- Task naming and references in the Python template
- Small typo in Python template

## [0.6.6] - 2022-07-21

### Changed

- Rename `workloads` to `tasks` in the Python package template
- Documentation structure has been refactored

### Added

- Option (`--include-output`) to include run stderr and stdout output to the console output
- Docs describing how-to for Python packaging
- New option for Jinja-based deployment parameter passing from a YAML file (`--jinja-variables-file`)
- Support for multitask jobs in `dbx execute`

## [0.6.5] - 2022-07-19

### Fixed

- Local build command now produces only one file in the `dist` folder

### Added

- Add `dist` directory cleanup before core package build
- Add `--job-run-log-level` option to `dbx launch` to retrieve log after trace run

### Changed

- Separate `unit-requirements.txt` file has been deleted from the template

## [0.6.4] - 2022-07-01

### Fixed

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

### Fixed

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
