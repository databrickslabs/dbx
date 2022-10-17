# Migration between dbx versions

## From 0.7.0 to 0.8.0

- The interface for `--parameters` passing has been changed. Please check the special doc dedicated to parameter passing.
- `dbx deploy --write-specs-to-file` now generates a JSON payload which is `workflows` based (not `jobs`).

## From 0.6.0 and earlier to 0.7.0

- Azure Data Factory support has been **dropped**.<br/>
  Please use Azure Data Factory APIs directly on top of the deployed workflow definitions.<br/>
  To get the final workflow definition after deployment, use the `--write-specs-to-file` function:
  ```bash
  dbx deploy ... --write-specs-to-file=.dbx/deployment-result.json
  ```
- `--job`, `--jobs` arguments were deprecated. Please pass the workflow name as argument, and for `--jobs` use `--workflows`.
- `dbx sync` arguments `--allow-delete-unmatched`/`--disallow-delete-unmatched` were **replaced** with `--unmatched-behaviour` option.
- `jobs` section in the deployment file has been renamed to `workflows`. Old versions will continue working, but a warning will pop up.
- `--files-only` and `--as-run-submit` options are deprecated. Please use `--assets-only` and `--from-assets` instead.
- Project file format has been changed. Old format is supported, but a warning pops up. Please migrate to the new format as described [here](./reference/project.md).

