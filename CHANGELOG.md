# Changelog

This document describes set of issues resolved with the given release version.

# 0.0.8

Initial public release version.

# 0.0.9

This version finalizes the CI setup for the project. No code changes were done, release is required to start correct numeration in pypi.

# 0.0.10

This version fixes issue with job spec adjustment.

# 0.0.11

This version introduces small internal refactorings made after code coverage analysis.

# 0.0.12

This version introduces hotfix for execute command.

# 0.0.13

Introduced `--write-specs-to-file` option for `dbx deploy` command.

# 0.0.14

Introduces integrated permission management, please refer to documentation for details. 

# 0.1.0

Introduces integration with Azure Data Factory, as well as some small internal behaviour fixes. 
Also, the behaviour of `dbx deploy --write-specs-to-file` has been changed to make the structure of specs file compatible with environment structure.

# 0.1.1

Fixes the issue with pywin32 installation for Azure imports on win platforms.

# 0.1.2

Adds Run Submit API support.

# 0.1.3
- Add explicit exception for artifact location change
- Add experimental support for fixed properties' propagation from cluster policies