# Contributing

We happily welcome contributions to `dbx`. 
We use GitHub Issues to track community reported issues and GitHub Pull Requests for accepting changes.
Please create a PR only if you've created an issue related to it. PRs without a linked issue won't be reviewed.

## Local development

As a starting point, please install new environment as described in Makefile.
To make e2e runs you'll need a Databricks account, and a configured profile.

## Pull Request Process

1. Please create a fork of this repository, and a development branch in it. You can name the branch as you would like to, but please make thr branch name meaningful.
2. After finishing the development, please run the `./lint.sh` to make sure that code is properly formatted.
3. When opening a PR, it's mandatory to reference an issue (or set of issues) it resolves. PRs without linked issues won't be reviewed.
4. Please describe the PR is 4-5 meaningful sentences. These sentences shall answer 3W questions (What is the problem, What is the impact, What is the solution)
5. Please add tests to your PR. PRs which decrease the coverage metric won't be resolved, unless it's a special case of a big refactoring.
6. If you add new functionality, please add some meaningful descriptions to the docs folder.


Please let us know if you've met a problem in the development setup via raising an issue. Happy coding! 




