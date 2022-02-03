# Contributing

## CLA signing

To make a contribution to the Databricks Labs projects such as dbx, please fill in and sign the CLA (you can find the template in `contrib/databricks_cla.pdf`) and send it to the email mentioned in this document.
Please add ivan.trusov@databricks.com to the cc, so I can internally track the status of the CLA.

## Nota bene about PR requirements

We happily welcome contributions to `dbx`.
We use GitHub Issues to track community reported issues and GitHub Pull Requests for accepting changes.
Please create a PR only if you've created an issue related to it. PRs without a linked issue won't be reviewed.

## Local development

As a starting point, please install new environment as described in Makefile.
To make e2e runs you'll need a Databricks account, and a configured profile.

### Prerequisites
- `make`: This is the gnu make tool.
  - for mac: https://formulae.brew.sh/formula/make
- `pyenv`: https://github.com/pyenv/pyenv

Once you have the pre-requisites installed, you can run project functions like this:

```bash
make help
make clean install

make test
make test /tests/path/to/blah_test.py

make fix
make lint
```


## Pull Request Process

1. Please create a fork of this repository, and a development branch in it. You can name the branch as you would like to, but please make the branch name meaningful.
2. After finishing the development, please run the `make lint` to make sure that code is properly formatted.
3. When opening a PR, it's mandatory to reference an issue (or set of issues) it resolves. PRs without linked issues won't be reviewed.
4. Please describe the PR is 4-5 meaningful sentences. These sentences shall answer 3W questions (What is the problem, What is the impact, What is the solution)
5. Please add tests to your PR. PRs which decrease the coverage metric won't be resolved, unless it's a special case of a big refactoring.
6. If you add new functionality, please add some meaningful descriptions to the docs folder.
7. All commits shall have a GPG signature verification as per [this documentation](https://docs.github.com/en/github/authenticating-to-github/managing-commit-signature-verification/about-commit-signature-verification).

Please let us know if you've met a problem in the development setup via raising an issue. Happy coding!




