# :octicons-repo-template-24: Using custom templates

When using [`dbx init`](../../reference/cli.md#dbx-init) you have the option to define custom templates to use to create your project structure.
You can store these on git or decide to ship them as a Python package, and you can also use built in templates that are provided as a part of dbx.

If you would like to create a custom template, feel free to re-use the code you’ll find in the
[`python_basic` dbx templates folder](https://github.com/databrickslabs/dbx/tree/main/dbx/templates/projects/python_basic) and adjust it according to your needs, for example:

- generate other payload in the config file
- change the code in the `common.py`
- change the project structure
- choose another packaging tool, etc.

Pretty much anything you would like to add to your template could be configured using this functionality.

There are two options on how to ship your templates for further dbx usage:

- Git repo
- Python package

This page further describes both approaches.

## :material-git: Git repo

The following command:
```bash
dbx init --path PATH [--checkout LOC]
```
will check out a project template based on the [cookiecutter](https://cookiecutter.readthedocs.io/en/latest/index.html) approach from a git repository.

The `--checkout` flag is optional. If provided, `dbx` will check out a branch, tag or commit after cloning the repo. Default behaviour is to check out the `master` or `main` branch.

```
dbx init --path=https://git/repo/with/template.git
```

If you need versioning to your package add the `--checkout` flag:
```bash
#specific tag
dbx init --path=https://git/repo/with/template.git --checkout=v0.0.1

#specific branch
dbx init --path=https://git/repo/with/template.git --checkout=prod

#specific git commit
dbx init --path=https://git/repo/with/template.git --checkout=aaa111bbb
```

## :material-package-variant-closed: Python package

When you don't have direct access to the git repo, or you want to ship your template as packaged Python library this flag will be helpful to you.
Some organizations prefer to store their templates in an internal PyPi Repo as Python package. For such use-cases dbx can pick up the template source directly from the package code.

In your package you will need to have a root folder called `render`, and dbx will pick up the template spec from it.

Find the example steps below:

   1. Create template package with the following structure (minimal example):

      ```
      ├── render # define your cookiecutter project inside it
      │   └── cookiecutter.json
      └── setup.py
      ```

   2. Host this package in PyPi or Nexus, etc.
   3. Install package *before* the `init` command:
        ```bash
        pip install "my-template-pkg==0.0.1" # or whatever version
        ```
   4. Initialize template from the package:
        ```bash
        dbx init --package=my-template-pkg
        ```

## :material-note-edit-outline: Default templates

Using `dbx init --template` option provides access to the templates that are pre-shipped with dbx (currently there is only one template which is `python_basic`).

```
dbx init --template=python_basic
```
