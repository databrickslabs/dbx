# :material-rocket-launch-outline: Getting started with custom templates

When using `dbx-init` you have the option to define custom templates to use to create your project structure. You can store these on git or decide to ship them as a python package, and you can also use built in templates that are created by dbx.

You can re-use the code you’ll find in the [`python_basic` dbx templates folder](https://github.com/databrickslabs/dbx/tree/main/dbx/templates/projects/python_basic) and adjust it according to your needs, i.e. generate other payload in the config file, change the code in the common, change the project structure, packaging tool, etc. - pretty much anything you would like to configure. Then you can decide if you want to re-use it as a git repo or a python package and depending on your decision, you can instruct dbx it get your artifacts in the following ways.


## :material-git: Git repo

Using `dbx init --path PATH [--checkout LOC]` will checkout a project template based on the [cookiecutter](https://cookiecutter.readthedocs.io/en/latest/index.html) approach from a git repository. Checkout is optional: it will check out a branch, tag or commit after git cloning the cookiecutter repo.

```
dbx init --path=https://git/repo/with/template.git
```

If you need versioning to your package add the `--checkout` flag:
```
#specific tag
dbx init --path=https://git/repo/with/template.git --checkout=v0.0.1 

#specific branch
dbx init --path=https://git/repo/with/template.git --checkout=prod 

#specific git commit
dbx init --path=https://git/repo/with/template.git --checkout=asa2123ss
```

## :material-package-variant-closed: Python package

Using `dbx init --package my_template_package` is a simplified version of the cookiecutter approach. When you don't have direct access to the git repo, or you want to ship your template as packaged python library this flag will be helpful to you. Some companies will store their templates in their internal PyPi repo as python package and for such use-cases dbx can pick up the template source directly from the package code. In your package you will need to have a root folder called render, and dbx will pick up the template spec from it.

   1. Create template package

        ```
        ├── render # define your cookiecutter stuff inside it
        │   └── cookiecutter.json
        └── setup.py
        ```

   2. Host this package in PyPi or Nexus, etc.
   3. Install package before dbx init
        ```
        pip install "my-template-pkg==0.0.1" # or whatever version
        ```
   4. Initialize template from package
        ```
        dbx init --package=my-template-pkg
        ```


## :material-note-edit-outline: dbx templates

Using `dbx init --template` option provides access to the templates that are pre-shipped with dbx (currently there is only one template which is python_basic).

```
dbx init --template=python_basic
```