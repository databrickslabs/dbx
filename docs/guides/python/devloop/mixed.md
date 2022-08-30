# :material-blender-outline: Mixed-mode development loop for Python projects

In some cases you would like to develop your project via two interfaces.

Such cases could be:

* One part of the team that works with the repository prefers to do this from Databricks Repos, and another prefers IDE.
* You would like to store low-level general components in standard Python module, and re-share this code with consumers from Databricks Repos

In such cases `dbx` could be of use.

## :material-file-sync: Using `dbx sync repo` for local-to-repo synchronization

To develop your code locally, and partially use it in the notebook, you can use `dbx sync repo` functionality.

Follow these steps:

* Create a repo in your git provider
* Add this repo to Databricks Repos to your user folder in the workspace (`Repos/<username>`)
* Open the project locally and start synchronization by running:

```bash
dbx sync repo -d <repo-name> # Repo name shoud be the last part of /Repos/username/repo-name
```

* At the beginning of your notebooks, setup the following block:

```python

# cell #1
%load_ext autoreload #(1)
%autoreload 2

# cell #2

from pathlib import Path #(2)
import sys

project_root = Path(".").absolute().parent
print(f"appending the main project code from {project_root}")
sys.path.append(project_root)
```

1. Sets up automatic reload of the source code
2. Adds the project code to the path

---


* Start typing code in your local IDE.
* All changes will be automatically synchronized, so you can call the code of your package directly from the notebooks.

## :material-package-variant: Using packaged artifacts from notebooks

Another potential option is that your code in Python changes not so frequently, or follows separate release process.

In this case you can store your Python package in a custom pypi (e.g. Azure Artifacts, JFrog Artifactory, etc.) and then setup custom pypi repository.

After doing this, you'll be able to install specific versions of the packaged Python code in the notebook by using pip:

```jupyter
%pip install package-from-artifactory
```

To configure custom pypi repo, there are multiple options:

1. Pass the custom repo configuration to the `/etc/pip.conf` via init script.<br/>
   You can find specfication of `pip.conf` file [here](https://pip.pypa.io/en/stable/topics/configuration/).<br/>
   Most probably you would require to specify the `extra-index-url` in the `[global]` section.
2. Setup the environment variable `PIP_EXTRA_INDEX_URL` on the cluster environment variables
3. If your package is based on [poetry](https://python-poetry.org/) it supports passing the secondary URL. Take a look at [this](https://python-poetry.org/docs/repositories/) documentation for more guidance.
