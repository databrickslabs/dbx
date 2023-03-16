# :fontawesome-solid-ship: DevOps for projects written in Mixed-mode

!!! tip

    For general DevOps considerations please take a look at the [concepts](../../../concepts/devops.md).


For notebook-based projects that are mixed with Python code we recommend using a combination of two features:

  * the [notebooks in a remote Git repository](https://docs.databricks.com/workflows/jobs/jobs.html#run-jobs-using-notebooks-in-a-remote-git-repository) feature for [`notebook_task`](https://docs.databricks.com/dev-tools/api/latest/jobs.html#operation/JobsCreate)
  * standard packaging approach described in the [package DevOps approach](./package.md).

Describe your workflow steps, cluster properties and other configurations in the [:material-file-code: deployment file](../../../reference/deployment.md).

## :material-git: Using `git_source` to specify the remote source for Notebook-based tasks

Here is a quick layout of a deployment file for a mixed-mode project:

```yaml title="conf/deployment.yml" hl_lines="6-9"

environments:
  default:
    workflows:
      - name: "mixed-mode-workflow"
        job_clusters:
        # omitted
        git_source:
          git_url: https://some-git-provider.com/some/remote/repo.git
          git_provider: "git-provider-name"
          git_branch: "main" # or git_tag or git_commit
        tasks:
          - task_key: "notebook-remote"
            notebook_task:
              notebook_path: "notebooks/sample_notebook"
            deployment_config:
              no_package: true
            job_cluster_key: "default"
          - task_key: "packaged"
            python_wheel_task:
              package_name: "<your-package-name>"
              entry_point: "<your-entry-point>"
            job_cluster_key: "default"
```

The [`git_source`](https://docs.databricks.com/dev-tools/api/latest/jobs.html#operation/JobsCreate) section provides pointer to the remote git branch (`tag` could also be used).

The `deployment_config` section in the `notebook_task` disables adding the project package dependency to the task dependencies.

This tells `dbx` not to add the package dependency, because package code is checked out from the repository and imported by this command at the beginning of the notebook as [described here](../devloop/mixed.md):

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

To deploy this workflow use the standard command:

```bash
dbx deploy <workflow-name>
```

!!! tip

    [Assets-based deployment](../../../features/assets.md) shall be used in the :octicons-repo-forked-24: CI process to
    avoid parallel job configuration overrides from various branches.


## :material-tooltip-plus: Using Jinja to pass tags and branches from environment variables

In some cases it would be really useful to pass the `tag` or `branch` property from the [environment variable](../../../features/jinja_support.md).

You can use Jinja template to achieve that logic:

- Make sure that your project is configured to use inplace Jinja functions:
```bash
dbx configure --enable-inplace-jinja-support
```
- Change the git-related block in the following way:

```yaml title="conf/deployment.yml" hl_lines="8 13 18"
# only relevant block shown
environments:
  default:
    workflows:
      - name: "notebook-from-remote-branch"
        git_source:
          git_url: https://some-git-provider.com/some/remote/repo.git
          git_provider: "git-provider-name"
          git_branch: "{{env['GIT_BRANCH']}}" # assuming this env variable exists
      - name: "notebook-from-remote-tag"
        git_source:
          git_url: https://some-git-provider.com/some/remote/repo.git
          git_provider: "git-provider-name"
          git_tag: "{{env['GIT_TAG']}}" # assuming this env variable exists
      - name: "notebook-from-remote-commit"
        git_source:
          git_url: https://some-git-provider.com/some/remote/repo.git
          git_provider: "git-provider-name"
          git_commit: "{{env['GIT_COMMIT']}}" # assuming this env variable exists
```
