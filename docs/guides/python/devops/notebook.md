# :fontawesome-solid-ship: DevOps for Notebooks-based project

!!! tip

    For general DevOps considerations please take a look at the [concepts](../../../concepts/devops.md).


For notebook-based projects we recommend using the [notebooks in a remote Git repository](https://docs.databricks.com/workflows/jobs/jobs.html#run-jobs-using-notebooks-in-a-remote-git-repository) feature.

Describe your workflow steps, cluster properties and other configurations in the deployment file.

## :material-git: Using `git_source` to specify the remote source
Then use the `git_source` configuration properties to setup the source:

```yaml title="conf/deployment.yml" hl_lines="9-12 14-15"

build: #(1)
  no_build: True

environments:
  default:
    workflows:
      - name: "notebook-from-remote"
        job_clusters:
        # omitted
        git_source:
          git_url: https://some-git-provider.com/some/remote/repo.git
          git_provider: "git-provider-name"
          git_branch: "main"
        tasks:
          - task_key: "notebook-remote"
            notebook_task:
              notebook_path: "notebooks/sample_notebook"
            deployment_config:
              no_package: true
            job_cluster_key: "default"
```

1. Disables the standard `pip`-based build behaviour. Read more about build management [here](../../../features/build_management.md).

Point your attention to the highlighted section. The `git_source` section provides pointer to the remote git branch (`tag` could also be used).

The `deployment_config` section disables adding the project package dependency to the workflow dependencies.
This option shall be used in cases when you have only notebooks in your project and no project packaging is required.


To deploy this workflow use the standard command:

```bash
dbx deploy <workflow-name>
```

!!! tip

    [Assets-based deployment](../../../features/assets.md) can also be used in the :octicons-repo-forked-24: CI process to
    avoid parallel job configuration overrides from various branches.


## :material-tooltip-plus: Using Jinja to pass tags and branches from environment variables

In some cases it would be really useful to pass the `tag` or `branch` property from the environment variable.

You can use Jinja template to achieve that logic:

- Make sure that your project is configured to use inplace Jinja functions:
```bash
dbx configure --enable-inplace-jinja-support
```
- Change the git-related block in the following way:

```yaml title="conf/deployment.yml" hl_lines="8 12 17"
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
