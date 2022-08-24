# :material-package-up: Assets-based workflow deployment and launch

`dbx` deployment process provides two fundamentally different ways to deploy and define workflows.

## :material-package-up: Assets-based workflow

Sometimes during development lifecycle, you don't want to create or update a job definition out of your [:material-file-code: deployment file](../reference/deployment.md).

Some examples:

* You want to update or change job definitions **only** when you release the job
* Multiple users working in parallel on the same job (e.g. in CI pipelines)

In this case you shall use the `assets-based` approach by doing the steps described below.

To deploy the workflows **without creating or updating job definitions**, do the following:

```bash
dbx deploy <workflow-name> --assets-only
```

The `--assets-only` switch tells `dbx` to only upload the file references and prepare the workflow definitions, but **without creating a real job**.

To launch a workflow that has been deployed as asset, do the following:

```bash
dbx launch <workflow-name> --from-assets
```

With this launch a new ephemeral job will be created and launched.

!!! tip

    This job won't be visible in the Jobs UI (as expected), but you can find the run link in the command output.

## :material-call-merge: Resolving version conflicts between assets-based deployments

During the deployment and launch, `dbx` automatically picks up the git branch name from the environment.

However, in some cases you might want to have advanced tagging (e.g. multiple users, same branch).

In this case you can specify additional `--tags` to make these versions distinct, for example:

```bash
dbx deploy <workflow-name> --assets-only --tags "cake=cheesecake"
dbx deploy <workflow-name> --assets-only --tags "cake=strudel"

dbx launch <workflow-name> --from-assets --tags "cake=cheesecake" # will launch the latest cheesecake version
dbx launch <workflow-name> --from-assets --tags "cake=strudel" # will launch the latest strudel version
```

!!! tip

    If you see that `dbx` is unable to pick up the git branch name (e.g. for cases when git head is in `DETACHED` state,
    you can also provide the branch name explicitly via `--branch-name`.


## :fontawesome-regular-gem: Standard deployment

As for standard deployment process, simply omit the `--assets-only` and `--from-assets` flags from the `dbx deploy` and `dbx launch command.

E.g. to deploy the job and make it visible in the Jobs UI, run:

```bash
dbx deploy <workflow-name>
```

To launch the job by its name use:
```bash
dbx launch <workflow-name>
```

!!! tip

    The `dbx launch` command can also launch workflows that were not deployed with `dbx`.
