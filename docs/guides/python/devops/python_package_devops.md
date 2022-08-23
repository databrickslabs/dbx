# :fontawesome-solid-ship: DevOps for Python package-based projects

!!! tip

    For general DevOps considerations please take a look at the [concepts](../../../concepts/devops.md).


## :octicons-repo-forked-24: CI process

For CI process it's recommended to use the [assets-based workflow deployment and launch](../../../features/assets.md).
This approach allows multiple branches to be deployed and launched as jobs, without the actual job registration.

Your reference CI process could look like this:

* Install Python and pip
* Install package and it's dependencies
* Run unit tests
* Deploy integration tests in assets-based mode:
```bash
dbx deploy <workflow-name> --assets-only
```
* Run integration tests in assets-based mode:
```bash
dbx launch <workflow-name> --from-assets --trace
```
* Collect the coverage


## :fontawesome-solid-ship: CD process

For CD process simply use normal deployment mode.

Reference steps could like like this:

* Install `dbx`
* Deploy the workflows (note there is no `--assets-only`):

```bash
dbx deploy <workflow-name>
```

!!! tip

    Usually there is no need to launch the workflows from the CD process if they're in batch mode.
    However if this is required, you can always use:
    ```
    dbx launch <workflow-name> --trace
    ```
    To trace the job run to it's final state.

