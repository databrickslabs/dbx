## :fontawesome-solid-flask-vial: Running integration tests

For Python package-based projects it's pretty easy to setup the integration tests pipeline with the instruction below.

!!! hint

    For Notebook-based workflows, take a look at the [nutter project](https://github.com/microsoft/nutter).


## :octicons-file-code-24:  Preparing the configuration

* Add an entrypoint file to the `tests/entrypoint.py`

```python title="tests/entrypoint.py"
import sys

import pytest

if __name__ == '__main__':
    pytest.main(sys.argv[1:])
```

* Add your tests to the `tests/integration` folder
* Configure the special test-oriented `workflow`:

```yaml title="conf/deployment.yml"

environments:
  default:
    workflows:
      - name: "sample-tests"
        tasks:
          - task_key: "main"
            <<: *basic-static-cluster
            spark_python_task:
                python_file: "file://tests/entrypoint.py"
                # this call supports all standard pytest arguments
                parameters: ["file:fuse://tests/integration", "--cov=<insert-your-package-name>"]
```

* Provide the parameters as per [`pytest` documentation](https://docs.pytest.org/en/7.1.x/how-to/usage.html).


## :material-animation-play: Running the tests

There are two options to run the tests - all-purpose or job clusters. Read more about the differences between the cluster types [here](../../concepts/cluster_types.md).

=== "Running tests on the all-purpose clusters"

    To execute the tests on all-purpose cluster, run the following:
    ```bash
    dbx execute sample-tests \
        --task=main \
        --cluster-name=<name-of-the-all-purpose-cluster>
    ```

=== "Running tests on the job cluster"

    To deploy and launch the tests on a job cluster, use the [assets-based deployment and launch](../../features/assets.md):
    ```bash
    dbx deploy sample-tests --assets-only
    dbx launch sample-tests --from-assets
    ```

