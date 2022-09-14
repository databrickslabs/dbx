# :fontawesome-solid-ship: DevOps for JVM-based projects

!!! tip

    For general DevOps considerations please take a look at the [concepts](../../concepts/devops.md).

## :material-wrench-cog: Configuring environments

Start with the `dbx configure` command to add environment information to your project.
This command will generate the project file where you can specify various environments.

Please find more guidance on the project file [here](../../reference/project.md).

We'll need some dynamic configurations in the deployment file, so let's enable the inplace Jinja support:

```bash
dbx configure --enable-inplace-jinja-support
```

## :material-car-turbocharger: Supercharging the deployment file

Among other properties that could be configured in the deployment file, we'll need to add some extras.

Here is brief example of the features that shall be used in the deployment file:

```yaml title="conf/deployment.yml" hl_lines="26"

custom:
  basic-cluster-props: &basic-cluster-props
    spark_version: "10.4.x-cpu-ml-scala2.12"

  basic-static-cluster: &basic-static-cluster
    new_cluster:
      <<: *basic-cluster-props
    num_workers: 2

build:
    commands:
        - "mvn clean package" #(1)

environments:
  default:
    workflows:
      - name: "charming-aurora-sample-jvm"
        tasks:
          - task_key: "main"
            <<: *basic-static-cluster
            libraries:
              - jar: "{{ 'file://' + dbx.get_last_modified_file('target/scala-2.12', 'jar') }}" #(2)
            deployment_config: #(3)
              no_package: true
            spark_jar_task:
                main_class_name: "org.some.main.ClassName"
```


1. You can put any set of commands in this section, e.g. `sbt compile` etc. Please refer
   to [build management doc](../../features/build_management.md).
2. Here we're the built-in custom Jinja function which searches for the last modified file in the directory.
   Read more on the Jinja capabilities [here](../../features/jinja_support.md).
3. This setting disables default Python package behaviour.

Please note the highlighted `main_class_name` line, for various tasks you probably will have various classes.


## :octicons-repo-forked-24: CI process

For CI process it's recommended to use the [assets-based workflow deployment and launch](../../features/assets.md).
This approach allows multiple branches to be deployed and launched as jobs, without the actual job registration.

Your reference CI process could look like this:

* Install JVM and packaging tools (e.g. `maven` ot `sbt`)
* Install `dbx`
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

Reference steps could like this:

* Install JVM and packaging tools (e.g. `maven` ot `sbt`)
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

