# :material-package-variant: Development loop for JVM-based projects

Due to various technical reasons, `dbx execute` doesn't support JVM-based languages.

However, an alternative approach to JVM-based development from an IDE can be provided.

## :octicons-device-desktop-24: Local development

For local development you can continue using your setup based on local Apache Spark.

Don't forget to add the Apache Spark dependencies to your project, and keep them in sync with the used DBR version.
Add the dependencies as `provided` to avoid the `jar` from growing in size.

Use this setup for local unit tests.

As for integration tests, please follow the instructions below.

## :material-wrench-cog: Configuring environments

Start with the `dbx configure` command to add environment information to your project.
This command will generate the project file where you can specify various environments.

Please find more guidance on the project file [here](../../reference/project.md).

## :material-file-code: Adding the deployment file

After configuring the environment, you'll need to prepare the deployment file to define your workflows.

You can find more guidance about the deployment file [here](../../reference/deployment.md).

## :fontawesome-solid-microchip: Setting up an instance pool

Create an :fontawesome-solid-microchip: [instance pool](https://docs.databricks.com/clusters/instance-pools/index.html)
on the development workspace.

!!! tip

    This pool can have relatively small machines, and be based on `SPOT` instances.

## Enable inplace Jinja

We'll need some dynamic configurations in the deployment file, so let's enable the inplace Jinja support:

```bash
dbx configure --enable-inplace-jinja-support
```

## :material-car-turbocharger: Supercharging the deployment file

Among other properties that could be configured in the deployment file, we'll need to add some extras.

Here is brief example of the features that shall be used in the deployment file:

```yaml title="conf/deployment.yml" hl_lines="28"

custom:
  basic-cluster-props: &basic-cluster-props
    spark_version: "10.4.x-cpu-ml-scala2.12"

  basic-static-cluster: &basic-static-cluster
    new_cluster:
      <<: *basic-cluster-props
    num_workers: 2
    instance_pool_name: "dev-instance-pool-created-above" #(1)
    driver_instance_pool_name: "dev-instance-pool-created-above" #(2)

build:
    commands:
        - "mvn clean package" #(3)

environments:
  default:
    workflows:
      - name: "charming-aurora-sample-jvm"
        tasks:
          - task_key: "main"
            <<: *basic-static-cluster
            libraries:
              - jar: "{{ 'file://' + dbx.get_last_modified_file('target/scala-2.12', 'jar') }}" #(4)
            deployment_config: #(5)
              no_package: true
            spark_jar_task:
                main_class_name: "org.some.main.ClassName"
                parameters: []

```

1. We are providing the instance pool here to make the subsequent development runs faster.
2. We are providing the instance pool here to make the subsequent development runs faster.
3. You can put any set of commands in this section, e.g. `sbt compile` etc. Please refer
   to [build management doc](../../features/build_management.md).
4. Here we're the built-in custom Jinja function which searches for the last modified file in the directory.
   Read more on the Jinja capabilities [here](../../features/jinja_support.md).
5. This setting disables default Python package behaviour.

With this file, we can now easily launch the defined workload on the instance pools.

Please note the highlighted `main_class_name` line, for various tasks you probably will have various classes.

## :material-cog-play: Running the code

To run the code of the JVM project on Databricks in development mode, do the following:

```bash
dbx deploy <workflow-name> --assets-only
dbx launch <workflow-name> --from-assets --trace
```

Combination of both will deliver local files to Databricks and launch
an [asset-based workflow](../../features/assets.md)

With this approach you can get up to :material-av-timer: 1-3 minutes iteration for the new code.
