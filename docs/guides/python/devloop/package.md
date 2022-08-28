# :material-package-variant: Development loop for Python package-based projects

If you already have an existing Python package, you can easily start using `dbx` with it.

## :material-wrench-cog: Configuring environments

Start with the `dbx configure` command to add environment information to your project.
This command will generate the project file where you can specify various environments.

Please find more guidance on the project file [here](../../../reference/project.md).

## :material-file-code: Adding the deployment file

After configuring the environment, you'll need to prepare the deployment file to define your workflows.

You can find more guidance about the deployment file [here](../../../reference/deployment.md).

## :material-hammer-wrench: Configuring project build

Depending on your project packaging setup, you can use various approaches to packaging.

By default `dbx` uses `pip`-based packaging. We also support `poetry` and `flit` based packaging.
If you would like to fully customize the build process, take a look at the [build management](../../../features/build_management.md).
