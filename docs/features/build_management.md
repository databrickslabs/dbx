# :material-hammer-screwdriver: Build management

All build configurations shall be specified in the [:material-file-code: deployment file](../reference/deployment.md).

!!! tip

    By default, Python `pip wheel` build is used.

Build management in `dbx` is provided in three various flavours:

=== ":material-language-python: Python"

    With this approach you can setup Python-specific build management system.<br/>
    Currently we support [`pip wheel`](https://pip.pypa.io/en/stable/cli/pip_wheel/)(default option),
    [`poetry`](https://python-poetry.org/docs/cli/#build) and [`flit`](https://flit.pypa.io/en/latest/cmdline.html#flit-build).
    To override the build option, please provide the following in the `build` section:<br/>
    ```yaml title="conf/deployment.yml"
    # irrelevant parts are omitted
    build:
       python: "poetry" # or "flit" or "pip" (default setting)
    ```

=== ":no_entry_sign: No build"

    With this approach you can simply disable all build actions.<br/>
    To do so, please provide the following in the `build` section:<br/>
    ```yaml title="conf/deployment.yml"
    # irrelevant parts are omitted
    build:
       no_build: true
    ```


=== ":octicons-command-palette-16: Commands"

    With this approach you can fully customize the build actions.<br/>
    To do so, please provide the following in the `build` section:<br/>
    ```yaml title="conf/deployment.yml"
    # irrelevant parts are omitted
    build:
       commands:
         - "echo 'building!'"
         - "sleep 5"
         - "mvn clean package"
    ```

!!! danger

    Build options are **exclusive**, e.g. it's **not possible** to combine them like this:
    ```yaml title="conf/deployment.yml"
    # this will not work
    build:
       python: "pip"
       commands:
         - "echo 'building!'"
         - "sleep 5"
         - "mvn clean package"
    ```
    If you would like to achieve this result, please specify all the required steps in `commands` section.
