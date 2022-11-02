import click
import typer.rich_utils
from rich.traceback import install

from dbx.commands.configure import configure
from dbx.commands.deploy import deploy
from dbx.commands.destroy import destroy
from dbx.commands.execute import execute
from dbx.commands.init import init
from dbx.commands.launch import launch
from dbx.commands.sync.sync import sync_app
from dbx.commands.version import version_entrypoint
from dbx.custom import _get_custom_help_text

install(suppress=[click])

typer.rich_utils._get_help_text = _get_custom_help_text

app = typer.Typer(rich_markup_mode="markdown", pretty_exceptions_show_locals=False)

app.callback(
    name="dbx",
    help="""
    🧱 Databricks eXtensions aka dbx. Please find the main docs page [here](https://dbx.readthedocs.io/).
""",
)(version_entrypoint)

app.command(
    short_help="🔧 Configures project environment in the current folder.",
    help="""🔧 Configures project environment in the current folder.

    Project environments represent various Databricks workspaces.<br/>
    Workspaces also might be in various ☁️  clouds.

    This command might be used multiple times to change configuration or add new environment.<br/>
    If project file (located in `.dbx/project.json`) is non-existent, it will be initialized.

    There is no strict requirement to configure project file via this command.<br/>
    You can also make changes in the project file via any file editor.
    """,
)(configure)

app.command(
    short_help="📦 Deploy project to artifact storage.",
    help="""📦 Deploy project to artifact storage.

    This command deploys workflow definitions to the given environment.<br/>
    During the deployment, following actions will be performed:

    1. Python package will be built and stored in `dist` folder.<br/>
       This behaviour can be customized by using the `build` section of the deployment file.

    2. Deployment configuration will be taken from the deployment file, defined in `--deployment-file`.<br/>
        You can specify the deployment file in either JSON or YAML or Jinja-based JSON or YAML.<br/>
        `.json, .yaml, .yml, .j2` are all valid file types.<br/>
        If file is not provided, auto-discovery will try to find it in `conf` directory.

    3. From the provided deployment file, the environment specified in option `--environment` will be chosen.

    4. For this environment, the chosen set of workflows will be deployed.<br/>
       There are 3 options to choose the deployable workflows:
       ```
       dbx deploy workflows-name      # deploy one workflow
       dbx deploy --workflows=wf1,wf2 # deploy multiple workflows
       dbx deploy                     # deploy all workflows
       ```

    5. If a Python wheel file exists in `dist` folder, it will be added to the workflow dependencies.
       This behaviour can be disabled via `--no-package` option.<br/>
       It's also possible disable this behaviour by adding:
       ```yaml
       deployment_config:
         no_package: true
       ```
       to the relevant tasks in the deployment file.<br/>
       💡This option is frequently used with pure Notebook or pure JVM workflows.

    6. Any 📁 file references specified in the deployment file will be resolved.<br/>
       These files will be uploaded in a versioned fashion to the `artifact_location` specified<br/>
       for the environment in `.dbx/project.json`.<br/>
       Please note that file references shall start from `file://` or `file:fuse://`,<br/>
       otherwise they **won't be recognized**.

    7. If option `--requirements-file` is provided, all requirements will be added to workload definition.<br/>
       **Please note that this option is deprecated. We strongly recommend to use standard
       🐍 Python packaging for dependency management**.

    8. If option `--assets-only` or a deprecated `--files-only` is provided,<br/>
       then assets (📁 file references, 📦 python wheel package, etc.) will be uploaded to<br/>
       the `artifact_location`, but the **job won't be created/updated**.<br/>
       This option is frequently used in CI pipelines, when multiple users use the same environment<br/>
       to avoid collisions with the job definition.<br/>
       **Please note that the only way to launch a workflow that has been deployed with `--assets-only` option
       Is to use the `dbx launch --from-assets`**.<br/>
       To separate the workflows in the `artifact_location`, we use Git branch name.<br/>
       ℹ️ In some cases (e.g. when Git head is `DETACHED`), we won't be able to identity the branch name.<br/>
       Please use `--tags` and `--branch-name` options if you notice that workflow definitions are inconsistent.

    9. If `--write-specs-to-file` option is provided, writes the final workload definition into a given file.""",
)(deploy)

app.command(
    short_help="🔥 Executes chosen workload on the interactive cluster.",
    help="""🔥 Executes chosen workload on the interactive cluster.

    This command is very suitable to interactively execute your code on the interactive clusters.

    ---
    **Limitations**

    There are some limitations for `dbx execute`:

    * Only clusters which support `%pip execute` magic can work with execute.
    * Currently, only `spark_python_task` and `python_wheel_task` execution is supported.
    ---

    The following set of actions will be done during execution:

    1. If interactive cluster is stopped, it will be automatically started
    2. Package will be rebuilt from the source (can be disabled via `build` section of the deployment file).
    3. Workflow configuration will be taken from deployment file for given environment.
    4. All referenced files will be uploaded to the `artifact_location`.
    5. Code will be executed in a separate context. Other users can work with the same package
       on the same cluster without any limitations or overlapping.
    6. Execution results will be printed out in the shell. If result was an error, command will have error exit code.
    """,
)(execute)

app.command(short_help="💎 Generates new project from the template.", help="💎 Generates new project from the template.")(
    init
)

app.command(
    short_help="🚀 Launch the workflow on a job cluster.",
    help="""🚀 Launch the workload on a job cluster.

    This command will launch the given workload by it's name on a given environment.

    ⚠️  Please note that **workflows** shall be **deployed** prior to be launched.

    Launch command has two fundamentally different behaviours depending on the option `--from-assets`.

    If this flag is provided, launch command will search for the latest deployment that was assets-only.

    Assets-only deployment is executed as follows:

    ```
    dbx deploy --assets-only
    ```

    To avoid cases when various deployments happened from different branches,<br/>
    `dbx launch` will use Git branch name and tags (provided via `--tags` option).

    For the found deployment a new one-time run will be started
    via the [Jobs RunSubmit API](https://docs.databricks.com/dev-tools/api/latest/jobs.html#operation/JobsRunsSubmit).

    **One-time runs don't create a job and they're not visible in the Jobs UI tab**.<br/>
    Also please note that one-time runs don't support Shared Job clusters.


    💡 This option is a great choice for CI pipelines, when multiple users work in various branches.


    When `dbx launch` is running without `--from-assets` option,
    it will simply find the job by it's name and start a new job run.
    In this case `dbx launch` will use
    the [Jobs RunNow API](https://docs.databricks.com/dev-tools/api/latest/jobs.html#operation/JobsRunNow).""",
)(launch)

app.add_typer(
    sync_app,
    name="sync",
)

app.command(
    short_help="🚮 Delete defined workflows and relevant assets.",
    help="""🚮 Delete defined workflows and relevant assets.

    🚨 If neither workflow argument not `--workflows` option are provided,
    will destroy **all** workflows defined in the deployment file.""",
    name="destroy",
)(destroy)


# click app object here is used in the mkdocs.
# Don't delete it!
def get_click_app():
    return typer.main.get_command(app)


click_app = get_click_app()


def entrypoint():
    app()
