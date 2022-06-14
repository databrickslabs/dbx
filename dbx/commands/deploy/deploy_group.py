import click
from dbx.commands.deploy.deploy import deploy_job, deploy_snapshot


@click.group(
    help="""Deploy project to artifact storage.

    This command takes the project in current folder (file :code:`.dbx/project.json` shall exist)
    and performs deployment to the given environment.

    During the deployment, following actions will be performed:

    #. | Python package will be built and stored in :code:`dist/*` folder.
       | This behaviour can be disabled by using :code:`--no-rebuild` option.
    #. | Deployment configuration will be taken for the provided environment (see :option:`-e` for details)
       | from the deployment file, defined in  :option:`--deployment-file`.
       | You can specify the deployment file in either JSON or YAML or Jinja-based JSON or YAML.
       | :code:`[.json, .yaml, .yml, .j2]` are all valid file types.
    #. Deployment configuration will be rendered and resolved, specifically:

        #. | Local files referenced with :code:`file://` will be uploaded to the artifact storage.
           | Storage definition will taken from :code:`.dbx/project.json`.
        #. Named properties will be resolved accordingly to the documentation

    #. Wheel file location will be added to the :code:`libraries`. Can be disabled with :option:`--no-package`.
    #. If the job with given name exists, it will be updated, if not - created
    #. | If :option:`--save-final-definitions` is provided, the final deployment spec will be written into a file.
       | For example, this option can look like this: :code:`--save-final-definitions=.dbx/deployment-result.json`.
    """
)
def deploy():
    pass


deploy.add_command(deploy_job, "job")
deploy.add_command(deploy_snapshot, "snapshot")
