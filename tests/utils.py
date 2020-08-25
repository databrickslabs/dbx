from cookiecutter.main import cookiecutter
from retry import retry

CICD_TEMPLATES_URI = "https://github.com/databrickslabs/cicd-templates.git"


@retry(tries=10, delay=5, backoff=5)
def initialize_cookiecutter(project_name):
    cookiecutter(CICD_TEMPLATES_URI, no_input=True, extra_context={"project_name": project_name})
