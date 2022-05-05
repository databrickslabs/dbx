import os
import pathlib
from typing import Callable, Dict, Optional, Any

from jinja2 import Environment, FileSystemLoader

from dbx.commands.configure import configure
from dbx.constants import TEMPLATE_ROOT_PATH

DBX_TEMPLATE_NAME = "python_basic"
COMPONENTS_PATH = TEMPLATE_ROOT_PATH / DBX_TEMPLATE_NAME / "components"

CICD_TOOL = "{{cookiecutter.cicd_tool}}"
CLOUD = "{{cookiecutter.cloud}}"
PROJECT_SLUG = "{{cookiecutter.project_slug}}"
PROJECT_NAME = "{{cookiecutter.project_name}}"
PROFILE = "{{cookiecutter.profile}}"
WORKSPACE_DIR = "{{cookiecutter.workspace_dir}}"
ARTIFACT_LOCATION = "{{cookiecutter.artifact_location}}"


class NamedTemplate:
    def __init__(self, env: Environment, path: str):
        self._env = env
        self._path = pathlib.Path(path)
        self._rendered: Optional[str] = None

    def render(self, parameters: Dict[str, Any]):
        template = self._env.get_template(str(self._path.as_posix()))
        self._rendered = template.render(**parameters)

    def write(self):
        if not self._rendered:
            raise Exception("Template is not rendered")
        else:
            if not self._path.parent.exists():
                self._path.parent.mkdir(parents=True, exist_ok=True)
            self._path.write_text(self._rendered)

    def render_and_write(self, parameters: Dict[str, Any]):
        self.render(parameters)
        self.write()


class PostProcessor:
    TEMPLATE_PARAMETERS: Dict[str, str] = {
        "cicd_tool": CICD_TOOL,
        "cloud": CLOUD,
        "project_slug": PROJECT_SLUG,
        "project_name": PROJECT_NAME,
    }

    @staticmethod
    def _get_filter_func(prefix: str) -> Callable[[str], bool]:
        def filter_by_prefix(name: str):
            return name.startswith(prefix)

        return filter_by_prefix

    @staticmethod
    def process_ci_component(env: Environment):
        if CICD_TOOL == "GitHub Actions":
            github_templates = env.list_templates(filter_func=PostProcessor._get_filter_func(".github"))
            for template_path in github_templates:
                template = NamedTemplate(env, template_path)
                template.render_and_write(parameters=PostProcessor.TEMPLATE_PARAMETERS)

        elif CICD_TOOL == "Azure DevOps":
            NamedTemplate(env, "azure-pipelines.yml").render_and_write(parameters=PostProcessor.TEMPLATE_PARAMETERS)

        elif CICD_TOOL == "GitLab":
            NamedTemplate(env, ".gitlab-ci.yml").render_and_write(parameters=PostProcessor.TEMPLATE_PARAMETERS)

        elif CICD_TOOL == "None":
            print("CI/CD tool argument was set to None, no CI configuration would be provided")

    @staticmethod
    def process_cloud_component(env: Environment):

        if CLOUD == "AWS":
            PostProcessor.TEMPLATE_PARAMETERS["cloud_node_type_id"] = "i3.xlarge"

        elif CLOUD == "Azure":
            PostProcessor.TEMPLATE_PARAMETERS["cloud_node_type_id"] = "Standard_E8_v3"

        elif CLOUD == "Google Cloud":
            PostProcessor.TEMPLATE_PARAMETERS["cloud_node_type_id"] = "n1-standard-4"

        NamedTemplate(env, "conf/deployment.yml").render_and_write(parameters=PostProcessor.TEMPLATE_PARAMETERS)

    @staticmethod
    def process():

        configure.callback(
            environment="default", workspace_dir=WORKSPACE_DIR, artifact_location=ARTIFACT_LOCATION, profile=PROFILE
        )

        env = Environment(loader=FileSystemLoader(COMPONENTS_PATH))

        PostProcessor.process_ci_component(env)
        PostProcessor.process_cloud_component(env)

        os.system("git init")


if __name__ == "__main__":
    post_processor = PostProcessor()
    post_processor.process()
