from pathlib import Path

import pkg_resources

from dbx.models.workflow.common.task_type import TaskType

DBX_PATH = Path(".dbx")
PROJECT_INFO_FILE_PATH = DBX_PATH / "project.json"
LOCK_FILE_PATH = DBX_PATH / "lock.json"
CUSTOM_JINJA_FUNCTIONS_PATH = DBX_PATH / "_custom_jinja_functions.py"

DATABRICKS_MLFLOW_URI = "databricks"
PROJECTS_RELATIVE_PATH = "templates/projects"

TEMPLATE_CHOICES = pkg_resources.resource_listdir("dbx", PROJECTS_RELATIVE_PATH)
TEMPLATE_ROOT_PATH = Path(pkg_resources.resource_filename("dbx", PROJECTS_RELATIVE_PATH))

# Patterns for files that are ignored by default.  There don't seem to be any reasonable scenarios where someone
# would want to sync these, so we don't make this configurable.
DBX_SYNC_DEFAULT_IGNORES = [".git/", ".dbx", "*.isorted"]
TERMINAL_RUN_LIFECYCLE_STATES = ["TERMINATED", "SKIPPED", "INTERNAL_ERROR"]
TASKS_SUPPORTED_IN_EXECUTE = [TaskType.spark_python_task, TaskType.python_wheel_task]
