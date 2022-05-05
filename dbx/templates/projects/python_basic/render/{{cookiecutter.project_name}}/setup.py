"""
This file configures the Python package with entrypoints used for future runs on Databricks.

Please follow the `entry_points` documentation for more details on how to configure the entrypoint:
* https://setuptools.pypa.io/en/latest/userguide/entry_point.html
"""

from setuptools import find_packages, setup
from {{cookiecutter.project_slug}} import __version__

setup(
    name="{{cookiecutter.project_slug}}",
    packages=find_packages(exclude=["tests", "tests.*"]),
    setup_requires=["wheel"],
    entry_points = {
        "console_scripts": [
            "etl = {{cookiecutter.project_slug}}.workloads.sample_etl_job:entrypoint",
            "ml = {{cookiecutter.project_slug}}.workloads.sample_ml_job:entrypoint"
    ]},
    version=__version__,
    description="",
    author="",
)
