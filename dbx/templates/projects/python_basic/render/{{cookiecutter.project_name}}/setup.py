"""
This file configures the Python package with entrypoints used for future runs on Databricks.

Please follow the `entry_points` documentation for more details on how to configure the entrypoint:
* https://setuptools.pypa.io/en/latest/userguide/entry_point.html
"""

from setuptools import find_packages, setup
from {{cookiecutter.project_slug}} import __version__

PACKAGE_REQUIREMENTS = ["pyyaml"]

DEV_REQUIREMENTS = [
    # installation & build
    "setuptools",
    "wheel",
    # versions set in accordance with DBR 10.4 ML Runtime
    "pyspark==3.2.1",
    "delta-spark==1.1.0",
    # generic dependencies
    "pyyaml",
    "scikit-learn",
    "pandas",
    "mlflow",
    # development & testing tools
    "pytest",
    "coverage[toml]",
    "pytest-cov",
    "dbx>=0.7,<0.8"
]

setup(
    name="{{cookiecutter.project_slug}}",
    packages=find_packages(exclude=["tests", "tests.*"]),
    setup_requires=["wheel"],
    install_requires=PACKAGE_REQUIREMENTS,
    extras_require={"dev": DEV_REQUIREMENTS},
    entry_points = {
        "console_scripts": [
            "etl = {{cookiecutter.project_slug}}.tasks.sample_etl_task:entrypoint",
            "ml = {{cookiecutter.project_slug}}.tasks.sample_ml_task:entrypoint"
    ]},
    version=__version__,
    description="",
    author="",
)
