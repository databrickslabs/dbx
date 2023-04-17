"""
This file configures the Python package with entrypoints used for future runs on Databricks.

Please follow the `entry_points` documentation for more details on how to configure the entrypoint:
* https://setuptools.pypa.io/en/latest/userguide/entry_point.html
"""

from setuptools import find_packages, setup
from mlx_integration_test import __version__

PACKAGE_REQUIREMENTS = ["pyyaml", "pytest", "mlflow"]

# packages for local development and unit testing
# please note that these packages are already available in DBR, there is no need to install them on DBR.
LOCAL_REQUIREMENTS = [
    "pyspark==3.2.1",
    "delta-spark==1.1.0",
    "scikit-learn",
    "pandas",
    "mlflow",
]

TEST_REQUIREMENTS = [
    # development & testing tools
    "pytest",
    "coverage[toml]",
    "pytest-cov",
    "dbx>=0.8"
]

setup(
    name="mlx_integration_test",
    packages=find_packages(exclude=["tests", "tests.*"]),
    setup_requires=["setuptools","wheel"],
    install_requires=PACKAGE_REQUIREMENTS,
    extras_require={"local": LOCAL_REQUIREMENTS, "test": TEST_REQUIREMENTS},
    entry_points={
        "console_scripts": [
            "etl = mlx_integration_test.tasks.etl_task:entrypoint",
            "ml = mlx_integration_test.tasks.ml_task:entrypoint",
            "evaluate = mlx_integration_test.tasks.evaluate_task:entrypoint"
        ],
    },
    version=__version__,
    description="",
    author="",
)
