from setuptools import find_packages, setup
import sys

from dbx import __version__

with open("README.rst", "r", encoding="utf-8") as fh:
    long_description = fh.read()

INSTALL_REQUIRES = [
    "path>=15.0.0",
    "databricks-cli>=0.16.2",
    "click>=7.1.2",
    "retry>=0.9.2",
    "requests>=2.24.0",
    "mlflow>=1.11.0",
    "tqdm>=4.50.0",
    "azure-identity>=1.5.0",
    "azure-mgmt-datafactory>=1.0.0",
    "azure-mgmt-subscription>=1.0.0",
    "ruamel.yaml>=0.17.10",
    "cryptography>=3.3.1,<36.0.0",
]

if sys.platform.startswith("win32"):
    INSTALL_REQUIRES.append("pywin32")

setup(
    name="dbx",
    python_requires=">=3.7",
    packages=find_packages(exclude=["tests", "tests.*"]),
    setup_requires=["wheel"],
    install_requires=INSTALL_REQUIRES,
    entry_points="""
        [console_scripts]
        dbx=dbx.cli:cli
    """,
    long_description=long_description,
    long_description_content_type="text/x-rst",
    version=__version__,
    description="DataBricks CLI eXtensions aka dbx",
    author="Thunder Shiviah, Michael Shtelma, Ivan Trusov",
    license="Databricks License",
    classifiers=[
        "Intended Audience :: Developers",
        "Intended Audience :: System Administrators",
    ],
)
