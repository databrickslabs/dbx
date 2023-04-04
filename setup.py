import sys

from setuptools import find_packages, setup

from dbx import __version__

with open("README.md", "r", encoding="utf-8") as fh:
    long_description = fh.read()

INSTALL_REQUIRES = [
    # to use Databricks and MLflow APIs
    "requests>=2.24.0, <3.0.0",
    "mlflow-skinny>=1.28.0,<3.0.0",
    "databricks-cli>=0.17,<0.18",
    "tenacity>=8.2.2,<=9.0.0",
    # CLI interface
    "click>=8.1.0,<9.0.0",
    "rich==12.6.0",
    "typer[all]==0.7.0",
    # for templates creation
    "cookiecutter>=1.7.2, <3.0.0",
    # file formats and models
    "pyyaml>=6.0",
    "pydantic>=1.9.1,<=2.0.0",
    "Jinja2>=2.11.2",
    # misc - enforced to avoid issues with dependent libraries
    "cryptography>=3.3.1,<41.0.0",
    # required by dbx sync
    "aiohttp>=3.8.2",
    "pathspec>=0.9.0",
    "watchdog>=2.1.0",
]

if sys.platform.startswith("win32"):
    INSTALL_REQUIRES.append("pywin32==227")

DEV_REQUIREMENTS = [
    # utilities for documentation
    "mkdocs>=1.1.2,<2.0.0",
    "mkdocs-click>=0.8.0,<1.0",
    "mkdocs-material>=9.0.8,<10.0.0",
    "mdx-include>=1.4.1,<2.0.0",
    "mkdocs-markdownextradata-plugin>=0.1.7,<0.3.0",
    "mkdocs-glightbox>=0.2.1,<1.0",
    "mkdocs-git-revision-date-localized-plugin>=1.1.0,<=2.0",
    # pre-commit and linting utilities
    "pre-commit>=2.20.0,<4.0.0",
    "pylint==2.15.6",
    "pycodestyle==2.8.0",
    "pyflakes==2.5.0",
    "mccabe==0.6.1",
    "prospector==1.7.7",
    "black>=22.3.0,<23.0.0",
    "MarkupSafe>=2.1.1,<3.0.0",
    # testing framework
    "pytest>=7.1.3,<8.0.0",
    "pytest-mock>=3.8.2,<3.11.0",
    "pytest-xdist[psutil]>=2.5.0,<3.0.0",
    "pytest-asyncio>=0.18.3,<1.0.0",
    "pytest-cov>=4.0.0,<5.0.0",
    "pytest-timeout>=2.1.0,<3.0.0",
    "pytest-clarity>=1.0.1,<2.0.0",
    "poetry>=1.2.0",
]

AZURE_EXTRAS = ["azure-storage-blob>=12.14.1,<13.0.0", "azure-identity>=1.12.0,<2.0.0"]

AWS_EXTRAS = [
    "boto3>=1.26.13,<2",
]

GCP_EXTRAS = ["google-cloud-storage>=2.6.0,<3.0.0"]

setup(
    name="dbx",
    python_requires=">=3.8",
    packages=find_packages(exclude=["tests", "tests.*"]),
    install_requires=INSTALL_REQUIRES,
    extras_require={"dev": DEV_REQUIREMENTS, "azure": AZURE_EXTRAS, "aws": AWS_EXTRAS, "gcp": GCP_EXTRAS},
    entry_points={"console_scripts": ["dbx=dbx.cli:entrypoint"]},
    long_description=long_description,
    long_description_content_type="text/markdown",
    include_package_data=True,
    version=__version__,
    description="DataBricks CLI eXtensions aka dbx",
    author="Thunder Shiviah, Michael Shtelma, Ivan Trusov",
    license="Databricks License",
    classifiers=[
        "Intended Audience :: Developers",
        "Intended Audience :: System Administrators",
    ],
)
