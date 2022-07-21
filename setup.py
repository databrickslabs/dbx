import sys

from setuptools import find_packages, setup

from dbx import __version__

with open("README.rst", "r", encoding="utf-8") as fh:
    long_description = fh.read()

INSTALL_REQUIRES = [
    "databricks-cli>=0.16.4",
    "click>=7.1.2",
    "retry>=0.9.2",
    "requests>=2.24.0",
    "mlflow>=1.26.1",
    "tqdm>=4.50.0",
    "azure-identity>=1.7.1",
    "azure-mgmt-datafactory>=2.2.0",
    "azure-mgmt-subscription>=3.0.0",
    "pyyaml>=6.0",
    "pydantic>=1.9.1",
    "cryptography>=3.3.1,<38.0.0",
    "emoji>=1.6.1",
    "cookiecutter>=1.7.2",
    "Jinja2>=2.11.2",
    "aiohttp>=3.8.1",
    "pathspec>=0.9.0",
    "watchdog>=2.1.0",
]

if sys.platform.startswith("win32"):
    INSTALL_REQUIRES.append("pywin32==227")

DEV_REQUIREMENTS = [
    # utilities for documentation
    "sphinx>=4.5.0,<5.0.0",
    "sphinx_rtd_theme>=1.0.0,<2.0.0",
    "sphinx-autobuild>=2021.3.14,<2022.0.0",
    "sphinx-click>=4.1.0,<5.0.0",
    "sphinx-tabs>=3.3.1,<4.0.0",
    "rst2pdf>=0.99,<1.0",
    # pre-commit and linting utilities
    "pre-commit>=2.20.0,<3.0.0",
    "rstcheck>=5.0.0,<6.0.0",
    "prospector>=1.3.1,<1.7.0",
    "black>=22.3.0,<23.0.0",
    "MarkupSafe>=2.1.1,<3.0.0",
    # testing framework
    "pytest>=7.1.2,<8.0.0",
    "pytest-mock>=3.8.2,<3.9.0",
    "pytest-xdist[psutil]>=2.5.0,<3.0.0",
    "pytest-asyncio>=0.18.3,<1.0.0",
    "pytest-cov>=3.0.0,<4.0.0",
    "pytest-timeout>=2.1.0,<3.0.0",
    "pytest-clarity>=1.0.1,<2.0.0",
]

setup(
    name="dbx",
    python_requires=">=3.8",
    packages=find_packages(exclude=["tests", "tests.*"]),
    setup_requires=["wheel>=0.37.1,<0.38"],
    install_requires=INSTALL_REQUIRES,
    extras_require={"dev": DEV_REQUIREMENTS},
    entry_points="""
        [console_scripts]
        dbx=dbx.cli:cli
    """,
    long_description=long_description,
    long_description_content_type="text/x-rst",
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
