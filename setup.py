from setuptools import find_packages, setup
import sys

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
    "setuptools",
    "sphinx",
    "sphinx_rtd_theme",
    "sphinx-autobuild==2021.3.14",
    "sphinx-click",
    "sphinx-tabs",
    "cookiecutter",
    "rst2pdf==0.99",
    "wheel",
    "path",
    "pytest",
    "pytest-mock==3.8.2",
    "pytest-xdist[psutil]",
    "pytest-asyncio",
    "pytest-cov",
    "pytest-timeout",
    "pytest-clarity",
    "pandas",
    "gitpython",
    "tqdm",
    "rstcheck",
    "prospector>=1.3.1,<1.7.0",
    "black",
    "MarkupSafe>=2.0.0rc2",
]

setup(
    name="dbx",
    python_requires=">=3.8",
    packages=find_packages(exclude=["tests", "tests.*"]),
    setup_requires=["wheel"],
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
