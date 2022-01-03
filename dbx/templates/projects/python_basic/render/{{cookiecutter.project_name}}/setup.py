from setuptools import find_packages, setup
from {{cookiecutter.project_slug}} import __version__

setup(
    name="{{cookiecutter.project_slug}}",
    packages=find_packages(exclude=["tests", "tests.*"]),
    setup_requires=["wheel"],
    version=__version__,
    description="",
    author=""
)
