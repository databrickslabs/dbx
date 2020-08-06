from setuptools import find_packages, setup
from {{cookiecutter.project_name}} import __version__

setup(
    name='{{cookiecutter.project_name}}',
    packages=find_packages(),
    version=__version__
)