from setuptools import find_packages, setup

from dbx import __version__

with open("LICENSE", "r") as fh:
    lic = fh.read()

setup(
    name='dbx',
    packages=find_packages(exclude=['tests', 'tests.*']),
    setup_requires=['wheel'],
    install_requires=[
        "path~=15.0.0",
        "databricks-cli",
        "click",
        "retry",
        "requests",
        "mlflow",
        "pygit2"
    ],
    entry_points='''
        [console_scripts]
        dbx=dbx.cli.cli:cli
    ''',
    package_data={'dbx': ['template/deployment.json']},
    version=__version__,
    description='DataBricks eXtensions aka dbx',
    author='Thunder Shiviah, Michael Shtelma, Ivan Trusov',
    license=lic,
    classifiers=[
        'Intended Audience :: Developers',
        'Intended Audience :: System Administrators',
    ],
)
