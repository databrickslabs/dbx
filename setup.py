from setuptools import find_packages, setup

from databrickslabs_cicdtemplates import __version__

with open("LICENSE", "r") as fh:
    lic = fh.read()

setup(
    name='databrickslabs_cicdtemplates',
    packages=find_packages(exclude=['tests', 'tests.*']),
    setup_requires=['wheel'],
    entry_points='''
        [console_scripts]
        dbx=databrickslabs_cicdtemplates.cli:cli
    ''',
    version=__version__,
    description='ML deploy CICD pipeline',
    author='Thunder Shiviah, Michael Shtelma, Ivan Trusov',
    license=lic,
    classifiers=[
        'Intended Audience :: Developers',
        'Intended Audience :: System Administrators',
    ],
)
