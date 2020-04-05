from setuptools import find_packages, setup

with open("LICENSE", "r") as fh:
    lic = fh.read()

setup(
    name='databrickslabs_mlflowdepl',
    packages=find_packages(),
    version='0.2.0',
    description='ML deploy CICD pipeline',
    author='Thunder Shiviah, Michael Shtelma',
    license=lic
)
