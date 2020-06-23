from setuptools import find_packages, setup

with open("LICENSE", "r") as fh:
    lic = fh.read()

setup(
    name='databrickslabs_cicdtemplates',
    packages=find_packages(),
    version='0.2.3',
    description='ML deploy CICD pipeline',
    author='Thunder Shiviah, Michael Shtelma',
    license=lic
)
