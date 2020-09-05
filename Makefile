# This Makefile is for project development purposes only.
.PHONY: build
ENV_NAME=dbx

install-dev:
	pip install -e .

test:
	pytest --cov dbx

test-execute-aws:
	pytest --cov dbx -s -k "test_execute_aws" --capture=sys

test-launch-aws:
	pytest --cov dbx -s -k "test_launch_aws" --capture=sys

test-configure:
	pytest --cov dbx -s -k "ConfigureTest" --capture=sys --cov-report html

test-execute-azure:
	pytest --cov dbx -s -k "test_execute_azure"

test-launch-azure:
	pytest --cov dbx -s -k "test_launch_azure" --capture=sys

create-dev-env:
	conda create -n $(ENV_NAME) python=3.7

install-dev-reqs:
	pip install -U -r dev-requirements.txt

docs-html:
	rm -rf docs/build/html
	sphinx-build -a -b html docs/source docs/build/html

docs-pdf:
	rm -rf docs/build/pdf
	sphinx-build -a -b pdf docs/source docs/build/pdf

build:
	python setup.py clean bdist_wheel

artifact: build docs-pdf
	rm -rf artifact
	mkdir artifact
	cp dist/*.whl artifact/
	cp docs/build/pdf/dbx.pdf artifact/