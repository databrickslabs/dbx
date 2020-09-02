# This Makefile is for project development purposes only.
.PHONY: docs
ENV_NAME=dbx

install-dev:
	pip install -e .

test:
	pytest --cov dbx

test-execute-aws:
	pytest --cov dbx -s -k "test_execute_aws" --capture=sys

test-launch-aws:
	pytest --cov dbx -s -k "test_launch_aws" --capture=sys

create-dev-env:
	conda create -n $(ENV_NAME) python=3.7

install-dev-reqs:
	pip install -U -r requirements.txt

docs-html:
	rm -rf docs/build/html
	sphinx-build -a -b html docs/source docs/build/html

docs-pdf:
	rm -rf docs/build/pdf
	sphinx-build -a -b pdf docs/source docs/build/pdf