# This Makefile is for project development purposes only.
.PHONY: docs
ENV_NAME=dbx

install-dev:
	pip install -e .

test:
	pytest --cov dbx

test-only-aws:
	pytest --cov dbx -s -k "test_aws" --capture=sys

test-only-azure:
	pytest --cov dbx -s -k "test_azure" --capture=sys

create-dev-env:
	conda create -n $(ENV_NAME) python=3.7

install-dev-reqs:
	pip install -U -r requirements.txt

docs:
	sphinx-build -b html docs/source docs/build