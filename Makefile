# This Makefile is for project development purposes only.
.PHONY: build docs
ENV_NAME=dbx

install-dev:
	pip install -e .

install-dev-dependencies:
	pip install -r dev-requirements.txt

test:
	pytest --cov dbx

unit-test:
	pytest tests/unit --cov dbx

test-with-html-report:
	pytest --cov dbx --cov-report html -s

create-dev-env:
	conda create -n $(ENV_NAME) python=3.7.5

docs:
	cd docs && make html

build:
	rm -rf dist/*
	rm -rf build/*
	python setup.py clean bdist_wheel