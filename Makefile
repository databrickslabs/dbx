# This Makefile is for project development purposes only.
.PHONY: build
ENV_NAME=dbx

install-dev:
	pip install -e .

install-dev-dependencies:
	pip install -r dev-requirements.txt

test:
	pytest --cov dbx --timeout=3600

test-with-html-report:
	pytest --cov dbx --cov-report html -s

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
	rm -rf dist
	python setup.py clean bdist_wheel

artifact: build docs-pdf
	rm -rf artifact
	mkdir artifact
	cp dist/*.whl artifact/
	cp docs/build/pdf/dbx.pdf artifact/