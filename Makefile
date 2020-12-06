# This Makefile is for project development purposes only.
.PHONY: build
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
	rm -rf dist/*
	rm -rf build/*
	python setup.py clean bdist_wheel

artifact: build docs-pdf
	rm -rf artifact
	mkdir artifact
	cp dist/*.whl artifact/
	cp docs/build/pdf/dbx.pdf artifact/


test-local-azure:
	rm -rf dbx_dev_azure
	cookiecutter https://github.com/databrickslabs/cicd-templates.git --no-input project_name=dbx_dev_azure environment=demo-az cloud=Azure

deploy-local-azure:
	cd dbx_dev_azure && python setup.py clean bdist_wheel
	cd dbx_dev_azure && dbx deploy  \
		--environment="test" \
		--tags="cake=cheesecake" \
		--tags="branch=test"

launch-local-azure:
	cd dbx_dev_azure && dbx launch  \
		--environment="test" \
		--job="dbx_dev_azure-pipeline1"

release: build
	cp dist/* ~/IdeaProjects/cicd-templates/{{cookiecutter.project_slug}}/tools/*