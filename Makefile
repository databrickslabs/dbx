# This Makefile is for project development purposes only.
.PHONY: build docs
ENV_NAME=dbx

install-dev:
	pip install -e .

install-dev-dependencies:
	pip install -r dev-requirements.txt

test:
	pytest --cov dbx

lint:
	./lint.sh

fmt:
	black .

unit-test:
	pytest tests/unit --cov dbx

test-with-html-report:
	pytest --cov dbx --cov-report html -s

create-dev-env-conda:
	conda create -n $(ENV_NAME) python=3.7.5


# Will need to source this after `make create-dev-env-pyenv`
# source ~/.pyenv/versions/dbx-local-dev-env.3.7.5/bin/activate
create-dev-env-pyenv:
	pyenv virtualenv 3.7.5 dbx-local-dev-env.3.7.5


docs:
	cd docs && make html

build:
	rm -rf dist/*
	rm -rf build/*
	python setup.py clean bdist_wheel
