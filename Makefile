# This Makefile is for project development purposes only.
ENV_NAME=dbx

install-dev:
	pip install -e .

test:
	python -m unittest

create-dev-env:
	conda create -n $(ENV_NAME) python=3.7

install-dev-reqs:
	pip install -r requirements.txt

test-init:
	dbx init \
		--project-name dbx_test \
		--cloud="AWS" \
		--pipeline-engine="GitHub Actions"