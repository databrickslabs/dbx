# This Makefile is for project development purposes only.
ENV_NAME=dbx

install-dev:
	pip install -e .

test:
	python -m unittest

create-dev-env:
	conda create -n $(ENV_NAME) python=3.7

install-dev-reqs:
	pip install -r dev-requirements.txt

init-test:
	cd tmp && rm -rf dbx-test && dbx init \
		--project-name=dbx-test \
		--author="dev" \
		--cloud="AWS" \
		--cicd_tool="GitHub Actions"
