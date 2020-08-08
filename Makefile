# This Makefile is for project development purposes only.
ENV_NAME=dbx

install-dev:
	pip install -e .

test:
	pytest --cov dbx -s -n 2

create-dev-env:
	conda create -n $(ENV_NAME) python=3.7

install-dev-reqs:
	pip install -r requirements.txt

test-init:
	dbx init \
		--project-name basic_dbx \
		--cloud="AWS" \
		--pipeline-engine="GitHub Actions" \
		--override
