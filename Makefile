# This Makefile is for project development purposes only.
ENV_NAME=dbx

install-dev:
	pip install -e .

test:
	pytest --cov dbx -s -n 2

test-only-aws:
	pytest --cov dbx -s -k "test_aws"

test-only-azure:
	pytest --cov dbx -s -k "test_azure"

create-dev-env:
	conda create -n $(ENV_NAME) python=3.7

install-dev-reqs:
	pip install -U -r requirements.txt

test-init-aws:
	rm -rf dbx_dev_aws
	dbx init \
		--profile dbx-dev-aws \
		--project-name dbx_dev_aws \
		--dbx-workspace-dir="/Shared/dbx/projects" \
		--dbx-artifact-location="dbfs:/tmp/dbx/projects" \
		--cloud="AWS" \
		--pipeline-engine="GitHub Actions" \
		--override

test-init-azure:
	rm -rf dbx_dev_azure
	dbx init \
		--profile dbx-dev-azure \
		--project-name dbx_dev_azure \
		--cloud="Azure" \
		--pipeline-engine="GitHub Actions" \
		--override