# This Makefile is for project development purposes only.
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

test-init-aws:
	rm -rf dbx_dev_aws
	cookiecutter --no-input \
		https://github.com/databrickslabs/cicd-templates.git \
		project_name="dbx_dev_aws"
	dbx init \
		--project-name="dbx_dev_aws" \
		--project-local-dir="./dbx_dev_aws"
#	cd dbx_dev_aws && dbx configure \
#		--name="test" \
#		--profile="dbx-dev-aws" \
#		--workspace-dir="/Shared/dbx/projects/dbx_dev_aws"
#	cd dbx_dev_aws && dbx deploy \
#		--environment=test \
#		--dirs="pipelines"
#	cd dbx_dev_aws && dbx launch \
#		--environment=test \
#		--entrypoint-file="pipelines/pipeline1/pipeline_runner.py" \
#		--job-conf-file="pipelines/pipeline1/job_spec_aws.json"

test-init-azure:
	rm -rf dbx_dev_azure
	cookiecutter --no-input \
		https://github.com/databrickslabs/cicd-templates.git \
		project_name="dbx_dev_azure"
	dbx init \
		--project-name="dbx_dev_azure" \
		--project-local-dir="./dbx_dev_azure"
	cd dbx_dev_azure && dbx configure \
		--name="test" \
		--profile="dbx-dev-azure" \
		--workspace-dir="/dbx/projects/dbx_dev_azure"
#	cd dbx_dev_aws && dbx deploy \
#		--environment=test \
#		--dirs="pipelines"