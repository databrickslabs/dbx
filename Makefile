# Make file tutorials: https://makefiletutorial.com/#getting-started
SHELL=/bin/bash
PYTHON_VERSION=3.7.5
VENV=dbx-local-dev-env-${PYTHON_VERSION}
VENV_DIR=$(shell pyenv root)/versions/${VENV}
PYTHON=${VENV_DIR}/bin/python

## Make sure you have `pyenv` and `pyenv-virtualenv` installed beforehand
##
## https://github.com/pyenv/pyenv
## https://github.com/pyenv/pyenv-virtualenv
##
## On a Mac: $ brew install pyenv pyenv-virtualenv
##
## Configure your shell with $ eval "$(pyenv virtualenv-init -)"
##

# to see all colors, run
# bash -c 'for c in {0..255}; do tput setaf $c; tput setaf $c | cat -v; echo =$c; done'
# the first 15 entries are the 8-bit colors

# define standard colors
ifneq (,$(findstring xterm,${TERM}))
	BOLD         := $(shell tput -Txterm bold)
	UNDERLINE    := $(shell tput -Txterm smul)
	STANDOUT     := $(shell tput -Txterm smso)
	BLACK        := $(shell tput -Txterm setaf 0)
	RED          := $(shell tput -Txterm setaf 1)
	GREEN        := $(shell tput -Txterm setaf 2)
	YELLOW       := $(shell tput -Txterm setaf 3)
	BLUE         := $(shell tput -Txterm setaf 4)
	PURPLE       := $(shell tput -Txterm setaf 5)
	CYAN         := $(shell tput -Txterm setaf 6)
	WHITE        := $(shell tput -Txterm setaf 7)
	NORMAL := $(shell tput -Txterm sgr0)
else
	BOLD         := ""
	UNDERLINE    := ""
	STANDOUT     := ""
	BLACK        := ""
	RED          := ""
	GREEN        := ""
	YELLOW       := ""
	BLUE         := ""
	PURPLE       := ""
	CYAN         := ""
	WHITE        := ""
	NORMAL       := ""
endif

# set target color
TARGET_COLOR := $(BLUE)

POUND = \#

# This Makefile is for project development purposes only.
.PHONY: help clean venv install lint fix test build docs
ENV_NAME=dbx

.DEFAULT_GOAL := help


helper-line:
	@echo "${BOLD}${BLUE}--------------------------------------------------${NORMAL}"

help:
	@$(MAKE) helper-line
	@echo "${BOLD}${BLUE}Here are all the tasks available with make command.${NORMAL}"
	@$(MAKE) helper-line

	@echo "    ${YELLOW}help${NORMAL}                         display the help text"
	@echo ""
	@echo "    ${YELLOW}create-dev-env-conda${NORMAL}                         "
	@echo "    ${YELLOW}pyenv-create-env${NORMAL}                         "
	@echo "    ${YELLOW}pyenv-delete-env${NORMAL}                         "
	@echo ""
	@echo "    ${YELLOW}install${NORMAL}                      Install self as editable and its dev dependencies."
	@echo "    ${YELLOW}install-editable${NORMAL}             Install self as editable package."
	@echo "    ${YELLOW}install-dev-dependencies${NORMAL}     Install the development dependencies."
	@echo ""
	@echo "    ${YELLOW}lint${NORMAL}                         "
	@echo "    ${YELLOW}fix${NORMAL}                         "
	@echo ""
	@echo "    ${YELLOW}test${NORMAL}                         "
	@echo "    ${YELLOW}unit-test${NORMAL}                         "
	@echo "    ${YELLOW}test-with-html-report${NORMAL}                         "
	@echo ""
	@echo "    ${YELLOW}docs${NORMAL}                         "
	@echo "    ${YELLOW}build${NORMAL}                         "


clean: ## >> remove all environment and build files
	@echo ""
	@echo "${YELLOW}Removing virtual environment ${NORMAL}"
	@make helper-line
	-pyenv virtualenv-delete --force ${VENV}
	-rm .python-version

	@make helper-line
	@echo "${YELLOW}Current python:${NORMAL}"
	@python --version

# Will need to source this after `make create-dev-env-pyenv`
# source ~/.pyenv/versions/dbx-local-dev-env.3.7.5/bin/activate
venv: $(VENV_DIR)

$(VENV_DIR):
	@echo "${YELLOW}Init virtual env${NORMAL}"
	@make helper-line
	# python3 -m pip install --upgrade pip
	pyenv install -s ${PYTHON_VERSION}
	pyenv virtualenv ${PYTHON_VERSION} ${VENV}
	echo ${VENV} > .python-version
	$(PYTHON) -m pip install --upgrade pip


install: venv install-editable install-dev-dependencies

install-editable:
	$(PYTHON) -m pip install -e .

install-dev-dependencies:
	$(PYTHON) -m pip install -r dev-requirements.txt



lint:
	$(PYTHON) -m prospector --profile prospector.yaml
	$(PYTHON) -m rstcheck README.rst
	@make check

check:
	$(PYTHON) -m black --check ./dbx
	$(PYTHON) -m black --check ./tests


fix:
	$(PYTHON) -m black ./dbx
	$(PYTHON) -m black ./tests


test:
	$(PYTHON) -m pytest -vv --cov dbx $(file)

unit-test:
	$(PYTHON) -m pytest tests/unit --cov dbx

test-with-html-report:
	$(PYTHON) -m pytest --cov dbx --cov-report html -s



docs:
	cd docs && make html

build:
	rm -rf dist/*
	rm -rf build/*
	$(PYTHON) setup.py clean bdist_wheel
