##############################################################################
# NOTE: Make sure you have `pyenv` and `pyenv-virtualenv` installed beforehand
#
# https://github.com/pyenv/pyenv
# https://github.com/pyenv/pyenv-virtualenv
#
# On a Mac: $ brew install pyenv pyenv-virtualenv
#
# Configure your shell with $ eval "$(pyenv virtualenv-init -)"
#
##############################################################################
# Make file tutorials: https://makefiletutorial.com/#getting-started
##############################################################################
# Default shell that make will use.
SHELL=/bin/bash


##############################################################################
PYTHON_VERSION=3.7.5
VENV=dbx-local-dev-env-${PYTHON_VERSION}
VENV_DIR=$(shell pyenv root)/versions/${VENV}
PYTHON=${VENV_DIR}/bin/python
##############################################################################

##############################################################################
# Terminal Colors:
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


# This Makefile is for project development purposes only.
.PHONY: help helper-line clean venv install install-editable install-dev-dependencies lint check fix test build docs
.DEFAULT_GOAL := help

##############################################################################
# Auto document targets by adding comment next to target name
# starting with double pound and space like "## doc text"
##############################################################################
helper-line:
	@echo "${BOLD}${BLUE}--------------------------------------------------${NORMAL}"

help: ## Show help documentation.
	@make helper-line
	@echo "${BOLD}${BLUE}Here are all the targets available with make command.${NORMAL}"
	@make helper-line
	@echo ""
	@grep -E '^[a-zA-Z_0-9%-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "    ${YELLOW}%-30s${NORMAL} %s\n", $$1, $$2}'

##############################################################################

clean: ## Clean virtualenvs, dist, build
	@echo ""
	@echo "${YELLOW}Removing virtual environment ${NORMAL}"
	@make helper-line
	-pyenv virtualenv-delete --force ${VENV}
	-rm .python-version

	@echo ""
	@echo "${YELLOW}Remove build and dist${NORMAL}"
	@make helper-line
	-rm -rf dist/*
	-rm -rf build/*

	@make helper-line
	@echo "${YELLOW}Current python:${NORMAL}"
	@python --version

##############################################################################

# not sure why this separation is needed, but it is.
venv: $(VENV_DIR)

# not sure why this separation is needed, but it is.
$(VENV_DIR):
	@echo ""
	@echo "${YELLOW}Init virtual env${NORMAL}"
	@make helper-line
	# python3 -m pip install --upgrade pip
	pyenv install -s ${PYTHON_VERSION}
	pyenv virtualenv ${PYTHON_VERSION} ${VENV}
	@echo ${VENV} > .python-version
	$(PYTHON) -m pip install --upgrade pip

##############################################################################

install: venv install-editable install-dev-dependencies ## Install everything [editable, dev-dependencies]

install-editable: ## Install project as editable.
	$(PYTHON) -m pip install -e .

install-dev-dependencies: ## Install dev dependencies.
	$(PYTHON) -m pip install -r dev-requirements.txt

##############################################################################

lint: ## Run the lint and checks
	$(PYTHON) -m prospector --profile prospector.yaml
	$(PYTHON) -m rstcheck README.rst
	@make check

check: ## Run black checks
	$(PYTHON) -m black --check ./dbx
	$(PYTHON) -m black --check ./tests

fix: ## fix the code with black formatter.
	$(PYTHON) -m black ./dbx
	$(PYTHON) -m black ./tests

##############################################################################

test: ## Run the tests. (option): file=tests/path/to/file.py
	$(PYTHON) -m pytest -vv --cov dbx $(file)

unit-test: ## Run all unit tests.
	$(PYTHON) -m pytest tests/unit --cov dbx

test-with-html-report: ## Run all tests with html reporter.
	$(PYTHON) -m pytest --cov dbx --cov-report html -s

##############################################################################

docs:## Build the docs.
	cd docs && make html

build: ## Build the package.
	rm -rf dist/*
	rm -rf build/*
	$(PYTHON) setup.py clean bdist_wheel
