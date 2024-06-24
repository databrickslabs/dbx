##############################################################################
# NOTE: Make sure you have `pyenv` installed beforehand
# https://github.com/pyenv/pyenv
#
# On a Mac: $ brew install pyenv

##############################################################################
# Make file tutorials: https://makefiletutorial.com/#getting-started
##############################################################################
# Default shell that make will use.
SHELL=/bin/bash


##############################################################################
PYTHON_VERSION=3.8.13
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

##############################################################################
# Makefile TARGETS:
##############################################################################
.PHONY: help helper-line clean venv install install-e install-dev post-install-info lint check fix test test-with-html-report docs docs-serve build
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
	@echo "Start by running ${YELLOW}'make clean install'${NORMAL}"
	@echo ""
	@grep -E '^[a-zA-Z_0-9%-]+:.*?## .*$$' $(MAKEFILE_LIST) | awk 'BEGIN {FS = ":.*?## "}; {printf "    ${YELLOW}%-30s${NORMAL} %s\n", $$1, $$2}'

##############################################################################

clean: ## Clean .venv, dist, build
	@echo ""
	@echo "${YELLOW}Removing virtual environment ${NORMAL}"
	@make helper-line
	hatch env remove

	@echo ""
	@echo "${YELLOW}Remove temp files${NORMAL}"
	@make helper-line
	-rm -rf dist/*
	-rm -rf build/*
	-rm -rf dbx.egg-info/*



	@echo ""
	@echo "${YELLOW}Current python:${NORMAL}"
	@make helper-line
	@python --version

	@make docs-clean


##############################################################################

create-env:
	hatch env create

install: create-env install-e install-dev post-install-info ## >>> MAIN TARGET. Run this to start. <<<

install-e: ## Install project as editable.
	@echo ""
	@echo "${YELLOW}Install project as editable${NORMAL}"
	@make helper-line
	python -m pip install -e .

install-dev: ## Install dev dependencies.
	@echo ""
	@echo "${YELLOW}Install Dev dependencies.${NORMAL}"
	@make helper-line
	python -m pip install -e ".[dev]"

post-install-info: ## Just some post installation info.
	@echo ""
	@echo "${YELLOW}Post-Install Info:${NORMAL}"
	@make helper-line
	@echo "See what other make targets are available by running:"
	@echo "    ${YELLOW}'make'${NORMAL}"
	@echo "    ${YELLOW}'make help'${NORMAL}"
	@echo ""
	@echo "${YELLOW}NOTE${NORMAL}: Most of the time, you should be using the predefined make targets."
	@echo ""
	@echo "${YELLOW}Optionally${NORMAL}: If you really need to, you can activate the venv in your"
	@echo "terminal shell by running: "
	@echo "    ${YELLOW}'hatch shell'${NORMAL}"
	@echo ""
	@echo "This will put any executable python tools installed above in the PATH, allowing you"
	@echo "to run the tools from the shell if you really need to."
	@echo "    ex: ${YELLOW}'pytest'${NORMAL}"


##############################################################################

lint: ## Run the lint and checks
	@echo ""
	@echo "${YELLOW}Linting code:${NORMAL}"
	@make helper-line
	python -m prospector --profile prospector.yaml
	@make check

check: ## Run black checks
	@echo ""
	@echo "${YELLOW}Check code with black:${NORMAL}"
	@make helper-line
	python -m black --check .

fix: ## fix the code with black formatter.
	@echo ""
	@echo "${YELLOW}Fixing code with black:${NORMAL}"
	@make helper-line
	python -m black .

##############################################################################

test: ## Run the tests. (option): file=tests/path/to/file.py
	@echo ""
	@echo "${YELLOW}Running tests:${NORMAL}"
	@make helper-line
	python -m pytest -vv --cov dbx $(file) -n auto \
		--cov-report=xml \
		--cov-report=term-missing:skip-covered

test-with-html-report: ## Run all tests with html reporter.
	@echo ""
	@echo "${YELLOW}Testing with html report:${NORMAL}"
	@make helper-line
	python -m pytest --cov dbx -n auto --cov-report html -s

##############################################################################

DOCS_SOURCE=./docs/source
DOCS_BUILD=./docs/build

docs-clean: ## Clean the docs build folder
	@echo ""
	@echo "${YELLOW}Clean ${DOCS_BUILD} folder:${NORMAL}"
	@make helper-line
	-rm -rf ${DOCS_BUILD}/*

docs-serve: ## sphinx autobuild & serve docs on localhost
	mkdocs serve

##############################################################################

build: ## Build the package.
	@echo ""
	@echo "${YELLOW}Building the project:${NORMAL}"
	@make helper-line
	hatch  build -c -t wheel
