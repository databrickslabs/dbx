SHELL=/bin/bash

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
.PHONY: help build docs
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
	@echo "    ${YELLOW}create-dev-env-pyenv${NORMAL}                         "
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



install: install-editable install-dev-dependencies

install-editable:
	pip install -e .

install-dev-dependencies:
	pip install -r dev-requirements.txt

lint:
	./lint.sh

fix:
	black ./dbx
	black ./tests
	isort ./dbx
	isort ./tests


test:
	pytest --cov dbx

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
