#!/bin/bash -ex

prospector --profile prospector.yaml

black --check ./dbx
black --check ./tests

isort -c ./dbx
isort -c ./tests

rstcheck README.rst
