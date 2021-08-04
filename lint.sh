#!/bin/bash -ex

prospector --profile prospector.yaml

black --check ./dbx
black --check ./tests

rstcheck README.rst
