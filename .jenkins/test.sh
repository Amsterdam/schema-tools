#!/usr/bin/env bash

set -u # crash on missing env
set -e # stop on any error

echo "Waiting for db"
source .jenkins/docker-wait.sh

echo "Running style checks"
flake8 --config=.flake8 ./ || true

echo "Running tests"
pytest --nomigrations -vs tests django_tests
