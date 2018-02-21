#!/bin/bash

set -ev

cd "${RECIPE_DIR}/.."
flake8
python -m pytest --cov="${SP_DIR}/${PKG_NAME}"
