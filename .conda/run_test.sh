#!/bin/bash

set -ev

SRC_DIR="${RECIPE_DIR}/.."

python -m pytest -c "${SRC_DIR}/setup.cfg" --cov="${SP_DIR}/${PKG_NAME}" "${SRC_DIR}"
