#!/bin/bash

set -ev

PACKAGE_ROOT_DIR="${RECIPE_DIR}/.."

python -m pytest \
    -c "${PACKAGE_ROOT_DIR}/setup.cfg" \
    --cov="${SP_DIR}/${PKG_NAME}" \
    "${PACKAGE_ROOT_DIR}"
