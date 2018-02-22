#!/bin/bash

set -ex

function echo_err {
    echo "$@" 1>&2
}

function report_error {
    echo_err "ERROR!"
}
trap report_error ERR

exec 1> "$STDOUT_LOG.tmp"
exec 2> "$STDERR_LOG.tmp"

eval $SYSTEM_COMMAND

mv "$STDOUT_LOG.tmp" "$STDOUT_LOG"
mv "$STDERR_LOG.tmp" "$STDERR_LOG"

echo_err "DONE!"
