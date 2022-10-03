#!/bin/bash

HERE=`dirname "$(realpath $0)"`
BUILD=( $HERE/build/lib.* )
export PYTHONPATH=${BUILD[0]}:$PYTHONPATH
export HG_LOG_LEVEL=""

python3 -m unittest --verbose
