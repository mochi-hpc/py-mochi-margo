#!/bin/bash

HERE=`dirname "$(realpath $0)"`
BUILD=( $HERE/build/lib.* )
export PYTHONPATH=${BUILD[0]}:$PYTHONPATH

python3 -m unittest
