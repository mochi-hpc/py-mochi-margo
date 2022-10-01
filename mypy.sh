#!/bin/bash

HERE=`dirname "$(realpath $0)"`
BUILD=( $HERE/build/lib.* )
export PYTHONPATH=${BUILD[0]}:$PYTHONPATH
export MYPYPATH=$HERE/stubs

pybind11-stubgen --root-module-suffix "" _pymargo

# fix missing buffer class
printf "from typing import Any\nbuffer = Any\n" \
    | cat - $HERE/stubs/_pymargo/__init__.pyi > temp \
    && mv temp $HERE/stubs/_pymargo/__init__.pyi

mypy $HERE/pymargo
