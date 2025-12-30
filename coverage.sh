#!/bin/bash

HERE=`dirname "$(realpath $0)"`
BUILD=( $HERE/build/lib.* )
export PYTHONPATH=${BUILD[0]}:$PYTHONPATH
export HG_LOG_LEVEL=""

# Run tests with coverage
coverage run --source=mochi.margo -m unittest --verbose

# Generate coverage report
echo ""
echo "==================================="
echo "Coverage Report"
echo "==================================="
python3 -m coverage report

# Generate HTML coverage report
python3 -m coverage html
echo ""
echo "HTML coverage report generated in htmlcov/index.html"
