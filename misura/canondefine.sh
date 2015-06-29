#!/bin/bash

T=`readlink -m "${BASH_SOURCE[0]}"`
CANONROOT="$( cd "$( dirname "${T}" )"/.. && pwd )"
export PYTHONPATH=$CANONROOT:$PYTHONPATH
