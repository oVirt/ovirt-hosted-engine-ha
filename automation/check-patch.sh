#!/bin/bash -e
./autogen.sh --system
./configure
make distcheck
