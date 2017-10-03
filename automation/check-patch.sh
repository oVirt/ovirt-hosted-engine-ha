#!/bin/bash -e
./autogen.sh --system
make distcheck

./automation/build-artifacts.sh
