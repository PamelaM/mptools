#!/usr/bin/env bash

export PYTHONPATH="`(pwd)`"
rm -rf send_file.txt || true
python examples/mptools_example_client.py AUTO &
python examples/mptools_example.py
rm -rf send_file.txt || true

