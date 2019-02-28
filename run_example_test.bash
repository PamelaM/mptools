#!/usr/bin/env bash

export PYTHONPATH="`(pwd)`"
python examples/mptools_example_client.py AUTO &
python examples/mptools_example.py
