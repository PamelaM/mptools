 MP Tools
===========

**MP Tools** provides classes and functions that make writing complex multiprocessing easier.

This project began while working on a number of IoT projects and was fleshed out while I was 
writing up a talk proposal and blog post based on what I'd learned. _TODO: Url goes here_

Project Home: https://github.com/PamelaM/mptools
Build Status: [![Build Status](https://travis-ci.org/PamelaM/mptools.svg?branch=master)](https://travis-ci.org/PamelaM/mptools)

Examples
--------

 * `examples/mptools_example.py` provides a fairly complete sketch of an IoT kind of app, though
it doesn't do much.  
 * `examples/mptools_example_client.py` is a simple script that exercises the socket
interface of the example app.

Development
-----------

 * Clone git repo
 * Create a virtual environment, Python v 3.6+
 * pip install -r requirements.txt
 * pytest --cov=mptools --cov-report html:skip-covered -v
 * _TODO: Build and test examples_
 * _TODO: Update Version and make a release_

Roadmap
--------
 
 * Write up proper docs and get them up on Readthedocs
 
 