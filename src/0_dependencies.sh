#!/bin/bash

# create and activate a virtual environment if it doesn't exist
[ -d "env" ] || python3 -m venv env
source env/bin/activate

# install required packages from the requirements.txt file
pip install -r requirements.txt