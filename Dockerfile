FROM python:3.7

ENV RUN_TYPE="tests"

# Builds started failing on 2020-03-01 with new pybuilder version, figure out later
RUN pip install -U pybuilder

