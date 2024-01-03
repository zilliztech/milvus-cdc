#!/bin/bash

# get current git branch
BRANCH=$(git symbolic-ref --short HEAD)

# get current git commit id
COMMIT_ID=$(git rev-parse --short HEAD)

# build docker image
docker build --build-arg GIT_COMMIT_ARG=${COMMIT_ID} -t milvus-cdc:${BRANCH}-${COMMIT_ID} .
docker tag milvus-cdc:${BRANCH}-${COMMIT_ID} milvus-cdc:latest