#!/usr/bin/env bash

ROOT_DIR="$( dirname $( dirname "$0" ) )"
LINT_CONFIG="${ROOT_DIR}"/.golangci.yml

golangci-lint cache clean
DIR_ARR=("core" "server")
for i in "${DIR_ARR[@]}"
do
  pushd "${ROOT_DIR}/${i}"
  golangci-lint run --timeout=30m --config "$LINT_CONFIG" ./...
  popd
done