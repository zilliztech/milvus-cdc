#!/usr/bin/env bash

set -e

ROOT_DIR="$( dirname $( dirname "$0" ) )"
COVER_OUT="${ROOT_DIR}"/coverage.project.out

echo "" > "$COVER_OUT"

DIR_ARR=("core" "server")
for i in "${DIR_ARR[@]}"
do
    pushd "${ROOT_DIR}/${i}"
    go mod tidy
    go test -race -coverprofile=coverage.out -covermode=atomic "./..." -v
    if [[ -f coverage.out ]]; then
        cat coverage.out >> "$COVER_OUT"
        rm coverage.out
    fi
    popd
done