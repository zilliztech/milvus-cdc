ROOT_DIR="$( dirname $( dirname "$0" ) )"

pushd "${ROOT_DIR}/core"
go test -race -cover ${APPLE_SILICON_FLAG} "./..." -failfast
popd
pushd "${ROOT_DIR}/server"
go test -race -cover ${APPLE_SILICON_FLAG} "./..." -failfast
popd