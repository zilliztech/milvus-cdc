package tag

import (
	"fmt"

	"go.uber.org/zap"

	"github.com/zilliztech/milvus-cdc/core/log"
)

var (
	BuildTime = "unknown"
	GitCommit = "unknown"
	GoVersion = "unknown"
)

func LogInfo() {
	log.Info("base info", zap.String("BuildTime", BuildTime), zap.String("GitCommit", GitCommit), zap.String("GoVersion", GoVersion))
}

//nolint
func PrintInfo() {
	fmt.Println("# Build time:", BuildTime)
	fmt.Println("# Git commit:", GitCommit)
	fmt.Println("# Go version:", GoVersion)
}
