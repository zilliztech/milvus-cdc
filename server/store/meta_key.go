package store

import (
	"path"
	"strconv"
)

const (
	taskInfoPrefix     = "task_info"
	taskPositionPrefix = "task_position"
)

func getTaskInfoPrefix(rootPath string) string {
	return path.Join(rootPath, taskInfoPrefix) + "/"
}

func getTaskInfoKey(rootPath string, taskID string) string {
	return path.Join(rootPath, taskInfoPrefix, taskID)
}

func getTaskCollectionPositionPrefix(rootPath string) string {
	return path.Join(rootPath, taskPositionPrefix) + "/"
}

func getTaskCollectionPositionPrefixWithTaskID(rootPath string, taskID string) string {
	return path.Join(rootPath, taskPositionPrefix, taskID) + "/"
}

func getTaskCollectionPositionKey(rootPath string, taskID string, collectionID int64) string {
	return path.Join(rootPath, taskPositionPrefix, taskID, strconv.FormatInt(collectionID, 10))
}
