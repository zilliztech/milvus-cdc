package store

import (
	"testing"

	"github.com/stretchr/testify/assert"
)

func TestGetTaskInfoPrefix(t *testing.T) {
	rootPath := "/root"
	expected := "/root/task_info/"
	actual := getTaskInfoPrefix(rootPath)
	assert.Equal(t, expected, actual)
}

func TestGetTaskInfoKey(t *testing.T) {
	rootPath := "/root"
	taskID := "1234"
	expected := "/root/task_info/1234"
	actual := getTaskInfoKey(rootPath, taskID)
	assert.Equal(t, expected, actual)
}

func TestGetTaskCollectionPositionPrefix(t *testing.T) {
	rootPath := "/root"
	expected := "/root/task_position/"
	actual := getTaskCollectionPositionPrefix(rootPath)
	assert.Equal(t, expected, actual)
}

func TestGetTaskCollectionPositionPrefixWithTaskID(t *testing.T) {
	rootPath := "/root"
	taskID := "1234"
	expected := "/root/task_position/1234/"
	actual := getTaskCollectionPositionPrefixWithTaskID(rootPath, taskID)
	assert.Equal(t, expected, actual)
}

func TestGetTaskCollectionPositionKey(t *testing.T) {
	rootPath := "/root"
	taskID := "1234"
	collectionID := int64(5678)
	expected := "/root/task_position/1234/5678"
	actual := getTaskCollectionPositionKey(rootPath, taskID, collectionID)
	assert.Equal(t, expected, actual)
}
