package server

import (
	"os"
	"sync"
	"testing"
	"time"

	"github.com/stretchr/testify/assert"
	"go.uber.org/zap"
	"sigs.k8s.io/yaml"
)

func TestID(t *testing.T) {
	a := make(map[int]int)
	for i := 0; i < 10; i++ {
		a[i] = i
	}
	wg := sync.WaitGroup{}
	for j := range a {
		wg.Add(1)
		j := j
		go func() {
			defer wg.Done()
			time.Sleep(time.Millisecond * 100)
			println(j)
		}()
	}
	wg.Wait()
}

func TestYaml(t *testing.T) {
	//file, err := os.Open(filepath.Clean("/Users/derek/fubang/milvus-cdc/server/configs/cdc.yaml"))
	//if err != nil {
	//	t.Fail()
	//}
	//scanner := bufio.NewScanner(file)
	//fileContent := ""
	//for scanner.Scan() {
	//	fileContent += scanner.Text() + "\n"
	//}
	//if err := scanner.Err(); err != nil {
	//	t.Fail()
	//}

	fileContent, _ := os.ReadFile("/Users/derek/fubang/milvus-cdc/server/configs/cdc2.yaml")
	println(string(fileContent))
	var p2 CDCServerConfig
	err := yaml.Unmarshal(fileContent, &p2)
	assert.NoError(t, err)
	log.Info("cdc config", zap.Any("config", p2))
}
