package reader

import (
	"encoding/json"
	"testing"

	"github.com/milvus-io/milvus/pkg/mq/msgstream"
	"github.com/stretchr/testify/assert"

	"github.com/zilliztech/milvus-cdc/core/config"
	"github.com/zilliztech/milvus-cdc/core/util"
)

func TestDefaultFactoryCreator(t *testing.T) {
	c := NewDefaultFactoryCreator()
	t.Run("pulsar", func(t *testing.T) {
		{
			f := c.NewPmsFactory(&config.PulsarConfig{})
			pulsarFactory := f.(*msgstream.PmsFactory)
			assert.Equal(t, "{}", pulsarFactory.PulsarAuthParams)
		}

		{
			f := c.NewPmsFactory(&config.PulsarConfig{
				AuthParams: "a:b,c:d",
			})
			pulsarFactory := f.(*msgstream.PmsFactory)

			jsonMap := map[string]string{
				"a": "b",
				"c": "d",
			}
			jsonData, _ := json.Marshal(&jsonMap)
			authParams := util.ToString(jsonData)
			assert.Equal(t, authParams, pulsarFactory.PulsarAuthParams)
		}
	})

	t.Run("kafka", func(t *testing.T) {
		c.NewKmsFactory(&config.KafkaConfig{})
	})
}
