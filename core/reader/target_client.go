package reader

import (
	"context"

	"github.com/cockroachdb/errors"
	"github.com/milvus-io/milvus-sdk-go/v2/client"
	"github.com/milvus-io/milvus/pkg/log"
	"go.uber.org/zap"

	"github.com/zilliztech/milvus-cdc/core/api"
	"github.com/zilliztech/milvus-cdc/core/model"
)

var _ api.TargetAPI = (*TargetClient)(nil)

type TargetClient struct {
	client client.Client
}

type TargetConfig struct {
	Address   string
	Username  string
	Password  string
	EnableTLS bool
}

func NewTarget(ctx context.Context, config TargetConfig) (api.TargetAPI, error) {
	targetClient := &TargetClient{}
	var err error
	targetClient.client, err = client.NewClient(ctx, client.Config{
		Address:       config.Address,
		Username:      config.Username,
		Password:      config.Password,
		EnableTLSAuth: config.EnableTLS,
	})
	if err != nil {
		log.Warn("fail to new target client", zap.String("address", config.Address), zap.Error(err))
		return nil, err
	}
	return targetClient, nil
}

func (t *TargetClient) GetCollectionInfo(ctx context.Context, collectionName string) (*model.CollectionInfo, error) {
	collectionInfo := &model.CollectionInfo{}
	collection, err := t.client.DescribeCollection(ctx, collectionName)
	if err != nil {
		log.Warn("fail to describe collection", zap.Error(err))
		return nil, err
	}
	collectionInfo.CollectionID = collection.ID
	collectionInfo.CollectionName = collectionName
	collectionInfo.PChannels = collection.PhysicalChannels
	collectionInfo.VChannels = collection.VirtualChannels

	tmpCollectionInfo, err := t.GetPartitionInfo(ctx, collectionName)
	if err != nil {
		log.Warn("fail to get partition info", zap.Error(err))
		return nil, err
	}
	collectionInfo.Partitions = tmpCollectionInfo.Partitions
	return collectionInfo, nil
}

func (t *TargetClient) GetPartitionInfo(ctx context.Context, collectionName string) (*model.CollectionInfo, error) {
	collectionInfo := &model.CollectionInfo{}
	partition, err := t.client.ShowPartitions(ctx, collectionName)
	if err != nil || len(partition) == 0 {
		log.Warn("failed to show partitions", zap.Error(err))
		return nil, errors.New("fail to show the partitions")
	}
	partitionInfo := make(map[string]int64, len(partition))
	for _, e := range partition {
		partitionInfo[e.Name] = e.ID
	}
	collectionInfo.Partitions = partitionInfo
	return collectionInfo, nil
}
