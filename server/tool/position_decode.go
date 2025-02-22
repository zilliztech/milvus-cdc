package tool

import (
	"encoding/base64"

	"github.com/milvus-io/milvus-proto/go-api/v2/commonpb"
	"github.com/zilliztech/milvus-cdc/core/util"
)

// DecodeType0, same as the cdc create request
func DecodeType0(_, position string) (*commonpb.KeyDataPair, error) {
	msgPosition, err := util.Base64DecodeMsgPosition(position)
	if err != nil {
		return nil, err
	}
	return &commonpb.KeyDataPair{
		Key:  msgPosition.GetChannelName(),
		Data: msgPosition.GetMsgID(),
	}, nil
}

// DecodeType1, msg position raw data
func DecodeType1(channel, position string) (*commonpb.KeyDataPair, error) {
	positionBytes, err := base64.StdEncoding.DecodeString(position)
	if err != nil {
		return nil, err
	}
	return &commonpb.KeyDataPair{
		Key:  channel,
		Data: positionBytes,
	}, nil
}
