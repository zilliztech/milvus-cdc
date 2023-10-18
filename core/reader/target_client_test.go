package reader

import (
	"context"
	"fmt"
	"testing"
)

func TestTargetClient(t *testing.T) {
	ctx := context.Background()
	target, _ := NewTarget(ctx, TargetConfig{
		Address:   "in01-f562eb88009199f.aws-us-west-2.vectordb-uat3.zillizcloud.com:19534",
		Username:  "root",
		Password:  "T3]S0^1%XB1aD}}ft^fKLCv&(j]}V[*V",
		EnableTLS: true,
	})
	fmt.Println(target.GetCollectionInfo(ctx, "foo2"))
}
