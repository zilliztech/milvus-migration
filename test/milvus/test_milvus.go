package main

import (
	"context"
	"fmt"
	"github.com/milvus-io/milvus-sdk-go/v2/client"
	"testing"
)

func TestCount(t *testing.T) {
	ctx := context.Background()

	milvusx, _ := client.NewDefaultGrpcClient(ctx, "localhost:19530")
	listCollection(milvusx, ctx)

	//var collectionName = "test_mul_field"
	//load(milvusx, ctx, collectionName)

	//var collectionName3 = "test_mul_field3"
	//load(milvusx, ctx, collectionName3)

}

func main() {
	ctx := context.Background()

	milvusx, _ := client.NewDefaultGrpcClient(ctx, "localhost:19530")
	ListBulkInsertTasks(milvusx, ctx, "test_mul_field2")
}

func listCollection(milvusx client.Client, ctx context.Context) (bool, error) {
	lists, _ := milvusx.ListCollections(ctx)
	for _, coll := range lists {
		fmt.Printf(coll.Name + ", " + string(coll.ShardNum))
		stats, _ := milvusx.GetCollectionStatistics(ctx, coll.Name)
		fmt.Println(stats)
	}
	return true, nil
}

func load(milvusx client.Client, ctx context.Context, collectionName string) error {
	err := milvusx.LoadCollection(ctx, collectionName, false)
	fmt.Println(err)
	return err
}

func ListBulkInsertTasks(milvusx client.Client, ctx context.Context, collectionName string) error {
	var limit int64 = 10
	stateList, err := milvusx.ListBulkInsertTasks(ctx, collectionName, limit)
	for idx, state := range stateList {
		fmt.Println(stateList[idx])
		fmt.Println(state)
	}
	fmt.Println(err)
	return err
}
