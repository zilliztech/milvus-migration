package main

import (
	"bytes"
	"errors"
	"fmt"
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/elastic/go-elasticsearch/v8/esapi"
	"github.com/tidwall/gjson"
	bizlog "github.com/zilliztech/milvus-migration/internal/log"
	"go.uber.org/zap"
	"io"
	"log"
)

func main() {
	log.SetFlags(0)

	//esClient := getClient()
	esClient := getClientByCloudId()
	fmt.Println(esClient)

	//var index = "test_mul_field"
	var index = "test_elastic"

	// Index 100 documents into the "test-scroll" index
	//insertVector(esClient)

	//scrollVector(esClient)
	Info_(index, esClient)

	Count_(index, esClient)

	Mapping_(index, esClient)
}

func Count_(index string, client *elasticsearch.Client) error {
	resp, err := client.Count(client.Count.WithIndex(index))
	if err != nil {
		bizlog.Error("Count ES Index Response Error",
			zap.String("Index", index), zap.Error(err))
		return err
	}
	if resp.IsError() {
		bizlog.Error("Count ES Index Response Data Error", zap.Int("code", resp.StatusCode),
			zap.String("Index", index), zap.String("error", resp.String()))
		return errors.New(resp.String())
	}
	bizlog.Info(resp.String())
	json := read_(resp.Body)
	log.Println(json)
	return err
}

func Mapping_(index string, client *elasticsearch.Client) error {
	resp, err := client.Indices.GetMapping(client.Indices.GetMapping.WithIndex(index))
	err2 := printMappingInfo_(index, err, resp)
	if err2 != nil {
		return err2
	}
	return err
}

func printMappingInfo_(index string, err error, resp *esapi.Response) error {
	if err != nil {
		bizlog.Error(" ES Response Error",
			zap.String("Index", index), zap.Error(err))
		return err
	}
	if resp.IsError() {
		bizlog.Error(" ES  Response Data Error", zap.Int("code", resp.StatusCode),
			zap.String("Index", index), zap.String("error", resp.String()))
		return errors.New(resp.String())
	}
	bizlog.Info(resp.String())
	json := read_(resp.Body)
	log.Println(json)
	/*
		{"test-vector":{"mappings":{"properties":{"my_vector":{"type":"dense_vector","dims":3},"title":{"type":"keyword"}}}}}
	*/
	//bjson.Unmarshal(json, maps)
	res := gjson.Get(json, index+".mappings.properties")
	mapResult := res.Map()
	for key, res := range mapResult {
		log.Println(key)
		log.Println(res.String())
	}
	return nil
}

func Info_(index string, client *elasticsearch.Client) error {
	resp, err := client.Info(client.Info.WithHuman())
	if err != nil {
		bizlog.Error("Count ES Index Response Error",
			zap.String("Index", index), zap.Error(err))
		return err
	}
	if resp.IsError() {
		bizlog.Error("Count ES Index Response Data Error", zap.Int("code", resp.StatusCode),
			zap.String("Index", index), zap.String("error", resp.String()))
		return errors.New(resp.String())
	}
	bizlog.Info(resp.String())
	json := read_(resp.Body)
	log.Println(json)
	return err
	/*
		{
		  "name" : "8962736dd32a",
		  "cluster_name" : "elasticsearch",
		  "cluster_uuid" : "tc_P2LchQDyhvD_fGqdIMA",
		  "version" : {
		    "number" : "7.17.0",
		    "build_flavor" : "default",
		    "build_type" : "docker",
		    "build_hash" : "bee86328705acaa9a6daede7140defd4d9ec56bd",
		    "build_date" : "2022-01-28T08:36:04.875279988Z",
		    "build_snapshot" : false,
		    "lucene_version" : "8.11.1",
		    "minimum_wire_compatibility_version" : "6.8.0",
		    "minimum_index_compatibility_version" : "6.0.0-beta1"
		  },
		  "tagline" : "You Know, for Search"
		}

	*/
}

func getClientByCloudId() *elasticsearch.Client {
	cfg := elasticsearch.Config{
		//CloudID: "https://my-deployment-8a9da2.es.us-central1.gcp.cloud.es.io",
		CloudID: "My_deployment:dXMtY2VudHJhbDEuZ2NwLmNsb3VkLmVzLmlvOjQ0MyQyOGUzYzNiYTc4ZGI0N2MxOTc0NWFlMDRjOGZlY2VmMyQxOTVlZTMzYWM2Mzg0ZDE3YWRhNTVhZGM4MDgwMThmNQ==",

		APIKey: "SlRfV0Nva0JudUprS2VFOHNPYzg6MHNPSFlHcUtUMzZPdXhHWjBiU0lhZw==",
	}
	esClient, err := elasticsearch.NewClient(cfg)
	if err != nil {
		log.Fatalf("new client error: %s", err)
		return nil
	}
	return esClient
}

func read_(r io.Reader) string {
	var b bytes.Buffer
	b.ReadFrom(r)
	return b.String()
}
