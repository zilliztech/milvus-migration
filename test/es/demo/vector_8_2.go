package main

import (
	"bytes"
	"errors"
	"github.com/elastic/go-elasticsearch/v7"
	bizlog "github.com/zilliztech/milvus-migration/internal/log"
	"go.uber.org/zap"
	"io"
	"log"
	"os"
)

func main() {
	log.SetFlags(0)

	esClient := getClient8_2()

	//var index2 = "test-vector3"
	//insertVector2(esClient, index2, 10000)

	Info8_2(esClient)
}

func getClient8_2() *elasticsearch.Client {

	//cert, _ := os.ReadFile("/Users/zilliz/gitCode/cloud_team/milvus-migration/files/cert_8_2/http_ca.crt")
	//cert, _ := os.ReadFile("/Users/zilliz/gitCode/cloud_team/milvus-migration/files/10_15_9_78/http_ca.crt")
	cert, _ := os.ReadFile("/Users/zilliz/gitCode/cloud_team/milvus-migration/files/10_15_11_224/http_ca.crt")

	cfg := elasticsearch.Config{
		//Addresses: []string{"https://localhost:9200"},
		//Addresses: []string{"http://10.15.9.78:9700"},
		Addresses: []string{"https://10.15.11.224:9200"},
		Username:  "elastic", //1
		Password:  "elastic",

		//CloudID: "xx", //2.es cloud: if set cloudId, Address cannot set, Error :new client error: cannot create client: both Addresses and CloudID are set
		//APIKey: "xx",

		//ServiceToken: "eyJ2ZXIiOiI4LjIuMiIsImFkciI6WyIxMC4xNS4xMS4yMjQ6OTIwMCJdLCJmZ3IiOiIyYjNhYWMyZDgwMGU0NzhkODFlMzNhMWQ4OTY4ZGM2YzcwMTA4NTk3YTQ5ODc3ZWY0MzM4ZWEzNDA2OTU3YzgyIiwia2V5IjoiNTBpd3JvZ0JhQTVjNjhndGZldWc6RV9MSnhOdUxUY3lucXd5ZGFkbE1ZUSJ9", //3.bearer token

		// ca, fingerprint 二选1都支持：verifying the HTTPS connection
		CACert: cert, //2:custom certificate authority
		//CACert: nil, //2:custom certificate authority
		//CertificateFingerprint: "",
		//CertificateFingerprint: "fabfe5c3783a06514e72448d3d041e229a6d1afaeaa98fcbbd1bd35c76242767", //
		//CertificateFingerprint: "3c2c7ca44a998b07a793b2863fe50f6ef9e94aa2b34d2ddaa38e7db20cf01929", //78
		CertificateFingerprint: "2b3aac2d800e478d81e33a1d8968dc6c70108597a49877ef4338ea3406957c82", //224

	}
	esClient, err := elasticsearch.NewClient(cfg)
	if err != nil {
		log.Fatalf("new client error: %s", err)
		return nil
	}
	return esClient
}
func Info8_2(client *elasticsearch.Client) error {
	resp, err := client.Info(client.Info.WithHuman())
	if err != nil {
		bizlog.Error("Count ES Index Response Error", zap.Error(err))
		return err
	}
	if resp.IsError() {
		bizlog.Error("Count ES Index Response Data Error", zap.Int("code", resp.StatusCode), zap.String("error", resp.String()))
		return errors.New(resp.String())
	}
	bizlog.Info(resp.String())
	json := read8_2(resp.Body)
	log.Println(json)
	return err
}

func read8_2(r io.Reader) string {
	var b bytes.Buffer
	b.ReadFrom(r)
	return b.String()
}
