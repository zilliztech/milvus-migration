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

	esClient := getClient2()

	//var index2 = "test-vector3"
	//insertVector2(esClient, index2, 10000)

	Info7(esClient)
}

// https://www.elastic.co/guide/en/elasticsearch/client/go-api/8.8/connecting.html
func getClient2() *elasticsearch.Client {

	//- 10.15.9.78
	//- 127.0.0.1
	//- localhost
	//- elasticsearch7

	//cert, _ := os.ReadFile("/Users/zilliz/gitCode/cloud_team/milvus-migration/files/cert/ca.crt")
	//cert, _ := os.ReadFile("/Users/zilliz/gitCode/cloud_team/milvus-migration/files/cert/instance/instance.crt")

	//cert, _ := os.ReadFile("/Users/zilliz/gitCode/cloud_team/milvus-migration/files/cert2/ca/ca.crt")
	//cert, _ := os.ReadFile("/Users/zilliz/gitCode/cloud_team/milvus-migration/files/cert2/elasticsearch/elasticsearch.crt")

	cert, _ := os.ReadFile("/Users/zilliz/gitCode/cloud_team/milvus-migration/files/certificate-bundle-http/ca/ca.crt")

	cfg := elasticsearch.Config{
		//Addresses: []string{"http://10.15.9.78:9700"},
		Addresses: []string{"https://10.15.9.78:9700"},
		Username:  "elastic", //1
		Password:  "123456",

		//CloudID: "xx", //2.es cloud: if set cloudId, Address cannot set, Error :new client error: cannot create client: both Addresses and CloudID are set
		APIKey: "xx",

		ServiceToken: "xx", //3.bearer token

		// ca, fingerprint 二选1都支持：verifying the HTTPS connection
		CACert: cert, //2:custom certificate authority
		//CertificateFingerprint: "",   //

		//Transport: &http.Transport{
		//	MaxIdleConnsPerHost:   10,
		//	ResponseHeaderTimeout: time.Second,
		//	TLSClientConfig: &tls.Config{
		//		MinVersion: tls.VersionTLS12,
		//		// ...
		//	},
		//},

		//Transport: &CountingTransport{},
	}
	esClient, err := elasticsearch.NewClient(cfg)
	if err != nil {
		log.Fatalf("new client error: %s", err)
		return nil
	}
	return esClient
}
func Info7(client *elasticsearch.Client) error {
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
	json := read7(resp.Body)
	log.Println(json)
	return err
}

func read7(r io.Reader) string {
	var b bytes.Buffer
	b.ReadFrom(r)
	return b.String()
}
