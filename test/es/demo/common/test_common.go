package common

import (
	"bytes"
	"context"
	"errors"
	"fmt"
	"github.com/elastic/go-elasticsearch/v8"
	"github.com/olivere/elastic/v7"
	bizlog "github.com/zilliztech/milvus-migration/internal/log"
	"go.uber.org/zap"
	"io"
	"log"
	"math/rand"
	"strconv"
	"time"
)

func GetInsertValue(i int, dims int) *InsertValue {
	var bl bool
	if i%2 == 0 {
		bl = true
	}
	vector := make([]float32, 0, dims)
	for j := 0; j < dims; j++ {
		vector = append(vector, rand.Float32())
	}
	return &InsertValue{
		Text1: "text1" + strconv.Itoa(i),
		Keyw1: "keyxx" + strconv.Itoa(i),
		Long1: int64(i),
		Int1:  int32(i),
		Bl2:   bl,
		Doub1: rand.Float64(),
		Dvec:  vector,
	}
}

type InsertValue struct {
	Text1 string    `json:"text1"`
	Keyw1 string    `json:"keyw1"`
	Long1 int64     `json:"long1"`
	Int1  int32     `json:"int1"`
	Bl2   bool      `json:"bl2"`
	Doub1 float64   `json:"doub1"`
	Dvec  []float32 `json:"dvec"`
}

func GetClient8_2() *elasticsearch.Client {

	//cert, _ := os.ReadFile("/Users/zilliz/gitCode/cloud_team/milvus-migration/files/cert_8_2/http_ca.crt")
	//cert, _ := os.ReadFile("/Users/zilliz/gitCode/cloud_team/milvus-migration/files/10_15_9_78/http_ca.crt")
	//cert, _ := os.ReadFile("/Users/zilliz/gitCode/cloud_team/milvus-migration/files/10_15_11_224/http_ca.crt")

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
		//CACert: cert, //2:custom certificate authority
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

func Read(r io.Reader) string {
	var b bytes.Buffer
	b.ReadFrom(r)
	return b.String()
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
	json := Read(resp.Body)
	log.Println(json)
	return err
}

func BulkInsert(min_id int, max_id int, indexName string) {
	// 创建client
	//client, err := elastic.NewClient(elastic.SetURL("localhost:9200"))
	client, err := elastic.NewClient(elastic.SetURL("http://10.15.9.78:9700"), elastic.SetSniff(false))
	//client, err := elastic.NewClient(elastic.SetURL("http://localhost:9200"), elastic.SetSniff(false))
	if err != nil {
		// Handle error
		fmt.Printf("连接失败: %v\n", err)
		panic(err)
	}

	// 执行ES请求需要提供一个上下文对象
	ctx := context.Background()

	w, err := client.BulkProcessor().BulkActions(5000).
		FlushInterval(time.Millisecond).
		Workers(20).Stats(true).After(GetFailed2).Do(ctx)
	if err != nil {
		panic(err)
	}
	w.Start(ctx)
	defer w.Close() //关闭并提交所有队列里的数据，一定要做

	for i := min_id; i <= max_id; i++ {
		item := GetInsertValue(i, 128)
		req := elastic.NewBulkIndexRequest().Index(indexName).Id(strconv.Itoa(i)).Doc(item)
		w.Add(req)
		if i%10000 == 0 {
			fmt.Printf("Current Id : %d, ", i)
		}
	}
}

func GetFailed2(executionId int64, requests []elastic.BulkableRequest, response *elastic.BulkResponse, err error) {
	if response == nil { //可能存在为空的情况
		log.Println("GetNil response return")
		return
	}
	fi := response.Failed()
	if len(fi) != 0 {
		for _, f := range fi {
			log.Printf("DebugFailedEs: index:%s type:%s id:%s version:%d  status:%d result:%s ForceRefresh:%v errorDetail:%v getResult:%v\n", f.Index, f.Type, f.Id, f.Version, f.Status, f.Result, f.ForcedRefresh, f.Error, f.GetResult)
		}
	}
}
