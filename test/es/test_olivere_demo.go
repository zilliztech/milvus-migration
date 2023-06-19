package main

import (
	"context"
	"fmt"
	"github.com/olivere/elastic/v7"
	uuid "github.com/satori/go.uuid"
	"log"
	"strconv"
	"time"
)

const mapping = `
{
"settings":{
      "number_of_shards":5,
      "number_of_replicas":2
   },
 "mappings": {
   "properties": {
      "actionName": {
         "type": "keyword"
      },
      "bucketID": {
         "type": "keyword"
      },
      "clientIP": {
         "type": "keyword"
      },
      "contentLength": {
         "type": "long"
      },
      "injectTime": {
         "type": "long"
      },
      "method": {
         "type": "keyword"
      },
      "nodeID": {
         "type": "keyword"
      },
      "objectHash": {
         "type": "keyword"
      },
      "objectKey": {
         "type": "text"
      },
      "objectName": {
         "type": "text"
      },
      "objectPaths": {
         "type": "text"
      },
      "objectUUID": {
         "type": "keyword"
      },
      "objectVersionID": {
         "type": "keyword"
      },
      "offset": {
         "type": "long"
      },
      "regionID": {
         "type": "keyword"
      },
      "requestTime": {
         "type": "long"
      },
      "responseTime": {
         "type": "long"
      },
      "statusCode": {
         "type": "keyword"
      },
      "storageID": {
         "type": "keyword"
      },
      "tenantID": {
         "type": "keyword"
      },
      "transactionID": {
         "type": "keyword"
      },
      "username": {
         "type": "keyword"
      }
   }
 }
}`

type Item struct {
	ActionName      string `json:"actionName"`
	BucketID        string `json:"bucketID"`
	ClientIP        string `json:"clientIP"`
	ContentLength   int64  `json:"contentLength"`
	InjectTime      int64  `json:"injectTime"`
	Method          string `json:"method"`
	NodeID          string `json:"nodeID"`
	ObjectHash      string `json:"objectHash"`
	ObjectKey       string `json:"objectKey"`
	ObjectName      string `json:"objectName"`
	ObjectUUID      string `json:"objectUUID"`
	ObjectVersionID string `json:"objectVersionID"`
	Offset          int64  `json:"offset"`
	RegionID        string `json:"regionID"`
	RequestTime     int64  `json:"requestTime"`
	ResponseTime    int64  `json:"responseTime"`
	StatusCode      string `json:"statusCode"`
	StorageID       string `json:"storageID"`
	TenantID        string `json:"tenantID"`
	TransactionID   string `json:"transactionID"`
	Username        string `json:"username"`
}

func main() {
	indexName := "test"
	// 创建client
	client, err := elastic.NewClient(elastic.SetURL("http://10.0.11.101:9200", "http://10.0.11.102:9200"))
	if err != nil {
		// Handle error
		fmt.Printf("连接失败: %v\n", err)
		panic(err)
	}

	// 执行ES请求需要提供一个上下文对象
	ctx := context.Background()

	// 首先检测下weibo索引是否存在
	exists, err := client.IndexExists(indexName).Do(ctx)
	if err != nil {
		// Handle error
		panic(err)
	}
	if !exists {
		_, err := client.CreateIndex(indexName).BodyString(mapping).Do(ctx)
		if err != nil {
			// Handle error
			panic(err)
		}
	}
	time.Sleep(time.Second * 10)

	w, err := client.BulkProcessor().BulkActions(5000).
		FlushInterval(time.Millisecond).
		Workers(20).Stats(true).After(GetFailed).Do(ctx)
	if err != nil {
		panic(err)
	}
	w.Start(ctx)
	defer w.Close() //关闭并提交所有队列里的数据，一定要做

	item := Item{
		ActionName:      "",
		BucketID:        "1666234914826",
		ClientIP:        "10.0.6.254",
		ContentLength:   19,
		InjectTime:      1666247542004,
		Method:          "PUT",
		NodeID:          "N02",
		ObjectHash:      "1726a395efec7614463737d669a4f922",
		ObjectKey:       "WND4nF2BypcVgFWTjwTO",
		ObjectName:      "WND4nF2BypcVgFWTjwTO",
		ObjectUUID:      "81be6526-e2d5-48f2-86b4-6882c3a8e3f8",
		ObjectVersionID: "",
		RegionID:        "RN1",
		RequestTime:     1666247541936,
		ResponseTime:    1666247542006,
		StatusCode:      "200",
		StorageID:       "VSP2",
		TenantID:        "01",
		TransactionID:   "4367967956479508501000000",
		Username:        "admin",
	}
	id := 1
	for i := 0; i < 4000000; i++ {
		for j := 0; j < 100; j++ {
			id++
			item.InjectTime = int64(1666247542004 + id)
			item.RequestTime = int64(1666247542004 + id)
			item.ResponseTime = int64(1666247542004 + id)
			v4 := uuid.NewV4()
			item.ObjectHash = v4.String()
			idstr := strconv.Itoa(i)
			req := elastic.NewBulkIndexRequest().Index(indexName).Id(idstr).Doc(item)
			w.Add(req)
			//if id > 99999 {
			// id = 0
			// st := w.Stats() //获取数据写入情况
			// log.Printf("BulkMonitor state Succeeded:%d Failed:%d Created:%d Updated:%d Deleted:%d Flushed:%d Committed:%d Indexed:%d\n", st.Succeeded, st.Failed, st.Created, st.Updated, st.Deleted, st.Flushed, st.Committed, st.Indexed)
			// for _, s := range st.Workers {
			//    log.Println("BulkMonitor Queue:", s.Queued)
			// }
			//}
		}
		fmt.Println((i + 1) * 100)
	}
	//fmt.Println(err, do.Id)
	//get1, err := client.Get().
	// Index(indexName). // 指定索引名
	// Id(do.Id). // 设置文档id
	// Do(ctx) // 执行请求
	//if err != nil {
	// panic(err)
	//}
	//if get1.Found {
	// fmt.Printf("文档id=%s 版本号=%d 索引名=%s\n", get1.Id, get1.Version, get1.Index)
	//}
	//
	//msg2 := Item{}
	//提取文档内容，原始类型是json数据
	//data, _ := get1.Source.MarshalJSON()
	//将json转成struct结果
	//json.Unmarshal(data, &msg2)
	//打印结果
	//fmt.Println(msg2.InjectTime)
}

// GetFailed 是发生数据写入失败时获取详情的回调函数
func GetFailed(executionId int64, requests []elastic.BulkableRequest, response *elastic.BulkResponse, err error) {
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
