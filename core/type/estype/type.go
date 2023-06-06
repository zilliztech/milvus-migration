package estype

/*
	{
	  "version" : "7.17.7",
	  "indexs": [
	    {
	      "index": "index1",
	      "id" : "_id",
	      "dense_vector" : "my_vector",
	      "dim": 256,
	      "fields" : ["title", "name"],
	      "metric": 1,
	      "rows": 10000
	    },{
	      "index": "index2",
	      "id" : "_id",
	      "dense_vector" : "my_vector",
	      "dim": 256,
	      "fields" : ["title", "name"],
	      "metric": 1,
	      "rows": 10000
	    }
	  ]
	}
*/
type MetaJSON struct {
	IdxCfgs []IdxCfg `json:"indexs"`
	Version string   `json:"version"`
}

type IdxCfg struct {
	Index        string   `json:"index"`
	Id           string   `json:"id"`
	DenseVector  string   `json:"dense_vector"`
	Dim          int      `json:"dim"`
	Metric       int      `json:"metric"`
	Rows         int      `json:"rows"`
	FilterFields []string `json:"filterFields"`
}
