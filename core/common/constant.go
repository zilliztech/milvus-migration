package common

import "time"

const EMPTY = ""

type DumpMode string

// dump mode type
const (
	Faiss         DumpMode = "faiss"
	Milvus1x      DumpMode = "milvus1x"
	Elasticsearch DumpMode = "elasticsearch"
	Milvus2x      DumpMode = "milvus2x"
)

type SourceMode string

// source mode type
const (
	S_Local  SourceMode = "local"
	S_Remote SourceMode = "remote"
)

type TargetMode string

// target mode type
const (
	T_LOCAL  TargetMode = "local"
	T_REMOTE TargetMode = "remote"
)

// ES connection auth type
//const (
//	Non         = "non"
//User        = "user"
//Cert        = "cert"
//FingerPrint = "fingerprint"

//)

// reader type
const (
	MILVUS2X   = "milvus2x"
	ES         = "es"
	RV         = "rv"
	UID        = "uid"
	FAISS_ID   = "faiss-id"
	FAISS_DATA = "faiss-data"
)

// current Milvus support max shard num: https://milvus.io/docs/v2.3.x/create_collection.md#Create-a-collection-with-the-schema
var MAX_SHARD_NUM = 16
var DEF_SHARD_NUM = 2

var DEBUG = false
var DUMP_SUB_TASK_NUM = 3
var LOAD_CHECK_BULK_STATE_INTERVAL = time.Second * 10 //second
var LOAD_CHECK_BACKLOG_INTERVAL = time.Second * 10    //second
// const SUB_FILE_SIZE = 1024 * 1024 * 512 //512MB
const SUB_FILE_SIZE = 1024 * 1024 * 300
