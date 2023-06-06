package common

const EMPTY = ""

type DumpMode string

// dump mode type
const (
	Faiss         DumpMode = "faiss"
	Milvus1x      DumpMode = "milvus1x"
	Elasticsearch DumpMode = "elasticsearch"
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
const (
	Non         = "non"
	Cert        = "cert"
	User        = "user"
	FingerPrint = "fingerprint"
)

// reader type
const (
	ES         = "es"
	RV         = "rv"
	UID        = "uid"
	FAISS_ID   = "faiss-id"
	FAISS_DATA = "faiss-data"
)