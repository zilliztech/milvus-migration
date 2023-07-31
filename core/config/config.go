package config

import (
	"github.com/spf13/viper"
	"github.com/zilliztech/milvus-migration/core/common"
	"github.com/zilliztech/milvus-migration/core/type/estype"
	"github.com/zilliztech/milvus-migration/internal/log"
	"go.uber.org/zap"
	"hash/fnv"
	"reflect"
	"sync/atomic"
)

// MigrationConfig : struct for store the resolved  migration.yaml file config
type MigrationConfig struct {

	// meta config
	MetaConfig *MetaConfig

	// source
	SourceMode      string // local, remote
	SourceTablesDir string
	SourceRemote    *RemoteConfig
	SourceFaissFile string
	SourceESConfig  *ESConfig

	// target
	TargetMode      string
	TargetOutputDir string
	Milvus2xCfg     *Milvus2xConfig
	TargetRemote    *RemoteConfig

	// dumper
	DumperWorkLimit int
	DumperWorkCfg   *DumperWorkConfig

	// loader
	LoaderWorkLimit int
	LoaderWorkCfg   *LoaderWorkConfig

	// controller params
	FilterCols []string

	// runtime store config
	RuntimeStore *RuntimeStore
}

type ESConfig struct {
	//Security    string   // cert  #non, cert, password, fingerPrint,

	CloudId string
	ApiKey  string

	Urls        []string //http://localhost:9200,http://localhost:9201
	Username    string
	Password    string
	Cert        string // /local/certificates/ca/ca.crt
	FingerPrint string //: "xxxxxxx...."

	ServiceToken string

	Version   string //internal param
	hashCache atomic.Uint32
}

type Milvus1xConfig struct {
	Address string
	Port    string
}

type Milvus2xConfig struct {
	Endpoint string
	UserName string
	Password string
}

type CollectionConfig struct {
	CollectionName string
	ShardsNum      int
	MetricType     string
	Dim            int
}

type MetaConfig struct {
	MetaMode string

	// local mock
	LocalMockFile string
	// local sqlite
	LocalSqliteFile string
	// local mysql
	LocalMysqlURL string

	// remote meta
	RemoteMetaFile string

	EsMeta *estype.MetaJSON
}

type DumperWorkConfig struct {
	//common.Faiss, common.Milvus1x, common.Elasticsearch:
	WorkMode         string
	Limit            int
	ReaderBufferSize int
	WriterBufferSize int

	// inner
	InnerReadCfg  *ReadConfig
	InnerWriteCfg *WriteConfig
}

type LoaderWorkConfig struct {
	WorkMode     string
	CreateColCfg CollectionConfig
}

type ReadConfig struct {
	ReadMode     string //local, remote
	ReaderType   string //common.ES, RV, UID, FAISS_ID, FAISS_DATA
	BufSize      int    // 1024 * 1024
	Dim          int
	RemoteConfig *RemoteConfig
	//read data from file param
	FileParam  *common.FileParam
	DeleteFile *common.FileParam

	//read data from es connection config
	//ESConfig *ESConfig
	//ESIdxCfg *estype.IdxCfg
	//ESSource *source.ESSource
}

type WriteConfig struct {
	WriteMode    string
	BufSize      int
	RemoteConfig *RemoteConfig
	FileParam    *common.FileParam
}

type RemoteConfig struct {
	Endpoint          string
	BucketName        string
	AccessKeyID       string
	SecretAccessKeyID string
	UseSSL            bool
	CheckBucket       bool
	UseIAM            bool
	Cloud             string
	IamEndpoint       string
	Region            string

	hashCache atomic.Uint32
}

func (r *ESConfig) Hash() uint32 {

	cache := r.hashCache.Load()
	if cache != 0 {
		return cache
	}
	hashCode := getHashCode(r)
	r.hashCache.Store(hashCode)
	return hashCode
}

func (r *RemoteConfig) Hash() uint32 {
	cache := r.hashCache.Load()
	if r.hashCache.Load() != 0 {
		return cache
	}
	hashCode := getHashCode(r)
	r.hashCache.Store(hashCode)
	return hashCode
}

func getHashCode(r interface{}) uint32 {
	h := fnv.New32()
	val := reflect.ValueOf(r).Elem()
	for i := 0; i < val.NumField(); i++ {
		var b []byte
		field := val.Field(i)
		switch field.Type().Kind() {
		case reflect.String:
			b = []byte(field.String())
		case reflect.Bool:
			if field.Bool() {
				b = []byte("true")
			} else {
				b = []byte("false")
			}
		}
		if _, err := h.Write(b); err != nil {
			panic(err)
		}
	}

	hashCode := h.Sum32()
	return hashCode
}

type RuntimeStore struct {
	CollectionDim int
}

func InitConfigFile(configFile string) (*viper.Viper, error) {
	if configFile == "" {
		log.Info("input configFile is empty, will read from dir")
		v := viper.New()
		v.AddConfigPath(".")
		v.AddConfigPath("./configs")
		v.SetConfigType("yaml")
		v.SetConfigName("migration.yaml")

		err := v.ReadInConfig()
		return v, err
	} else {
		log.Info("begin to read config", zap.String("configFile", configFile))
		v := viper.New()
		v.SetConfigType("yaml")
		v.SetConfigFile(configFile)

		err := v.ReadInConfig()
		return v, err
	}
}
