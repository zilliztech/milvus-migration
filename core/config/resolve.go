package config

import (
	"errors"
	"fmt"
	"github.com/spf13/viper"
	"github.com/zilliztech/milvus-migration/core/common"
	"path/filepath"
	"strings"
)

func ResolveInsConfig(v *viper.Viper) (*MigrationConfig, error) {

	// first get workMode
	dumperWorkMode, err := resolveWorkMode(v)
	if err != nil {
		return nil, err
	}
	//202405: milvus2x use iterator/batchInsert not need source/target mode param
	dumpMode := common.DumpMode(dumperWorkMode)
	if dumpMode == common.Milvus2x {
		return assertBatchInsertMode(v, dumpMode)
	} else {
		return assertBulkInsertMode(v, dumpMode)
	}
}

func assertBulkInsertMode(v *viper.Viper, dumpMode common.DumpMode) (*MigrationConfig, error) {

	dumpWrkLimit := v.GetInt("dumper.worker.limit")
	if dumpWrkLimit <= 0 {
		dumpWrkLimit = 2
	}

	loadWrkLimit := v.GetInt("loader.worker.limit")
	if loadWrkLimit <= 0 {
		loadWrkLimit = 2
	}

	sourceMode, err := assertSourceMode(v, dumpMode)
	if err != nil {
		return nil, err
	}

	targetMode, err := assertTargetMode(v)
	if err != nil {
		return nil, err
	}

	targetOutputDir, err := getOutputDirByTargetMode(targetMode, v)
	if err != nil {
		return nil, err
	}

	dumpWorkCfg, err := resolveDumpWorkConfig(v, dumpWrkLimit)
	if err != nil {
		return nil, err
	}

	loadWorkCfg, err := resolveLoadWorkConfig(v)
	if err != nil {
		return nil, err
	}

	cfg := MigrationConfig{
		SourceMode:   sourceMode,
		SourceRemote: resolveSourceRemoteConfig(v),

		TargetMode:        targetMode,
		TargetOutputDir:   targetOutputDir,
		TargetMilvus2xCfg: resolveTargetMilvus2xConfig(v),
		TargetRemote:      resolveTargetRemoteConfig(v),

		// dumper
		DumperWorkLimit: dumpWrkLimit,
		DumperWorkCfg:   dumpWorkCfg,

		// loader
		LoaderWorkLimit: loadWrkLimit,
		LoaderWorkCfg:   loadWorkCfg,
	}

	switch dumpMode {
	case common.Faiss:
		sourceFaissFile, err := getFaissFileBySourceMode(sourceMode, v)
		if err != nil {
			return nil, err
		}
		cfg.SourceFaissFile = sourceFaissFile
	case common.Milvus1x:
		sourceTablesDir, err := getTableDirBySourceMode(sourceMode, v)
		if err != nil {
			return nil, err
		}
		metaConfig, err := resolveMetaConfig(v, common.Milvus1x)
		if err != nil {
			return nil, err
		}
		cfg.MetaConfig = metaConfig
		cfg.SourceTablesDir = sourceTablesDir
	case common.Elasticsearch:
		cfg.SourceESConfig, err = getSourceESConfig(v)
		if err != nil {
			return nil, err
		}
		cfg.MetaConfig, err = resolveMetaConfig(v, common.Elasticsearch)
		if err != nil {
			return nil, err
		}
	}

	return &cfg, nil
}

func assertTargetMode(v *viper.Viper) (string, error) {
	mode := v.GetString("target.mode")
	switch common.TargetMode(mode) {
	case common.T_REMOTE, common.T_LOCAL:
		return mode, nil
	default:
		return "", fmt.Errorf("not support [target.mode], %s", mode)
	}
}

func getTableDirBySourceMode(sourceMode string, v *viper.Viper) (string, error) {
	var tableDir string
	switch sourceMode {
	case "local":
		tableDir = v.GetString("source.local.tablesDir")
	case "remote":
		tableDir = v.GetString("source.remote.tablesDir")
	default:
		return "", fmt.Errorf("not support [source.mode], %s", sourceMode)
	}
	return tableDir, nil
}

func getFaissFileBySourceMode(sourceMode string, v *viper.Viper) (string, error) {
	var faissFile string
	switch sourceMode {
	case "local":
		faissFile = v.GetString("source.local.faissFile")
		if faissFile == "" {
			return "", fmt.Errorf("[source.local.faissFile] can not empty")
		}
	case "remote":
		faissFile = v.GetString("source.remote.faissFile")
		if faissFile == "" {
			return "", fmt.Errorf("[source.remote.faissFile] can not empty")
		}
	default:
		return "", fmt.Errorf("not support [source.mode], %s", sourceMode)
	}

	return faissFile, nil
}

func getOutputDirByTargetMode(targetMode string, v *viper.Viper) (string, error) {
	var outputDir string

	switch targetMode {
	case "local":
		outputDir = v.GetString("target.local.outputDir")
	case "remote":
		outputDir = getOutputDirByRemote(v)
	default:
		return "", fmt.Errorf("not support [target.mode], %s", targetMode)
	}

	// add magic suffix
	return filepath.Join(outputDir, "/migration"), nil
}

func getOutputDirByRemote(v *viper.Viper) string {
	outputDir := v.GetString("target.remote.outputDir")
	return strings.TrimPrefix(outputDir, "/")
}

func assertSourceMode(v *viper.Viper, dumpMode common.DumpMode) (string, error) {

	//es not need source mode param
	if dumpMode == common.Elasticsearch {
		return common.EMPTY, nil
	}

	mode := v.GetString("source.mode")
	switch common.SourceMode(mode) {
	case common.S_Local, common.S_Remote:
		return mode, nil
	default:
		return "", fmt.Errorf("not support [source.mode], %s", mode)
	}
}

func resolveSourceRemoteConfig(v *viper.Viper) *RemoteConfig {
	return resolveRemoteConfig("source", v)
}

func resolveTargetRemoteConfig(v *viper.Viper) *RemoteConfig {
	return resolveRemoteConfig("target", v)
}

func resolveRemoteConfig(prefix string, v *viper.Viper) *RemoteConfig {
	return &RemoteConfig{
		Cloud:             v.GetString(prefix + ".remote.cloud"),
		Endpoint:          v.GetString(prefix + ".remote.endpoint"),
		BucketName:        v.GetString(prefix + ".remote.bucket"),
		Region:            v.GetString(prefix + ".remote.region"),
		AccessKeyID:       v.GetString(prefix + ".remote.ak"),
		SecretAccessKeyID: v.GetString(prefix + ".remote.sk"),
		UseSSL:            v.GetBool(prefix + ".remote.useSSL"),
		UseIAM:            v.GetBool(prefix + ".remote.useIAM"),
		CheckBucket:       v.GetBool(prefix + ".remote.checkBucket"),
	}
}

func resolveTargetMilvus2xConfig(v *viper.Viper) *Milvus2xConfig {
	return &Milvus2xConfig{
		Endpoint:           v.GetString("target.milvus2x.endpoint"),
		UserName:           v.GetString("target.milvus2x.username"),
		Password:           v.GetString("target.milvus2x.password"),
		GrpcMaxRecvMsgSize: v.GetInt("target.milvus2x.grpc.maxCallRecvMsgSize"),
		GrpcMaxSendMsgSize: v.GetInt("target.milvus2x.grpc.maxCallSendMsgSize"),
	}
}

func resolveDumpWorkConfig(v *viper.Viper, limit int) (*DumperWorkConfig, error) {
	workMode, err := resolveWorkMode(v)
	if err != nil {
		return nil, err
	}
	var writerBufferSize = v.GetInt("dumper.worker.writer.bufferSize")
	if writerBufferSize <= 0 {
		writerBufferSize = 1048576
	}
	return &DumperWorkConfig{
		WorkMode:         workMode,
		Limit:            limit,
		ReaderBufferSize: v.GetInt("dumper.worker.reader.bufferSize"),
		WriterBufferSize: writerBufferSize,
	}, nil
}

func resolveLoadWorkConfig(v *viper.Viper) (*LoaderWorkConfig, error) {
	workMode, err := resolveWorkMode(v)
	if err != nil {
		return nil, err
	}

	createCol, err := resolveCreatColMode(workMode, v)
	if err != nil {
		return nil, err
	}

	return &LoaderWorkConfig{
		WorkMode:     workMode,
		CreateColCfg: *createCol,
	}, nil
}

func resolveCreatColMode(workMode string, v *viper.Viper) (*CollectionConfig, error) {
	colCfg := &CollectionConfig{
		CollectionName: v.GetString("target.create.collection.name"),
		ShardsNum:      v.GetInt("target.create.collection.shardsNum"),
		Dim:            v.GetInt("target.create.collection.dim"),
		MetricType:     v.GetString("target.create.collection.metricType"),
	}

	// fast return
	mode := common.DumpMode(workMode)
	if mode == common.Milvus1x || mode == common.Elasticsearch {
		return colCfg, nil
	}

	if colCfg.CollectionName == "" {
		return nil, fmt.Errorf("[target.create.collection.name] cat not empty")
	}

	if colCfg.Dim == 0 {
		return nil, fmt.Errorf("[target.create.collection.dim] cat not be 0")
	}

	if colCfg.MetricType == "" {
		return nil, fmt.Errorf("[target.create.collection.metricType] cat not empty, should by L2 or IP")
	}

	if colCfg.MetricType != "L2" && colCfg.MetricType != "IP" {
		return nil, fmt.Errorf("not support [target.create.collection.metricType] %s", colCfg.MetricType)
	}

	return colCfg, nil
}

func resolveWorkMode(v *viper.Viper) (string, error) {
	workMode := v.GetString("dumper.worker.workMode")

	switch common.DumpMode(workMode) {
	case common.Faiss, common.Milvus1x, common.Elasticsearch, common.Milvus2x:
		break
	default:
		return "", errors.New("[dumper.worker.workMode] not support " + workMode)
	}

	return workMode, nil
}

func assertBatchInsertMode(v *viper.Viper, dumpMode common.DumpMode) (*MigrationConfig, error) {
	dumpWorkCfg, err := resolveMilvus2xDumpWorkConfig(v, dumpMode)
	if err != nil {
		return nil, err
	}
	loadWorkCfg := &LoaderWorkConfig{
		WorkMode: dumpWorkCfg.WorkMode,
	}
	metaCfg, err := resolveMetaConfig(v, common.Milvus2x)
	if err != nil {
		return nil, err
	}
	cfg := MigrationConfig{
		SourceMilvus2xConfig: resolveSourceMilvus2xConfig(v),
		TargetMilvus2xCfg:    resolveTargetMilvus2xConfig(v),
		// dumper
		DumperWorkCfg:   dumpWorkCfg,
		DumperWorkLimit: dumpWorkCfg.Limit,
		//loader
		LoaderWorkCfg:   loadWorkCfg,
		LoaderWorkLimit: dumpWorkCfg.Limit,
		MetaConfig:      metaCfg,
	}
	return &cfg, nil
}

func resolveMilvus2xDumpWorkConfig(v *viper.Viper, workMode common.DumpMode) (*DumperWorkConfig, error) {
	dumpWrkLimit := v.GetInt("dumper.worker.limit")
	if dumpWrkLimit <= 0 {
		dumpWrkLimit = 1
	}
	var writerBufferSize = v.GetInt("dumper.worker.writer.bufferSize")
	if writerBufferSize <= 0 {
		writerBufferSize = 1000
	}
	var readerBufferSize = v.GetInt("dumper.worker.reader.bufferSize")
	if readerBufferSize <= 0 {
		writerBufferSize = 1000
	}
	return &DumperWorkConfig{
		WorkMode:         string(workMode),
		ReaderBufferSize: readerBufferSize,
		WriterBufferSize: writerBufferSize,
		Limit:            dumpWrkLimit,
	}, nil
}

func resolveSourceMilvus2xConfig(v *viper.Viper) *Milvus2xConfig {
	return &Milvus2xConfig{
		Endpoint:           v.GetString("source.milvus2x.endpoint"),
		UserName:           v.GetString("source.milvus2x.username"),
		Password:           v.GetString("source.milvus2x.password"),
		GrpcMaxRecvMsgSize: v.GetInt("source.milvus2x.grpc.maxCallRecvMsgSize"),
		GrpcMaxSendMsgSize: v.GetInt("source.milvus2x.grpc.maxCallSendMsgSize"),
	}
}
