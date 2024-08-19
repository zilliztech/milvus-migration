package starter

import (
	"context"
	"encoding/json"
	"fmt"
	"github.com/zilliztech/milvus-migration/core/cleaner"
	"github.com/zilliztech/milvus-migration/core/common"
	"github.com/zilliztech/milvus-migration/core/config"
	"github.com/zilliztech/milvus-migration/core/dumper"
	"github.com/zilliztech/milvus-migration/core/gstore"
	"github.com/zilliztech/milvus-migration/core/loader"
	"github.com/zilliztech/milvus-migration/internal/log"
	"github.com/zilliztech/milvus-migration/starter/migration"
	"github.com/zilliztech/milvus-migration/starter/param"
	"time"
)

func Dump(ctx context.Context, configFile string, param *param.DumpParam, jobId string) error {
	err := stepStore(jobId)
	if err != nil {
		return err
	}

	insCfg, err := stepConfig(configFile)
	if err != nil {
		return err
	}

	// options:  ./milvus-migration load --col=coll1,coll2
	if param != nil {
		stepFilterCols(insCfg, param.Collections)
	}

	dump := dumper.NewDumperWithConfig(insCfg, jobId)

	log.LL(ctx).Info("[Dumper] begin to do dump!")
	return dump.Run(ctx)
}

// Load function: start load logic
func Load(ctx context.Context, configFile string, param *param.LoadParam, jobId string) error {
	// store
	err := stepStore(jobId)
	if err != nil {
		return err
	}

	// config
	insCfg, err := stepConfig(configFile)
	if err != nil {
		return err
	}

	// param
	if param != nil {
		stepFilterCols(insCfg, param.Collections)
	}

	load, err := loader.NewMilvus2xLoader(insCfg, jobId)
	if err != nil {
		return err
	}

	log.LL(ctx).Info("[Loader] begin to do load!")
	err = load.Run(ctx)
	if err != nil {
		return err
	}

	log.LL(ctx).Info("[Loader] load success! Will go to clean temp files, if clean step fail, just do clean!")

	clean, err := cleaner.NewCleaner(insCfg, jobId)
	err = clean.ClenFiles()
	if err != nil {
		return err
	}

	log.LL(ctx).Info("[Cleaner] clean file success!")
	return nil
}

func Start(ctx context.Context, configFile string, collection string, jobId string) error {

	if collection != "" {
		fmt.Printf("Migration CmdParam Collection: %s Start..", collection)
	}

	start := time.Now()

	err := stepStore(jobId)
	if err != nil {
		return err
	}

	migrCfg, err := stepConfig(configFile)
	if err != nil {
		return err
	}

	if collection != "" {
		replaceCollectionName(migrCfg, collection)
	}

	if migrCfg.DumperWorkCfg.WorkMode == string(common.Elasticsearch) {
		//record: es dump will split many small json file task
		gstore.InitFileTask(jobId)
	}

	//管理进度处理器
	gstore.InitProcessHandler(jobId, migrCfg.DumperWorkCfg.WorkMode)

	starter, err := migration.NewStarter(migrCfg, jobId)
	if err != nil {
		return err
	}
	log.LL(ctx).Info("[Starter] begin to do migration...")
	err = starter.Run(ctx)
	if err != nil {
		return err
	}

	clean, err := cleaner.NewCleaner(migrCfg, jobId)
	err = clean.ClenFiles()
	if err != nil {
		return err
	}
	log.LL(ctx).Info("[Cleaner] clean file success!")

	if collection != "" {
		fmt.Printf("Migration CmdParam Collection: %s Done!", collection)
	}

	fmt.Printf("Migration Success! Job %s cost=[%f]\n", jobId, time.Since(start).Seconds())
	printStartJobMessage(jobId)
	return nil
}

func replaceCollectionName(migrCfg *config.MigrationConfig, collection string) {
	if migrCfg.MetaConfig.Milvus2xMeta != nil {
		migrCfg.MetaConfig.Milvus2xMeta.CollCfgs[0].Collection = collection
		if migrCfg.MetaConfig.Milvus2xMeta.CollCfgs[0].MilvusCfg != nil {
			migrCfg.MetaConfig.Milvus2xMeta.CollCfgs[0].MilvusCfg.Collection = collection
		}
	}
	if migrCfg.MetaConfig.EsMeta != nil {
		migrCfg.MetaConfig.EsMeta.IdxCfgs[0].MilvusCfg.Collection = collection
	}
}

func printStartJobMessage(jobId string) {
	jobInfo, _ := gstore.GetJobInfo(jobId)
	val, _ := json.Marshal(&jobInfo)
	fmt.Printf("Migration JobInfo: %s\n", string(val))

	procInfo := gstore.GetProcessHandler(jobId)
	val, _ = json.Marshal(&procInfo)
	fmt.Printf("Migration ProcessInfo: %s, Process:%d\n", string(val), procInfo.CalcProcess())

	fileTaskInfo := gstore.GetFileTask(jobId)
	val, _ = json.Marshal(&fileTaskInfo)
	fmt.Printf("Migration FileTaskInfo:  %s\n", string(val))
}
