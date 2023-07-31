package server

import (
	"errors"
	"fmt"
	"github.com/gin-gonic/gin"
	"github.com/zilliztech/milvus-migration/core/gstore"
	"github.com/zilliztech/milvus-migration/core/util"
	"github.com/zilliztech/milvus-migration/internal/log"
	"github.com/zilliztech/milvus-migration/starter"
	"github.com/zilliztech/milvus-migration/starter/param"
)

// @Summary check healthy
// @Description check server healthy
// @Tags Migration
// @Param RequestId header string false "RequestId"
// @Produce json
// @Success 200 {object} string
// @Router /healthy [get]
func handleHealthy(c *gin.Context) (interface{}, error) {
	log.LL(c.Request.Context()).Info("hello world")
	return "ok", nil
}

// @Summary migration dump
// @Description migration dump files
// @Tags Migration
// @Param RequestId header string false "RequestId"
// @Param object body param.DumpParam true "param"
// @Produce json
// @Success 200 {object} string
// @Router /dump [post]
func handleDump(c *gin.Context) (interface{}, error) {
	var req param.DumpParam
	err := c.BindJSON(&req)
	if err != nil {
		return nil, err
	}

	jobId := util.GenerateUUID("dump_")
	if req.Async {
		go starter.Dump(log.NewContextWithRequestId(c.Request.Context()), "", &req, jobId)
		return param.NewJobResponse(jobId), nil
	}

	return param.NewJobResponse(jobId), starter.Dump(c.Request.Context(), "", &req, jobId)
}

// @Summary migration load
// @Description migration load files
// @Tags Migration
// @Param RequestId header string false "RequestId"
// @Param object body param.LoadParam true "param"
// @Produce json
// @Success 200 {object} string
// @Router /load [post]
func handleLoad(c *gin.Context) (interface{}, error) {
	var req param.LoadParam
	err := c.BindJSON(&req)
	if err != nil {
		return nil, err
	}

	jobId := util.GenerateUUID("load_")
	if req.Async {
		go starter.Load(log.NewContextWithRequestId(c.Request.Context()), "", &req, jobId)
		return param.NewJobResponse(jobId), nil
	}

	return param.NewJobResponse(jobId), starter.Load(c.Request.Context(), "", &req, jobId)
}

// @Summary get job info
// @Description get job info
// @Tags Migration
// @Param RequestId header string false "RequestId"
// @Param jobId query string true "jobId"
// @Produce json
// @Success 200 {object} string
// @Router /get_job [get]
func handleGetJob(c *gin.Context) (interface{}, error) {
	jobId := c.Query("jobId")
	if jobId == "" {
		return nil, errors.New("jobid is empty")
	}
	info, err := gstore.GetJobInfo(jobId)
	if err != nil {
		return nil, err
	}
	ph := gstore.GetProcessHandler(jobId)
	if ph != nil {
		info.JobProcess = ph.CalcProcess()
	} else {
		info.CalculateJobProcess()
	}
	return info, nil
}

// @Summary migration start
// @Description migration start
// @Tags Migration
// @Param RequestId header string false "RequestId"
// @Param object body param.StartParam true "param"
// @Produce json
// @Success 200 {object} string
// @Router /start [post]
func handleStart(c *gin.Context) (interface{}, error) {
	var req param.StartParam
	err := c.BindJSON(&req)
	if err != nil {
		return nil, err
	}
	jobId := util.GenerateUUID("start_")

	defer func() {
		if _any := recover(); _any != nil {
			handlePanic(_any, jobId)
			return
		}
	}()

	if req.Async {
		go starter.Start(log.NewContextWithRequestId(c.Request.Context()), "", jobId)
		return param.NewJobResponse(jobId), nil
	}

	return param.NewJobResponse(jobId), starter.Start(c.Request.Context(), "", jobId)
}

func handlePanic(_any any, jobId string) {
	var errMsg string
	err, ok := _any.(error)
	if ok {
		errMsg = err.Error()
	} else {
		errMsg, _ = _any.(string)
	}
	if err == nil {
		err = errors.New(errMsg)
	}
	fmt.Printf("Handle invoke Migration panic error! Job: %s , err: %s\n", jobId, errMsg)
	gstore.RecordJobError(jobId, err)
}
