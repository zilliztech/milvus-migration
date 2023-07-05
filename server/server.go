package server

import (
	"github.com/gin-gonic/gin"
	swaggerFiles "github.com/swaggo/files"
	ginSwagger "github.com/swaggo/gin-swagger"
	"github.com/zilliztech/milvus-migration/core/gstore"
	_ "github.com/zilliztech/milvus-migration/docs"
	"github.com/zilliztech/milvus-migration/internal/log"
	"net/http"
)

const (
	HEALTHY_API    = "/healthy"
	START_DATA_API = "/start"
	DUMP_DATA_API  = "/dump"
	LOAD_DATA_API  = "/load"
	GET_JOB_API    = "/get_job"

	API_V1_PREFIX = "/api/v1"
	DOCS_API      = "/docs/*any"
)

type Server struct {
	engine *gin.Engine
	config *ServerConfig
}

func NewServer(opts ...ServerOption) (*Server, error) {
	c := newDefaultServerConfig()
	for _, opt := range opts {
		opt(c)
	}

	return &Server{
		config: c,
	}, nil
}

func (s *Server) Init() {
	// init global store
	gstore.Init()

	// register http
	s.registerHTTPServer()
}

func (s *Server) Start() {
	log.Info("Start migration server backend")
	s.engine.Run(s.config.port)
}

// register the http server, panic when failed
func (s *Server) registerHTTPServer() {

	gin.SetMode(gin.ReleaseMode)
	r := gin.New()
	r.Use(log.TraceGinMiddleware())
	r.Use(log.LogGinMiddleware())

	apiV1 := r.Group(API_V1_PREFIX)

	// hello
	apiV1.GET(HEALTHY_API, wrap(handleHealthy))
	apiV1.POST(START_DATA_API, wrap(handleStart))
	apiV1.POST(DUMP_DATA_API, wrap(handleDump))
	apiV1.POST(LOAD_DATA_API, wrap(handleLoad))
	apiV1.GET(GET_JOB_API, wrap(handleGetJob))
	r.GET(DOCS_API, ginSwagger.WrapHandler(swaggerFiles.Handler))

	s.engine = r
}

type HandlerFunc func(c *gin.Context) (interface{}, error)

func wrap(handler HandlerFunc) func(c *gin.Context) {
	return func(c *gin.Context) {
		res, err := handler(c)
		if err != nil {
			c.JSON(http.StatusOK, gin.H{
				"code":      500,
				"msg":       err.Error(),
				"data":      res,
				"requestId": log.GetReqID(c.Request.Context()),
			})
			return
		}

		c.JSON(http.StatusOK, gin.H{
			"code":      0,
			"msg":       "success",
			"data":      res,
			"requestId": log.GetReqID(c.Request.Context()),
		})
	}
}
