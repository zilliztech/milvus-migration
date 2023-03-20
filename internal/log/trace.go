package log

import (
	"context"
	"github.com/gin-gonic/gin"
	"github.com/zilliztech/milvus-migration/core/util"
)

const HTTPReqIDKey string = "RequestId"

type ctxReqIDType struct{}

var _ctxReqIDKey = ctxReqIDType{}

func GetReqID(ctx context.Context) string {
	return ctx.Value(_ctxReqIDKey).(string)
}

func ExtractReqID() string {
	reqID := util.GenerateUUID("req-")
	return reqID
}

func TraceWithReqID(ctx context.Context, reqID string) context.Context {
	return context.WithValue(ctx, _ctxReqIDKey, reqID)
}

func TraceGinMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		reqID := c.Request.Header.Get(HTTPReqIDKey)
		if reqID == "" {
			reqID = ExtractReqID()
		}

		ctx := TraceWithReqID(c.Request.Context(), reqID)
		c.Request = c.Request.WithContext(ctx)
	}
}
