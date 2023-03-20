package log

import (
	"context"
	"github.com/gin-gonic/gin"
	"go.uber.org/zap"
)

type ctxLoggerType struct{}

var _ctxLoggerKey = ctxLoggerType{}

const _reqIDKey = "req"

func WithHTTPCtx(ctx context.Context, reqID, url string) context.Context {
	var logger *zap.Logger
	if ctxLogger, ok := ctx.Value(_ctxLoggerKey).(*zap.Logger); ok {
		logger = ctxLogger
	} else {
		logger = L()
	}

	logger = logger.With(zap.String(_reqIDKey, reqID), zap.String("url", url))
	return context.WithValue(ctx, _ctxLoggerKey, logger)
}

func SS(ctx context.Context) *zap.SugaredLogger {
	return LL(ctx).Sugar()
}

func NewContextWithRequestId(ctx context.Context) context.Context {
	lg := ctx.Value(_ctxLoggerKey).(*zap.Logger)
	return context.WithValue(context.Background(), _ctxLoggerKey, lg)
}

func LL(ctx context.Context) *zap.Logger {
	if ctxLogger, ok := ctx.Value(_ctxLoggerKey).(*zap.Logger); ok {
		return ctxLogger
	}

	return L()
}

// set requestId in header
func LogGinMiddleware() gin.HandlerFunc {
	return func(c *gin.Context) {
		reqID := c.Request.Header.Get(HTTPReqIDKey)
		if reqID == "" {
			reqID = GetReqID(c.Request.Context())
		}
		url := c.Request.URL.String()
		ctx := WithHTTPCtx(c.Request.Context(), reqID, url)
		c.Request = c.Request.WithContext(ctx)
	}
}
