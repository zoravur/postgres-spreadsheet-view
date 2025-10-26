package api

import (
	// "log"
	"context"
	"net/http"
	"time"

	"go.uber.org/zap"

	"github.com/google/uuid"
)

func LoggingMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		start := time.Now()
		ww := &statusWriter{ResponseWriter: w, status: http.StatusOK}

		traceID := r.Header.Get("X-Request-ID")
		if traceID == "" {
			traceID = uuid.NewString()
		}

		logger := zap.L().With(
			zap.String("trace_id", traceID),
			zap.String("method", r.Method),
			zap.String("path", r.URL.Path),
		)

		ctx := context.WithValue(r.Context(), "logger", logger)
		r = r.WithContext(ctx)

		next.ServeHTTP(ww, r)

		duration := time.Since(start)
		logger.Info("HTTP request complete",
			zap.Int("status", ww.status),
			zap.Duration("duration_ms", duration),
		)
	})
}

// statusWriter captures the HTTP status for logging.
type statusWriter struct {
	http.ResponseWriter
	status int
}

func (w *statusWriter) WriteHeader(code int) {
	w.status = code
	w.ResponseWriter.WriteHeader(code)
}
