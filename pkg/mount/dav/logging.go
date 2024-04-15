package dav

import (
	"log/slog"
	"net/http"
	"time"
)

var _ http.ResponseWriter = &loggingWriter{}

type loggingWriter struct {
	writer http.ResponseWriter

	writeErr   error
	n          int
	statusCode int
}

func (w *loggingWriter) Header() http.Header {
	return w.writer.Header()
}

func (w *loggingWriter) Write(bytes []byte) (int, error) {
	n, err := w.writer.Write(bytes)
	w.n += n
	if err != nil {
		w.writeErr = err
	}
	return n, err
}

func (w *loggingWriter) WriteHeader(statusCode int) {
	w.writer.WriteHeader(statusCode)
	w.statusCode = statusCode
}

var _ http.Handler = &loggingHandler{}

type loggingHandler struct {
	logger *slog.Logger
	next   http.Handler
}

func (h *loggingHandler) ServeHTTP(writer http.ResponseWriter, request *http.Request) {
	start := time.Now()
	w := &loggingWriter{
		writer: writer,
	}
	h.next.ServeHTTP(w, request)
	h.logger.DebugContext(request.Context(), "HTTP request done",
		"method", request.Method,
		"url", request.URL.String(),
		"response_bytes", w.n,
		"response_status_code", w.statusCode,
		"response_error", w.writeErr,
		"took_us", time.Since(start).Microseconds())
}
