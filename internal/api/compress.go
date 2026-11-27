package api

import (
	"bytes"
	"compress/gzip"
	"fmt"
	"io"
	"log/slog"
	"mime"
	"net/http"
	"strconv"
	"strings"

	"github.com/klauspost/compress/zstd"
)

const minimumCompressionSize = 1024

func acceptedEncoding(header string) string {
	gzipQ, gzipOK := encodingQuality(header, "gzip")
	zstdQ, zstdOK := encodingQuality(header, "zstd")
	if zstdOK && zstdQ > 0 && (!gzipOK || zstdQ >= gzipQ) {
		return "zstd"
	}
	if gzipOK && gzipQ > 0 {
		return "gzip"
	}
	return ""
}

func encodingQuality(header, encoding string) (float64, bool) {
	var wildcard float64
	wildcardOK := false
	for _, item := range strings.Split(header, ",") {
		parts := strings.Split(strings.TrimSpace(item), ";")
		name := strings.TrimSpace(parts[0])
		q := 1.0
		for _, param := range parts[1:] {
			key, value, ok := strings.Cut(strings.TrimSpace(param), "=")
			if !ok || !strings.EqualFold(strings.TrimSpace(key), "q") {
				continue
			}
			parsed, err := strconv.ParseFloat(strings.TrimSpace(value), 64)
			if err != nil || parsed < 0 || parsed > 1 {
				q = 0
			} else {
				q = parsed
			}
		}
		if strings.EqualFold(name, encoding) {
			return q, true
		}
		if name == "*" {
			wildcard, wildcardOK = q, true
		}
	}
	return wildcard, wildcardOK
}

type compressionWriter struct {
	http.ResponseWriter
	encoding   string
	logger     *slog.Logger
	method     string
	status     int
	header     bool
	done       bool
	candidate  bool
	buffer     bytes.Buffer
	compressor io.WriteCloser
}

func (w *compressionWriter) Unwrap() http.ResponseWriter { return w.ResponseWriter }

func (w *compressionWriter) WriteHeader(status int) {
	if w.header {
		return
	}
	w.status, w.header = status, true
}

func (w *compressionWriter) Write(p []byte) (int, error) {
	if w.done {
		if w.compressor != nil {
			return w.compressor.Write(p)
		}
		return w.ResponseWriter.Write(p)
	}
	if !w.header {
		w.WriteHeader(http.StatusOK)
	}
	w.setContentType(p)
	w.candidate = w.compressionCandidate()
	if w.candidate {
		addVary(w.Header(), "Accept-Encoding")
	}
	if !w.candidate || w.encoding == "" {
		w.commitHeader()
		return w.ResponseWriter.Write(p)
	}
	n, err := w.buffer.Write(p)
	if err != nil {
		return n, err
	}
	if w.buffer.Len() >= minimumCompressionSize {
		if err := w.startCompression(); err != nil {
			return n, err
		}
	}
	return n, nil
}

func (w *compressionWriter) setContentType(body []byte) {
	if w.Header().Get("Content-Type") != "" || len(body) == 0 {
		return
	}
	sample := body
	if len(sample) > 512 {
		sample = sample[:512]
	}
	w.Header().Set("Content-Type", http.DetectContentType(sample))
}

func (w *compressionWriter) compressionCandidate() bool {
	if w.method == http.MethodHead || w.Header().Get("Content-Encoding") != "" ||
		w.status == http.StatusNoContent || w.status == http.StatusNotModified ||
		(w.status >= 100 && w.status < 200) {
		return false
	}
	mediaType, _, err := mime.ParseMediaType(w.Header().Get("Content-Type"))
	if err != nil {
		return false
	}
	return mediaType == "application/json" ||
		mediaType == "application/x-ndjson" ||
		mediaType == "application/ndjson" ||
		(strings.HasPrefix(mediaType, "application/") && strings.HasSuffix(mediaType, "+json"))
}

func (w *compressionWriter) commitHeader() {
	if w.done {
		return
	}
	if !w.header {
		w.status, w.header = http.StatusOK, true
	}
	w.ResponseWriter.WriteHeader(w.status)
	w.done = true
}

func (w *compressionWriter) startCompression() error {
	if w.done {
		return nil
	}
	var compressor io.WriteCloser
	switch w.encoding {
	case "gzip":
		compressor = gzip.NewWriter(w.ResponseWriter)
	case "zstd":
		encoder, err := zstd.NewWriter(w.ResponseWriter, zstd.WithEncoderConcurrency(1))
		if err != nil {
			return fmt.Errorf("create zstd response encoder: %w", err)
		}
		compressor = encoder
	default:
		return fmt.Errorf("unsupported response encoding %q", w.encoding)
	}

	w.Header().Set("Content-Encoding", w.encoding)
	w.Header().Del("Content-Length")
	w.ResponseWriter.WriteHeader(w.status)
	w.compressor = compressor
	w.done = true
	if _, err := compressor.Write(w.buffer.Bytes()); err != nil {
		return fmt.Errorf("write compressed response: %w", err)
	}
	w.buffer.Reset()
	return nil
}

func (w *compressionWriter) finish() error {
	if w.compressor != nil {
		if err := w.compressor.Close(); err != nil {
			return fmt.Errorf("finish compressed response: %w", err)
		}
		return nil
	}
	if w.done {
		return nil
	}
	w.setContentType(w.buffer.Bytes())
	w.candidate = w.compressionCandidate()
	if w.candidate {
		addVary(w.Header(), "Accept-Encoding")
	}
	w.commitHeader()
	if w.buffer.Len() == 0 {
		return nil
	}
	if _, err := w.ResponseWriter.Write(w.buffer.Bytes()); err != nil {
		return fmt.Errorf("write buffered response: %w", err)
	}
	return nil
}

func (w *compressionWriter) Flush() {
	if !w.done {
		w.candidate = w.compressionCandidate()
		if w.candidate {
			addVary(w.Header(), "Accept-Encoding")
		}
		if w.encoding != "" && w.candidate && w.buffer.Len() < minimumCompressionSize {
			return
		}
		if w.encoding != "" && w.candidate {
			if err := w.startCompression(); err != nil {
				w.logError("create compressed streaming response", err)
				if !w.done {
					w.encoding = ""
					w.commitHeader()
					w.flushBuffer()
				}
			}
		} else {
			w.encoding = ""
			w.commitHeader()
			w.flushBuffer()
		}
	}
	if w.compressor != nil {
		switch compressor := w.compressor.(type) {
		case *gzip.Writer:
			if err := compressor.Flush(); err != nil {
				w.logError("flush gzip response", err)
				return
			}
		case *zstd.Encoder:
			if err := compressor.Flush(); err != nil {
				w.logError("flush zstd response", err)
				return
			}
		}
	}
	if flusher, ok := w.ResponseWriter.(http.Flusher); ok {
		flusher.Flush()
	}
}

func (w *compressionWriter) flushBuffer() {
	if w.buffer.Len() == 0 {
		return
	}
	if _, err := w.ResponseWriter.Write(w.buffer.Bytes()); err != nil {
		w.logError("write buffered response", err)
		return
	}
	w.buffer.Reset()
}

func (w *compressionWriter) logError(message string, err error) {
	if w.logger != nil {
		w.logger.Error(message, "err", err)
	}
}

func addVary(header http.Header, value string) {
	for _, existing := range header.Values("Vary") {
		for _, token := range strings.Split(existing, ",") {
			if strings.EqualFold(strings.TrimSpace(token), value) {
				return
			}
		}
	}
	header.Add("Vary", value)
}
