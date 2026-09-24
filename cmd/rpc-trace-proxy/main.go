// rpc-trace-proxy exposes a deliberately narrow HTTP JSON-RPC interface over Unix.
package main

import (
	"bufio"
	"bytes"
	"context"
	"encoding/hex"
	"encoding/json"
	"errors"
	"flag"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"
)

const maxTracerTimeout = 15 * time.Second

var errResponseLimit = errors.New("response exceeds limit")

type proxy struct {
	ipc                         string
	slots                       chan struct{}
	timeout                     time.Duration
	requestLimit, responseLimit int64
	logger                      *log.Logger
}

type rpcRequest struct {
	JSONRPC string          `json:"jsonrpc"`
	ID      json.RawMessage `json:"id"`
	Method  string          `json:"method"`
	Params  json.RawMessage `json:"params,omitempty"`
}

// Reject duplicate keys at every level, including escaped spellings. A map-only
// decoder would silently accept the last value, creating parser ambiguity.
func uniqueJSON(d *json.Decoder, depth int) error {
	if depth > 12 {
		return errors.New("nesting too deep")
	}
	t, err := d.Token()
	if err != nil {
		return err
	}
	delim, ok := t.(json.Delim)
	if !ok {
		return nil
	}
	switch delim {
	case '{':
		seen := map[string]bool{}
		for d.More() {
			k, err := d.Token()
			if err != nil {
				return err
			}
			key, ok := k.(string)
			if !ok || seen[key] {
				return errors.New("duplicate key")
			}
			seen[key] = true
			if err := uniqueJSON(d, depth+1); err != nil {
				return err
			}
		}
	case '[':
		for d.More() {
			if err := uniqueJSON(d, depth+1); err != nil {
				return err
			}
		}
	default:
		return errors.New("unexpected delimiter")
	}
	_, err = d.Token()
	return err
}

func object(b []byte, allowed ...string) (map[string]json.RawMessage, error) {
	var m map[string]json.RawMessage
	if err := json.Unmarshal(b, &m); err != nil || m == nil {
		return nil, errors.New("expected object")
	}
	for k := range m {
		found := false
		for _, a := range allowed {
			if k == a {
				found = true
			}
		}
		if !found {
			return nil, errors.New("unknown field")
		}
	}
	return m, nil
}
func stringValue(b []byte) string { var s string; _ = json.Unmarshal(b, &s); return s }

// Returns only a newly encoded, validated request; raw user JSON never reaches IPC.
// All batches and notifications are rejected, even all-allowlisted batches.
func validate(b []byte, budget time.Duration) (rpcRequest, int) {
	var r rpcRequest
	if !json.Valid(b) {
		return r, -32700
	}
	d := json.NewDecoder(bytes.NewReader(b))
	d.UseNumber()
	if uniqueJSON(d, 0) != nil {
		return r, -32600
	}
	m, err := object(b, "jsonrpc", "id", "method", "params")
	if err != nil || stringValue(m["jsonrpc"]) != "2.0" {
		return r, -32600
	}
	id := m["id"]
	if len(id) == 0 || bytes.Equal(id, []byte("null")) {
		return r, -32600
	}
	var idValue any
	idDecoder := json.NewDecoder(bytes.NewReader(id))
	idDecoder.UseNumber()
	if idDecoder.Decode(&idValue) != nil {
		return r, -32600
	}
	switch idValue.(type) {
	case string, json.Number:
	default:
		return r, -32600
	}
	r = rpcRequest{JSONRPC: "2.0", ID: id, Method: stringValue(m["method"])}
	switch r.Method {
	case "eth_chainId", "eth_blockNumber":
		if p, exists := m["params"]; exists && !bytes.Equal(bytes.TrimSpace(p), []byte("[]")) {
			var args []json.RawMessage
			if json.Unmarshal(p, &args) != nil || args == nil || len(args) != 0 {
				return r, -32602
			}
		}
		r.Params = json.RawMessage("[]")
	case "debug_traceBlockByHash":
		var args []json.RawMessage
		if json.Unmarshal(m["params"], &args) != nil || len(args) != 2 {
			return r, -32602
		}
		hash := stringValue(args[0])
		if len(hash) != 66 || hash[:2] != "0x" {
			return r, -32602
		}
		if _, err := hex.DecodeString(hash[2:]); err != nil {
			return r, -32602
		}
		opts, err := object(args[1], "tracer", "tracerConfig", "timeout")
		if err != nil || stringValue(opts["tracer"]) != "callTracer" {
			return r, -32602
		}
		tc, err := object(opts["tracerConfig"], "withLog")
		if err != nil || !bytes.Equal(tc["withLog"], []byte("true")) {
			return r, -32602
		}
		limit := maxTracerTimeout
		if budget*3/4 < limit {
			limit = budget * 3 / 4
		}
		if v, exists := opts["timeout"]; exists {
			duration, err := time.ParseDuration(stringValue(v))
			if err != nil || duration <= 0 || duration > limit {
				return r, -32602
			}
			limit = duration
		}
		// Nitro's timeout is per transaction; our context bounds the
		// whole HTTP request. IPC work keeps its slot until Nitro finishes.
		r.Params, _ = json.Marshal([]any{hash, map[string]any{"tracer": "callTracer", "tracerConfig": map[string]bool{"withLog": true}, "timeout": limit.String()}})
	default:
		return r, -32601
	}
	return r, 0
}

func fail(w http.ResponseWriter, status int, id json.RawMessage, code int, message string) {
	if len(id) == 0 {
		id = json.RawMessage("null")
	}
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(struct {
		JSONRPC string          `json:"jsonrpc"`
		ID      json.RawMessage `json:"id"`
		Error   any             `json:"error"`
	}{"2.0", id, struct {
		Code    int    `json:"code"`
		Message string `json:"message"`
	}{code, message}})
}

func (p *proxy) ServeHTTP(w http.ResponseWriter, req *http.Request) {
	w.Header().Set("Content-Type", "application/json")
	w.Header().Set("Cache-Control", "no-store")
	if req.Method != http.MethodPost || req.URL.Path != "/" || req.URL.RawQuery != "" {
		fail(w, 400, nil, -32600, "POST / required")
		return
	}
	if req.Header.Get("Content-Encoding") != "" {
		fail(w, 415, nil, -32600, "content encoding not supported")
		return
	}
	// No queue. Hold the slot through reading, IPC and writing to bound buffers.
	select {
	case p.slots <- struct{}{}:
	default:
		fail(w, 503, nil, -32000, "proxy busy")
		return
	}
	started := false
	clientDone := make(chan struct{})
	defer func() {
		close(clientDone)
		if !started {
			<-p.slots
		}
	}()
	ctx, cancel := context.WithTimeout(req.Context(), p.timeout)
	defer cancel()
	body, err := io.ReadAll(http.MaxBytesReader(w, req.Body, p.requestLimit))
	if err != nil {
		fail(w, 413, nil, -32600, "request unreadable or too large")
		return
	}
	rpc, code := validate(body, p.timeout)
	if code != 0 {
		fail(w, 400, rpc.ID, code, "request not allowed")
		return
	}
	start := time.Now()
	type reply struct {
		body []byte
		err  error
	}
	ready := make(chan reply, 1)
	started = true
	go func() {
		body, err := p.forward(ctx, rpc)
		ready <- reply{body, err}
		<-clientDone
		<-p.slots
	}()
	var response []byte
	select {
	case result := <-ready:
		response, err = result.body, result.err
	case <-ctx.Done():
		err = ctx.Err()
	}
	outcome := "ok"
	defer func() {
		if p.logger != nil {
			p.logger.Printf("method=%s outcome=%s duration_ms=%d response_bytes=%d", rpc.Method, outcome, time.Since(start).Milliseconds(), len(response))
		}
	}()
	if err != nil {
		outcome = "upstream_error"
		status, code, message := 502, -32002, "upstream unavailable or invalid response"
		var ne net.Error
		if errors.Is(err, context.DeadlineExceeded) || errors.Is(ctx.Err(), context.DeadlineExceeded) || (errors.As(err, &ne) && ne.Timeout()) {
			outcome = "timeout"
			status, code, message = 504, -32001, "upstream timeout"
		} else if errors.Is(err, errResponseLimit) {
			outcome = "response_limit"
			code, message = -32003, "upstream response too large"
		}
		fail(w, status, rpc.ID, code, message)
		return
	}
	w.Header().Set("Content-Length", fmt.Sprint(len(response)))
	if _, err := w.Write(response); err != nil {
		outcome = "client_write_error"
	}
}

func (p *proxy) forward(ctx context.Context, req rpcRequest) ([]byte, error) {
	conn, err := (&net.Dialer{}).DialContext(ctx, "unix", p.ipc)
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	deadline, _ := ctx.Deadline()
	if err := conn.SetWriteDeadline(deadline); err != nil {
		return nil, err
	}
	if err := ctx.Err(); err != nil {
		return nil, err
	}
	if err := json.NewEncoder(conn).Encode(req); err != nil {
		return nil, err
	}
	// geth's IPC jsonCodec uses Encoder.Encode: exactly one JSON value + newline.
	// ReadSlice caps each fragment; no unbounded Decoder/ReadBytes allocations.
	reader := bufio.NewReaderSize(conn, 64<<10)
	var buf bytes.Buffer
	var responseErr error
	// geth waits for in-flight calls before canceling the IPC root context.
	// Keep this connection/slot until a full reply (or EOF), even after the
	// HTTP deadline. Never let retries accumulate untracked Nitro work.
	for {
		fragment, err := reader.ReadSlice('\n')
		if responseErr == nil {
			if ctx.Err() != nil {
				responseErr = ctx.Err()
			} else if int64(buf.Len())+int64(len(fragment)) > p.responseLimit {
				responseErr = errResponseLimit
			}
		}
		if responseErr == nil {
			_, _ = buf.Write(fragment)
		} else {
			buf = bytes.Buffer{}
		}
		if err == nil {
			break
		}
		if !errors.Is(err, bufio.ErrBufferFull) {
			return nil, err
		}
	}
	if responseErr != nil {
		return nil, responseErr
	}
	b := buf.Bytes()
	if !json.Valid(b) || len(b) == 0 || b[0] != '{' {
		return nil, errors.New("invalid IPC response")
	}
	return b, nil
}

func run() error {
	socket := flag.String("listen", "/run/rh-trace-proxy/trace.sock", "HTTP Unix socket (0600; parent must exist)")
	ipc := flag.String("ipc", "/data/nitro/ipc/nitro-debug.ipc", "Nitro IPC Unix socket")
	concurrency := flag.Int("concurrency", 4, "maximum in-flight requests; excess receive HTTP 503")
	timeout := flag.Duration("timeout", 20*time.Second, "HTTP request deadline (1s..20s); IPC retains its slot until completion")
	requestLimit := flag.Int64("max-request-bytes", 16<<10, "HTTP body limit")
	responseLimit := flag.Int64("max-response-bytes", 64<<20, "IPC response limit including newline")
	flag.Parse()
	if *concurrency < 1 || *concurrency > 32 || *timeout < time.Second || *timeout > 20*time.Second || *requestLimit < 256 || *requestLimit > 1<<20 || *responseLimit < 256 || *responseLimit > 256<<20 {
		return errors.New("invalid resource limits")
	}
	// Do not unlink any pre-existing path. systemd owns/cleans RuntimeDirectory.
	syscall.Umask(0077)
	listener, err := net.Listen("unix", *socket)
	if err != nil {
		return err
	}
	defer listener.Close()
	if err := os.Chmod(*socket, 0600); err != nil {
		return err
	}
	p := &proxy{*ipc, make(chan struct{}, *concurrency), *timeout, *requestLimit, *responseLimit, log.Default()}
	server := &http.Server{Handler: p, ReadHeaderTimeout: 5 * time.Second, ReadTimeout: 5 * time.Second, WriteTimeout: *timeout + 5*time.Second, IdleTimeout: 30 * time.Second, MaxHeaderBytes: 8 << 10}
	ctx, stop := signal.NotifyContext(context.Background(), syscall.SIGINT, syscall.SIGTERM)
	defer stop()
	done := make(chan struct{})
	shutdownDone := make(chan struct{})
	go func() {
		defer close(shutdownDone)
		select {
		case <-ctx.Done():
			shutdown, cancel := context.WithTimeout(context.Background(), *timeout+5*time.Second)
			defer cancel()
			_ = server.Shutdown(shutdown)
		case <-done:
		}
	}()
	log.Print("HTTP Unix trace proxy ready")
	err = server.Serve(listener)
	close(done)
	<-shutdownDone
	if errors.Is(err, http.ErrServerClosed) {
		return nil
	}
	return err
}
func main() {
	if err := run(); err != nil {
		log.Fatal(err)
	}
}
