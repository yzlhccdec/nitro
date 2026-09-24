package main

import (
	"bytes"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"net/http"
	"net/http/httptest"
	"path/filepath"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

const health = `{"jsonrpc":"2.0","id":1,"method":"eth_chainId","params":[]}`
const healthResponse = "{\"jsonrpc\":\"2.0\",\"id\":1,\"result\":\"0xa4b1\"}\n"

var hash = "0x" + strings.Repeat("ab", 32)

func trace(opts string) string {
	return fmt.Sprintf(`{"jsonrpc":"2.0","id":"trace","method":"debug_traceBlockByHash","params":[%q,%s]}`, hash, opts)
}

const options = `{"tracer":"callTracer","tracerConfig":{"withLog":true}}`

func mockIPC(t *testing.T, handle func(net.Conn, rpcRequest)) (string, *atomic.Int32) {
	t.Helper()
	path := filepath.Join(t.TempDir(), "nitro.sock")
	l, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	var calls atomic.Int32
	var wg sync.WaitGroup
	wg.Add(1)
	go func() {
		defer wg.Done()
		for {
			c, err := l.Accept()
			if err != nil {
				return
			}
			wg.Add(1)
			go func() {
				defer wg.Done()
				defer c.Close()
				_ = c.SetDeadline(time.Now().Add(3 * time.Second))
				var r rpcRequest
				if json.NewDecoder(c).Decode(&r) == nil {
					calls.Add(1)
					handle(c, r)
				}
			}()
		}
	}()
	t.Cleanup(func() { _ = l.Close(); wg.Wait() })
	return path, &calls
}
func newProxy(ipc string) *proxy {
	return &proxy{ipc: ipc, slots: make(chan struct{}, 4), timeout: 20 * time.Second, requestLimit: 16 << 10, responseLimit: 64 << 20}
}
func request(p *proxy, body string) *httptest.ResponseRecorder {
	w := httptest.NewRecorder()
	p.ServeHTTP(w, httptest.NewRequest("POST", "/", strings.NewReader(body)))
	return w
}

func TestAllowRejectMatrix(t *testing.T) {
	path, calls := mockIPC(t, func(c net.Conn, r rpcRequest) {
		if r.Method == "debug_traceBlockByHash" {
			var params []json.RawMessage
			_ = json.Unmarshal(r.Params, &params)
			var opts map[string]json.RawMessage
			_ = json.Unmarshal(params[1], &opts)
			timeout, err := time.ParseDuration(stringValue(opts["timeout"]))
			if err != nil || timeout <= 0 || timeout > 15*time.Second {
				t.Errorf("unbounded tracer timeout")
			}
		}
		_, _ = io.WriteString(c, healthResponse)
	})
	p := newProxy(path)
	cases := []struct {
		name, body string
		code       int
	}{
		{"chain", health, 0},
		{"block", strings.Replace(health, "eth_chainId", "eth_blockNumber", 1), 0},
		{"no_params", `{"jsonrpc":"2.0","id":0,"method":"eth_chainId"}`, 0},
		{"empty_space", strings.Replace(health, "[]", "[ ]", 1), 0},
		{"trace", trace(options), 0},
		{"timeout", trace(`{"tracer":"callTracer","tracerConfig":{"withLog":true},"timeout":"15s"}`), 0},
		{"wrong_method", strings.Replace(health, "eth_chainId", "admin_peers", 1), -32601},
		{"send", strings.Replace(health, "eth_chainId", "eth_sendRawTransaction", 1), -32601},
		{"trace_number", strings.Replace(trace(options), "debug_traceBlockByHash", "debug_traceBlockByNumber", 1), -32601},
		{"js", trace(`{"tracer":"{step:function(){}}","tracerConfig":{"withLog":true}}`), -32602},
		{"other_tracer", trace(strings.Replace(options, "callTracer", "prestateTracer", 1)), -32602},
		{"no_logs", trace(strings.Replace(options, "true", "false", 1)), -32602},
		{"null_logs", trace(strings.Replace(options, "true", "null", 1)), -32602},
		{"string_logs", trace(strings.Replace(options, "true", `"true"`, 1)), -32602},
		{"extra_config", trace(`{"tracer":"callTracer","tracerConfig":{"withLog":true,"onlyTopCall":true}}`), -32602},
		{"extra_option", trace(`{"tracer":"callTracer","tracerConfig":{"withLog":true},"reexec":999}`), -32602},
		{"missing_config", trace(`{"tracer":"callTracer"}`), -32602},
		{"timeout_high", trace(`{"tracer":"callTracer","tracerConfig":{"withLog":true},"timeout":"16s"}`), -32602},
		{"timeout_zero", trace(`{"tracer":"callTracer","tracerConfig":{"withLog":true},"timeout":"0s"}`), -32602},
		{"timeout_negative", trace(`{"tracer":"callTracer","tracerConfig":{"withLog":true},"timeout":"-1s"}`), -32602},
		{"timeout_number", trace(`{"tracer":"callTracer","tracerConfig":{"withLog":true},"timeout":1}`), -32602},
		{"bad_hash", strings.Replace(trace(options), hash, "0x12", 1), -32602},
		{"nonhex_hash", strings.Replace(trace(options), hash, "0x"+strings.Repeat("zz", 32), 1), -32602},
		{"extra_param", strings.Replace(trace(options), "]}", ",1]}", 1), -32602},
		{"health_args", strings.Replace(health, "[]", "[1]", 1), -32602},
		{"health_null", strings.Replace(health, "[]", "null", 1), -32602},
		{"all_valid_batch", "[" + health + "]", -32600},
		{"mixed_batch", "[" + health + "," + strings.Replace(health, "eth_chainId", "admin_peers", 1) + "]", -32600},
		{"empty_batch", "[]", -32600},
		{"notification", `{"jsonrpc":"2.0","method":"eth_chainId"}`, -32600},
		{"null_id", strings.Replace(health, `"id":1`, `"id":null`, 1), -32600},
		{"bool_id", strings.Replace(health, `"id":1`, `"id":true`, 1), -32600},
		{"wrong_version", strings.Replace(health, "2.0", "1.0", 1), -32600},
		{"unknown_key", strings.Replace(health, `"id":1`, `"id":1,"extra":1`, 1), -32600},
		{"wrong_case", strings.Replace(health, `"method"`, `"Method"`, 1), -32600},
		{"duplicate", strings.Replace(health, `"id":1`, `"id":1,"id":2`, 1), -32600},
		{"escaped_duplicate", trace(strings.Replace(options, `"withLog":true`, `"withLog":false,"with\u004cog":true`, 1)), -32600},
		{"trailing", health + health, -32700},
		{"malformed", "{", -32700},
		{"null", "null", -32600},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			before := calls.Load()
			w := request(p, tc.body)
			waitReleased(t, p)
			if tc.code == 0 {
				if w.Code != 200 || calls.Load() != before+1 {
					t.Fatalf("status=%d calls=%d: %s", w.Code, calls.Load()-before, w.Body)
				}
				return
			}
			var result struct{ Error struct{ Code int } }
			if err := json.Unmarshal(w.Body.Bytes(), &result); err != nil {
				t.Fatal(err)
			}
			if w.Code != 400 || result.Error.Code != tc.code || calls.Load() != before {
				t.Fatalf("status=%d calls=%d: %s", w.Code, calls.Load()-before, w.Body)
			}
		})
	}
}

func TestLimitsAndInvalidUpstream(t *testing.T) {
	for _, tc := range []struct {
		name, response string
		limit          int64
		status         int
	}{
		{"at_limit", healthResponse, int64(len(healthResponse)), 200},
		{"over_limit", healthResponse, int64(len(healthResponse) - 1), 502},
		{"large", `{"result":"` + strings.Repeat("a", 200000) + "\"}\n", 300000, 200},
		{"large_over", `{"result":"` + strings.Repeat("a", 200000) + "\"}\n", 65536, 502},
		{"malformed", "not json\n", 1000, 502},
		{"truncated", `{"result":`, 1000, 502},
	} {
		t.Run(tc.name, func(t *testing.T) {
			path, _ := mockIPC(t, func(c net.Conn, _ rpcRequest) { _, _ = io.WriteString(c, tc.response) })
			p := newProxy(path)
			p.responseLimit = tc.limit
			w := request(p, health)
			if w.Code != tc.status {
				t.Fatalf("status=%d body=%s", w.Code, w.Body)
			}
			if tc.status != 200 && strings.Contains(w.Body.String(), strings.Repeat("a", 100)) {
				t.Fatal("leaked partial upstream body")
			}
		})
	}
	path, calls := mockIPC(t, func(c net.Conn, _ rpcRequest) { _, _ = io.WriteString(c, healthResponse) })
	p := newProxy(path)
	p.requestLimit = int64(len(health))
	if w := request(p, health); w.Code != 200 {
		t.Fatal(w.Code)
	}
	if w := request(p, health+" "); w.Code != 413 || calls.Load() != 1 {
		t.Fatal("body limit not enforced")
	}
}

// HTTP cancellation cannot cancel Nitro IPC execution. Admission stays occupied
// until its response arrives; otherwise retries would defeat the work limit.
func TestTimeoutAndCancellation(t *testing.T) {
	for _, cancelClient := range []bool{false, true} {
		t.Run(fmt.Sprint(cancelClient), func(t *testing.T) {
			entered, release := make(chan struct{}), make(chan struct{})
			var once sync.Once
			unblock := func() { once.Do(func() { close(release) }) }
			defer unblock()
			path, _ := mockIPC(t, func(c net.Conn, _ rpcRequest) { close(entered); <-release; _, _ = io.WriteString(c, healthResponse) })
			p := newProxy(path)
			p.timeout = 100 * time.Millisecond
			p.slots = make(chan struct{}, 1)
			req := httptest.NewRequest("POST", "/", strings.NewReader(health))
			ctx, cancel := context.WithCancel(req.Context())
			defer cancel()
			req = req.WithContext(ctx)
			done := make(chan *httptest.ResponseRecorder, 1)
			go func() { w := httptest.NewRecorder(); p.ServeHTTP(w, req); done <- w }()
			<-entered
			if cancelClient {
				cancel()
			}
			select {
			case w := <-done:
				if !cancelClient && w.Code != 504 {
					t.Fatal(w.Code)
				}
			case <-time.After(time.Second):
				t.Fatal("deadline ignored")
			}
			if w := request(p, health); w.Code != 503 {
				t.Fatal("orphaned IPC no longer counted")
			}
			unblock()
			waitReleased(t, p)
		})
	}
}
func waitReleased(t *testing.T, p *proxy) {
	t.Helper()
	deadline := time.Now().Add(time.Second)
	for len(p.slots) != 0 && time.Now().Before(deadline) {
		time.Sleep(time.Millisecond)
	}
	if len(p.slots) != 0 {
		t.Fatal("slot leaked after IPC completed")
	}
}

func TestConcurrency(t *testing.T) {
	entered, release := make(chan struct{}, 4), make(chan struct{})
	path, calls := mockIPC(t, func(c net.Conn, _ rpcRequest) {
		entered <- struct{}{}
		<-release
		_, _ = io.WriteString(c, healthResponse)
	})
	p := newProxy(path)
	var once sync.Once
	unblock := func() { once.Do(func() { close(release) }) }
	defer unblock()
	done := make(chan int, 4)
	for i := 0; i < 4; i++ {
		go func() { done <- request(p, health).Code }()
	}
	for i := 0; i < 4; i++ {
		select {
		case <-entered:
		case <-time.After(time.Second):
			t.Fatal("not concurrent")
		}
	}
	if w := request(p, health); w.Code != 503 || calls.Load() != 4 {
		t.Fatal("concurrency limit failed")
	}
	unblock()
	for i := 0; i < 4; i++ {
		if code := <-done; code != 200 {
			t.Fatal(code)
		}
	}
	waitReleased(t, p)
	if w := request(p, health); w.Code != 200 {
		t.Fatal("slot not released")
	}
}

func TestHTTPAndLogging(t *testing.T) {
	path, calls := mockIPC(t, func(c net.Conn, _ rpcRequest) { _, _ = io.WriteString(c, healthResponse) })
	p := newProxy(path)
	for _, tc := range []struct{ method, url, encoding string }{{"GET", "/", ""}, {"POST", "/other", ""}, {"POST", "/?x=1", ""}, {"POST", "/", "gzip"}} {
		req := httptest.NewRequest(tc.method, tc.url, strings.NewReader(health))
		req.Header.Set("Content-Encoding", tc.encoding)
		w := httptest.NewRecorder()
		p.ServeHTTP(w, req)
		if w.Code == 200 || calls.Load() != 0 {
			t.Fatal("bad HTTP accepted")
		}
	}
	var logs bytes.Buffer
	p.logger = log.New(&logs, "", 0)
	_ = request(p, trace(options))
	if strings.Contains(logs.String(), hash) || strings.Contains(logs.String(), "tracerConfig") || !strings.Contains(logs.String(), "response_bytes=") {
		t.Fatal("unsafe/missing logging", logs.String())
	}
	// Exercise the actual HTTP-over-Unix transport, not only ResponseRecorder.
	listener, err := net.Listen("unix", filepath.Join(t.TempDir(), "http.sock"))
	if err != nil {
		t.Fatal(err)
	}
	server := &http.Server{Handler: p}
	go func() { _ = server.Serve(listener) }()
	defer server.Close()
	transport := &http.Transport{DialContext: func(ctx context.Context, _, _ string) (net.Conn, error) {
		return (&net.Dialer{}).DialContext(ctx, "unix", listener.Addr().String())
	}}
	defer transport.CloseIdleConnections()
	client := &http.Client{Transport: transport, Timeout: time.Second}
	resp, err := client.Post("http://localhost/", "application/json", strings.NewReader(health))
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	if resp.StatusCode != 200 {
		t.Fatal(resp.StatusCode)
	}
}
