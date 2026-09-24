//go:build !wasm

package main

import (
	"bytes"
	"encoding/binary"
	"encoding/hex"
	"encoding/json"
	"errors"
	"io"
	"math/big"
	"net"
	"os"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/offchainlabs/nitro/arbos"
	"github.com/offchainlabs/nitro/gethhook"
)

type safeOutput struct {
	mu sync.Mutex
	bytes.Buffer
}

func (s *safeOutput) Write(p []byte) (int, error) {
	s.mu.Lock()
	defer s.mu.Unlock()
	return s.Buffer.Write(p)
}
func (s *safeOutput) String() string { s.mu.Lock(); defer s.mu.Unlock(); return s.Buffer.String() }

func sendTestFrame(t *testing.T, conn net.Conn, seq, number uint64, parent common.Hash) common.Hash {
	t.Helper()
	hash := common.BigToHash(new(big.Int).SetUint64(number))
	frame := gethhook.NativeJournalFrame{Sequence: seq, BlockNumber: number, ParentHash: parent, BlockHash: hash, Complete: true}
	payload, err := gethhook.EncodeNativeFrame(frame)
	if err != nil {
		t.Fatal(err)
	}
	var prefix [4]byte
	binary.BigEndian.PutUint32(prefix[:], uint32(len(payload)))
	if _, err := conn.Write(append(prefix[:], payload...)); err != nil {
		t.Fatal(err)
	}
	return hash
}

func sendTestPayload(t *testing.T, conn net.Conn, payload []byte) {
	t.Helper()
	var prefix [4]byte
	binary.BigEndian.PutUint32(prefix[:], uint32(len(payload)))
	if _, err := conn.Write(append(prefix[:], payload...)); err != nil {
		t.Fatal(err)
	}
}

func TestReceiverV2OutputsCompleteFactsAndGap(t *testing.T) {
	producer, server := net.Pipe()
	defer producer.Close()
	_ = producer.SetDeadline(time.Now().Add(2 * time.Second))
	out := new(safeOutput)
	r := &receiver{output: out, protocolVersion: 2}
	go r.accept(server)
	var hello [5]byte
	if _, err := io.ReadFull(producer, hello[:]); err != nil {
		t.Fatal(err)
	}
	if string(hello[:]) != "NTXR2" {
		t.Fatalf("wrong hello: %q", hello)
	}
	if _, err := producer.Write(append([]byte("NTXS2"), make([]byte, 16)...)); err != nil {
		t.Fatal(err)
	}
	manager := common.HexToAddress("0x8366a39cc670b4001a1121b8f6a443a643e40951")
	caller := common.HexToAddress("0xbd665831a520182e944af92b23a317791c5d8efa")
	path := []uint16{0, 3, 0, 5, 0, 2, 0, 0, 4}
	poolKey := make([]byte, 160)
	copy(poolKey[12:32], common.HexToAddress("0x5fc5360d0400a0fd4f2af552add042d716f1d168").Bytes())
	copy(poolKey[44:64], common.HexToAddress("0xce24439f2d9c6a2289f741120fe202248b666666").Bytes())
	poolKey[95], poolKey[127] = 75, 1
	input := append([]byte{0xf3, 0xcd, 0x91, 0x4c}, poolKey...)
	tx := arbos.NativeTxEvidence{TxHash: common.HexToHash("0x265a1284fcf97ad174592ef653fa63e87401f952ea1f29a16d924b3b9c55456c"), TxType: 2, FactsComplete: true,
		Transfers: []arbos.NativeTransfer{{From: caller, To: manager, Value: big.NewInt(42), Kind: 0xf1, TraceAddress: path}},
		Calls:     []arbos.NativeCallFact{{TraceAddress: path, From: caller, To: manager, Kind: 0xf1, Value: big.NewInt(0), Input: input, Success: true}},
		LogScopes: []arbos.NativeLogScope{{Index: 19, TraceAddress: path}},
	}
	first := gethhook.NativeJournalFrame{Sequence: 1, BlockNumber: 69477187, ParentHash: common.HexToHash("0x1111"), BlockHash: common.HexToHash("0x7f1ef4982ad19e254f04c01d1ec07ec57f2948b96922c3efa2fbe7937ff4186f"), Complete: true, Txs: []arbos.NativeTxEvidence{tx}}
	payload, err := gethhook.EncodeNativeStreamFrame(first)
	if err != nil {
		t.Fatal(err)
	}
	sendTestPayload(t, producer, payload)
	second := gethhook.NativeJournalFrame{Sequence: 3, BlockNumber: first.BlockNumber + 2, ParentHash: first.BlockHash, BlockHash: common.HexToHash("0x2222"), Complete: true, Txs: []arbos.NativeTxEvidence{{TxHash: common.HexToHash("0x3333"), FactsComplete: false, Calls: tx.Calls, LogScopes: tx.LogScopes}}}
	payload, err = gethhook.EncodeNativeStreamFrame(second)
	if err != nil {
		t.Fatal(err)
	}
	sendTestPayload(t, producer, payload)
	until := time.Now().Add(time.Second)
	for strings.Count(out.String(), "\n") < 2 && time.Now().Before(until) {
		time.Sleep(5 * time.Millisecond)
	}
	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	if len(lines) != 2 {
		t.Fatalf("expected two NDJSON blocks, got %q", out.String())
	}
	var a, b outputBlock
	if err := json.Unmarshal([]byte(lines[0]), &a); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal([]byte(lines[1]), &b); err != nil {
		t.Fatal(err)
	}
	if a.Schema != "native_stream_v2" || a.FrameVersion != 2 || !a.ContinuityUnknown || len(a.Transactions) != 1 {
		t.Fatalf("wrong V2 header: %+v", a)
	}
	got := a.Transactions[0]
	if got.Hash != tx.TxHash.Hex() || got.Index != 0 || got.Type != tx.TxType || !got.FactsComplete || len(got.Transfers) != 1 || got.Transfers[0].ValueWei != "42" || got.Transfers[0].Kind != 0xf1 || len(got.Transfers[0].TraceAddress) != len(path) || len(got.Calls) != 1 || got.Calls[0].InputHex != "0x"+hex.EncodeToString(input) || got.Calls[0].ValueWei != "0" || !got.Calls[0].Success || len(got.LogScopes) != 1 || got.LogScopes[0].LogIndex != 19 || got.LogScopes[0].TraceAddress[len(path)-1] != 4 {
		t.Fatalf("lost V2 facts: %+v", got)
	}
	if b.ContinuityUnknown || !strings.Contains(b.Gap, "sequence_1_to_3") || b.Transactions[0].FactsComplete {
		t.Fatalf("gap/incomplete not visible: %+v", b)
	}
	if len(b.Transactions[0].Calls) != 0 || len(b.Transactions[0].LogScopes) != 0 {
		t.Fatalf("incomplete facts escaped to JSON: %+v", b.Transactions[0])
	}
	if fixture := os.Getenv("NTX2_FIXTURE_OUT"); fixture != "" {
		if err := os.WriteFile(fixture, []byte(out.String()), 0o600); err != nil {
			t.Fatal(err)
		}
	}
}

func TestReceiverRejectsFrameVersionMismatch(t *testing.T) {
	for _, tc := range []struct {
		name    string
		version uint8
		ready   string
		encode  func(gethhook.NativeJournalFrame) ([]byte, error)
	}{
		{"v2_receives_v1", 2, "NTXS2", gethhook.EncodeNativeFrame},
		{"v1_receives_v2", 1, "NTXS1", gethhook.EncodeNativeStreamFrame},
	} {
		t.Run(tc.name, func(t *testing.T) {
			producer, server := net.Pipe()
			defer producer.Close()
			_ = producer.SetDeadline(time.Now().Add(time.Second))
			out := new(safeOutput)
			r := &receiver{output: out, protocolVersion: tc.version}
			go r.accept(server)
			var hello [5]byte
			if _, err := io.ReadFull(producer, hello[:]); err != nil {
				t.Fatal(err)
			}
			if string(hello[:4]) != "NTXR" || hello[4] != '0'+tc.version {
				t.Fatalf("unexpected hello: %q", hello)
			}
			if _, err := producer.Write(append([]byte(tc.ready), make([]byte, 16)...)); err != nil {
				t.Fatal(err)
			}
			payload, err := tc.encode(gethhook.NativeJournalFrame{Sequence: 1, BlockNumber: 100, BlockHash: common.HexToHash("0x1234"), Complete: true})
			if err != nil {
				t.Fatal(err)
			}
			sendTestPayload(t, producer, payload)
			var scratch [1]byte
			if _, err := producer.Read(scratch[:]); err == nil {
				t.Fatal("mismatched stream remained open")
			}
			if out.String() != "" {
				t.Fatalf("mismatched frame emitted output: %q", out.String())
			}
		})
	}
}

func TestReceiverV2RejectsWrongHandshakeAndMalformedFrame(t *testing.T) {
	for _, tc := range []struct {
		name   string
		ready  string
		mutate func([]byte) []byte
	}{
		{"wrong_ready", "NTXS1", nil},
		{"truncated", "NTXS2", func(b []byte) []byte { return b[:len(b)-1] }},
		{"trailing", "NTXS2", func(b []byte) []byte { return append(b, 0xff) }},
	} {
		t.Run(tc.name, func(t *testing.T) {
			producer, server := net.Pipe()
			defer producer.Close()
			_ = producer.SetDeadline(time.Now().Add(time.Second))
			out := new(safeOutput)
			r := &receiver{output: out, protocolVersion: 2}
			done := make(chan struct{})
			go func() { r.accept(server); close(done) }()
			var hello [5]byte
			if _, err := io.ReadFull(producer, hello[:]); err != nil {
				t.Fatal(err)
			}
			if string(hello[:]) != "NTXR2" {
				t.Fatalf("wrong hello %q", hello)
			}
			if _, err := producer.Write(append([]byte(tc.ready), make([]byte, 16)...)); err != nil {
				t.Fatal(err)
			}
			if tc.mutate == nil {
				<-done
				if r.current != nil || out.String() != "" {
					t.Fatal("wrong V2 ready was accepted")
				}
				return
			}
			<-done
			frame := gethhook.NativeJournalFrame{Sequence: 1, BlockNumber: 100, BlockHash: common.HexToHash("0x1234"), Complete: true}
			payload, err := gethhook.EncodeNativeStreamFrame(frame)
			if err != nil {
				t.Fatal(err)
			}
			sendTestPayload(t, producer, tc.mutate(payload))
			var scratch [1]byte
			if _, err := producer.Read(scratch[:]); err == nil {
				t.Fatal("malformed V2 stream remained open")
			}
			if out.String() != "" {
				t.Fatalf("malformed V2 frame emitted output: %q", out.String())
			}
		})
	}
}

func TestReceiverOutputsGapAndZeroTransferBlocks(t *testing.T) {
	producer, server := net.Pipe()
	defer producer.Close()
	if err := producer.SetDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	out := new(safeOutput)
	r := &receiver{output: out}
	go r.accept(server)
	var hello [5]byte
	if _, err := io.ReadFull(producer, hello[:]); err != nil {
		t.Fatal(err)
	}
	if string(hello[:]) != "NTXR1" {
		t.Fatalf("hello %q", hello)
	}
	ready := append([]byte("NTXS1"), make([]byte, 16)...)
	if _, err := producer.Write(ready); err != nil {
		t.Fatal(err)
	}
	first := sendTestFrame(t, producer, 1, 100, common.Hash{})
	_ = sendTestFrame(t, producer, 3, 102, first)
	until := time.Now().Add(time.Second)
	for time.Now().Before(until) {
		if strings.Count(out.String(), "\n") >= 2 {
			break
		}
		time.Sleep(5 * time.Millisecond)
	}
	lines := strings.Split(strings.TrimSpace(out.String()), "\n")
	if len(lines) != 2 {
		t.Fatalf("expected two blocks, got %q", out.String())
	}
	var a, b outputBlock
	if err := json.Unmarshal([]byte(lines[0]), &a); err != nil {
		t.Fatal(err)
	}
	if err := json.Unmarshal([]byte(lines[1]), &b); err != nil {
		t.Fatal(err)
	}
	if !a.Complete || !a.ContinuityUnknown || b.ContinuityUnknown || a.Number != 100 || len(a.Transactions) != 0 || b.Number != 102 || !strings.Contains(b.Gap, "sequence_1_to_3") || !strings.Contains(b.Gap, "chain_100_to_102") {
		t.Fatalf("bad output %+v %+v", a, b)
	}
}

func TestInvalidCandidateKeepsCurrentConnection(t *testing.T) {
	producer, server := net.Pipe()
	defer producer.Close()
	_ = producer.SetDeadline(time.Now().Add(2 * time.Second))
	out := new(safeOutput)
	r := &receiver{output: out}
	go r.accept(server)
	var hello [5]byte
	if _, err := io.ReadFull(producer, hello[:]); err != nil {
		t.Fatal(err)
	}
	if _, err := producer.Write(append([]byte("NTXS1"), make([]byte, 16)...)); err != nil {
		t.Fatal(err)
	}
	bad, candidate := net.Pipe()
	defer bad.Close()
	finished := make(chan struct{})
	go func() { r.accept(candidate); close(finished) }()
	if _, err := io.ReadFull(bad, hello[:]); err != nil {
		t.Fatal(err)
	}
	if _, err := bad.Write([]byte("BAD!!")); err != nil {
		t.Fatal(err)
	}
	select {
	case <-finished:
	case <-time.After(time.Second):
		t.Fatal("invalid candidate handshake did not finish")
	}
	sendTestFrame(t, producer, 1, 100, common.Hash{})
	until := time.Now().Add(time.Second)
	for !strings.Contains(out.String(), `"block_number":100`) && time.Now().Before(until) {
		time.Sleep(5 * time.Millisecond)
	}
	if !strings.Contains(out.String(), `"block_number":100`) {
		t.Fatalf("valid connection lost: %q", out.String())
	}
}

type failingOutput struct{}

func (failingOutput) Write([]byte) (int, error) { return 0, errors.New("stdout failed") }

func TestOutputFailureIsFatal(t *testing.T) {
	producer, server := net.Pipe()
	defer producer.Close()
	_ = producer.SetDeadline(time.Now().Add(2 * time.Second))
	r := &receiver{output: failingOutput{}, fatal: make(chan error, 1)}
	go r.accept(server)
	var hello [5]byte
	if _, err := io.ReadFull(producer, hello[:]); err != nil {
		t.Fatal(err)
	}
	if _, err := producer.Write(append([]byte("NTXS1"), make([]byte, 16)...)); err != nil {
		t.Fatal(err)
	}
	sendTestFrame(t, producer, 1, 100, common.Hash{})
	select {
	case err := <-r.fatal:
		if !strings.Contains(err.Error(), "stdout failed") || !r.failed.Load() {
			t.Fatalf("unexpected fatal state: %v", err)
		}
	case <-time.After(time.Second):
		t.Fatal("output failure was not fatal")
	}
	bad, candidate := net.Pipe()
	defer bad.Close()
	r.accept(candidate)
	if _, err := bad.Write([]byte("NTXS1")); err == nil {
		t.Fatal("receiver accepted a new connection after fatal output failure")
	}
}
