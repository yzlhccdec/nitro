//go:build !wasm

package main

import (
	"bytes"
	"encoding/binary"
	"encoding/json"
	"errors"
	"io"
	"math/big"
	"net"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
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
