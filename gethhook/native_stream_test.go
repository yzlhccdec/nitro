//go:build !wasm

package gethhook

import (
	"context"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/offchainlabs/nitro/arbos"
)

func streamTestBlock(number uint64, parent common.Hash) (*types.Block, *arbos.NativeBlockEvidence) {
	block := types.NewBlockWithHeader(&types.Header{Number: new(big.Int).SetUint64(number), ParentHash: parent})
	return block, &arbos.NativeBlockEvidence{BlockHash: block.Hash()}
}

func waitNativeConnected(t *testing.T, s *NativeStream, want bool) {
	t.Helper()
	until := time.Now().Add(2 * time.Second)
	for time.Now().Before(until) {
		if s.Connected() == want {
			return
		}
		time.Sleep(5 * time.Millisecond)
	}
	t.Fatalf("Connected=%v, want %v", s.Connected(), want)
}

func TestNativeStreamConnectZeroTransfersOrderAndDisconnect(t *testing.T) {
	dir, err := os.MkdirTemp("/tmp", "ntx-")
	if err != nil {
		t.Fatal(err)
	}
	defer os.RemoveAll(dir)
	path := filepath.Join(dir, "native.sock")
	s, err := NewNativeStream(path)
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	done := make(chan struct{})
	go func() { s.Run(ctx); close(done) }()
	first, firstEvidence := streamTestBlock(100, common.Hash{})
	if s.Connected() || s.Publish(first, firstEvidence) {
		t.Fatal("must drop without receiver")
	}
	listener, err := net.Listen("unix", path)
	if err != nil {
		t.Fatal(err)
	}
	defer listener.Close()
	if err := listener.(*net.UnixListener).SetDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	conn, err := listener.Accept()
	if err != nil {
		t.Fatal(err)
	}
	if _, err := PerformNativeReceiverHandshake(conn); err != nil {
		t.Fatal(err)
	}
	waitNativeConnected(t, s, true)
	second, secondEvidence := streamTestBlock(101, first.Hash())
	third, thirdEvidence := streamTestBlock(102, second.Hash())
	if !s.Publish(second, secondEvidence) || !s.Publish(third, thirdEvidence) {
		t.Fatal("connected publish dropped")
	}
	if err := conn.SetReadDeadline(time.Now().Add(2 * time.Second)); err != nil {
		t.Fatal(err)
	}
	a, err := ReadNativeStreamFrame(conn)
	if err != nil {
		t.Fatal(err)
	}
	b, err := ReadNativeStreamFrame(conn)
	if err != nil {
		t.Fatal(err)
	}
	if a.BlockNumber != 101 || b.BlockNumber != 102 || a.Sequence != 1 || b.Sequence != 2 || !a.Complete || len(a.Txs) != 0 || b.ParentHash != a.BlockHash {
		t.Fatalf("bad ordered zero-transfer frames: %+v %+v", a, b)
	}
	_ = conn.Close()
	fourth, fourthEvidence := streamTestBlock(103, third.Hash())
	_ = s.Publish(fourth, fourthEvidence)
	waitNativeConnected(t, s, false)
	if s.Publish(fourth, fourthEvidence) {
		t.Fatal("disconnected publish accepted")
	}
	cancel()
	<-done
}

func TestNativeStreamQueueOverflowFencesSession(t *testing.T) {
	s, err := NewNativeStream(filepath.Join(t.TempDir(), "native.sock"))
	if err != nil {
		t.Fatal(err)
	}
	s.connected.Store(true) // simulate a receiver stalled before Run can dequeue
	var dropped bool
	for i := 0; i < nativeStreamQueue+1; i++ {
		block, evidence := streamTestBlock(uint64(i+1), common.Hash{})
		if !s.Publish(block, evidence) {
			dropped = true
		}
	}
	if !dropped || !s.forceClose.Load() || s.Dropped() != 1 {
		t.Fatalf("overflow not visible: dropped=%v force=%v count=%d", dropped, s.forceClose.Load(), s.Dropped())
	}
}
