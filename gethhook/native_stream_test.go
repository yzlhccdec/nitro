//go:build !wasm

package gethhook

import (
	"bytes"
	"context"
	"math/big"
	"net"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/crypto"
	"github.com/offchainlabs/nitro/arbos"
)

// These two PoolKeys and call paths were captured from historical block 69477187,
// tx 0x265a1284...55456c, using callTracer(withLog). The hash must match the
// exact Swap log on each distinct child call, even though USDG never transfers.
func TestNativeStreamV2TwoPoolKeyCallLogReplay(t *testing.T) {
	manager := common.HexToAddress("0x8366a39cc670b4001a1121b8f6a443a643e40951")
	basePath := []uint16{0, 3, 0, 5, 0, 2, 0, 0}
	keys := []struct {
		currency0, currency1 string
		fee, spacing         uint64
		poolID               string
	}{
		{"0x5fc5360d0400a0fd4f2af552add042d716f1d168", "0xce24439f2d9c6a2289f741120fe202248b666666", 75, 1, "0xf399bd1544377680d48c62fd85c2105b869e55906c4189cc5ab3b4e83446928c"},
		{"0x2e8c31162b855a2ffa90f6f8634643ad6f111e18", "0x5fc5360d0400a0fd4f2af552add042d716f1d168", 2300, 23, "0x7aebd80541bfaaf23dbb6e99ce13d4d31c1a84c91414f971eadbff7db5f85995"},
	}
	tx := arbos.NativeTxEvidence{TxHash: common.HexToHash("0x265a1284fcf97ad174592ef653fa63e87401f952ea1f29a16d924b3b9c55456c"), FactsComplete: true}
	for i, key := range keys {
		poolKey := make([]byte, 160)
		copy(poolKey[12:32], common.HexToAddress(key.currency0).Bytes())
		copy(poolKey[44:64], common.HexToAddress(key.currency1).Bytes())
		poolKey[95] = byte(key.fee)
		poolKey[127] = byte(key.spacing)
		if key.fee > 255 {
			poolKey[94] = byte(key.fee >> 8)
		}
		if crypto.Keccak256Hash(poolKey).Hex() != key.poolID {
			t.Fatalf("fixture PoolKey %d does not match Swap poolId", i)
		}
		path := append(append([]uint16(nil), basePath...), uint16(4+i))
		input := append([]byte{0xf3, 0xcd, 0x91, 0x4c}, poolKey...)
		tx.Calls = append(tx.Calls, arbos.NativeCallFact{TraceAddress: path, From: common.HexToAddress("0xbd665831a520182e944af92b23a317791c5d8efa"), To: manager, Kind: 0xf1, Value: big.NewInt(0), Input: input, Success: true})
		tx.LogScopes = append(tx.LogScopes, arbos.NativeLogScope{Index: uint32(19 + i), TraceAddress: path})
	}
	f := NativeJournalFrame{Sequence: 1, BlockNumber: 69477187, BlockHash: common.HexToHash("0x7f1ef4982ad19e254f04c01d1ec07ec57f2948b96922c3efa2fbe7937ff4186f"), Complete: true, Txs: []arbos.NativeTxEvidence{tx}}
	payload, err := EncodeNativeStreamFrame(f)
	if err != nil {
		t.Fatal(err)
	}
	decoded, err := DecodeNativeStreamFrame(payload)
	if err != nil {
		t.Fatal(err)
	}
	got := decoded.Txs[0]
	if !got.FactsComplete || len(got.Calls) != 2 || len(got.LogScopes) != 2 {
		t.Fatalf("lost replay facts: %+v", got)
	}
	for i, scope := range got.LogScopes {
		matched := 0
		for _, call := range got.Calls {
			if len(call.TraceAddress) != len(scope.TraceAddress) {
				continue
			}
			same := true
			for j := range scope.TraceAddress {
				same = same && call.TraceAddress[j] == scope.TraceAddress[j]
			}
			if same && call.To == manager && call.Success && crypto.Keccak256Hash(call.Input[4:164]).Hex() == keys[i].poolID {
				matched++
			}
		}
		if matched != 1 || scope.Index != uint32(19+i) {
			t.Fatalf("Swap log %d has %d PoolKey proofs", scope.Index, matched)
		}
	}
}

func TestNativeStreamV2CallLogFactsRoundTripAndNTX1Compatibility(t *testing.T) {
	block, evidence := streamTestBlock(123, common.HexToHash("0x33"))
	manager := common.HexToAddress("0x8366a39cc670b4001a1121b8f6a443a643e40951")
	input := append([]byte{0xf3, 0xcd, 0x91, 0x4c}, bytes.Repeat([]byte{0x11}, 320)...)
	evidence.Txs = []arbos.NativeTxEvidence{{TxHash: common.HexToHash("0x1234"), FactsComplete: true,
		Transfers: []arbos.NativeTransfer{{From: manager, To: common.HexToAddress("0x2"), Value: big.NewInt(7), Kind: 0xf1, TraceAddress: []uint16{0, 3}}},
		Calls:     []arbos.NativeCallFact{{TraceAddress: []uint16{0, 3}, From: common.HexToAddress("0x1"), To: manager, Kind: 0xf1, Input: input, Success: true}},
		LogScopes: []arbos.NativeLogScope{{Index: 19, TraceAddress: []uint16{0, 3}}},
	}}
	f := NativeJournalFrame{Sequence: 1, BlockNumber: 123, ParentHash: block.ParentHash(), BlockHash: block.Hash(), Complete: true, Txs: evidence.Txs}
	old, err := EncodeNativeFrame(f)
	if err != nil {
		t.Fatal(err)
	}
	if string(old[:4]) != "NTX1" {
		t.Fatalf("journal format changed: %q", old[:4])
	}
	v2, err := EncodeNativeStreamFrame(f)
	if err != nil {
		t.Fatal(err)
	}
	if string(v2[:4]) != "NTX2" {
		t.Fatalf("stream format: %q", v2[:4])
	}
	got, err := DecodeNativeStreamFrame(v2)
	if err != nil {
		t.Fatal(err)
	}
	tx := got.Txs[0]
	if !tx.FactsComplete || len(tx.Calls) != 1 || !bytes.Equal(tx.Calls[0].Input, input) || !tx.Calls[0].Success || len(tx.LogScopes) != 1 || tx.LogScopes[0].Index != 19 || len(tx.Transfers[0].TraceAddress) != 2 || tx.Transfers[0].TraceAddress[1] != 3 {
		t.Fatalf("lost facts: %+v", tx)
	}
	if _, err := DecodeNativeStreamFrame(v2[:len(v2)-1]); err == nil {
		t.Fatal("accepted truncated facts")
	}
	if _, err := DecodeNativeFrame(v2); err == nil {
		t.Fatal("journal reader accepted NTX2")
	}
}

func TestNativeStreamHandshakeVersionNegotiation(t *testing.T) {
	s, err := NewNativeStream("unused.sock")
	if err != nil {
		t.Fatal(err)
	}
	for _, tc := range []struct{ v2 bool }{{false}, {true}} {
		server, client := net.Pipe()
		result := make(chan bool, 1)
		go func() { v2, e := s.handshake(server); result <- e == nil && v2 == tc.v2 }()
		var epoch [16]byte
		if tc.v2 {
			epoch, err = PerformNativeReceiverHandshakeV2(client)
		} else {
			epoch, err = PerformNativeReceiverHandshake(client)
		}
		if err != nil || epoch != s.epoch || !<-result {
			t.Fatalf("handshake v2=%v epoch=%x err=%v", tc.v2, epoch, err)
		}
		server.Close()
		client.Close()
	}
}

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
