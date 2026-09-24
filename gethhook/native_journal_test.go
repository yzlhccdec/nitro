//go:build !wasm

package gethhook

import (
	"encoding/binary"
	"math/big"
	"os"
	"path/filepath"
	"strings"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/offchainlabs/nitro/arbos"
)

func nativeTestFrame(seq, block uint64, parent common.Hash) NativeJournalFrame {
	return NativeJournalFrame{Sequence: seq, BlockNumber: block, ParentHash: parent, Complete: true,
		BlockHash: common.BigToHash(new(big.Int).SetUint64(block)),
		Txs: []arbos.NativeTxEvidence{{TxHash: common.HexToHash("0x1234"), Transfers: []arbos.NativeTransfer{{
			From: common.HexToAddress("0x1"), To: common.HexToAddress("0x2"), Value: big.NewInt(123), Kind: 0xf1,
		}}}}}
}

func TestNativeJournalRoundTripAndRestart(t *testing.T) {
	dir := t.TempDir()
	w, err := openNativeWriter(dir)
	if err != nil {
		t.Fatal(err)
	}
	a := nativeTestFrame(1, 100, common.Hash{})
	b := nativeTestFrame(2, 101, a.BlockHash)
	if err := w.append(a); err != nil {
		t.Fatal(err)
	}
	if err := w.append(b); err != nil {
		t.Fatal(err)
	}
	w.close()
	frames, gaps, err := ReadNativeJournal(dir)
	if err != nil || len(gaps) != 0 || len(frames) != 2 {
		t.Fatalf("frames=%d gaps=%v err=%v", len(frames), gaps, err)
	}
	tr := frames[1].Txs[0].Transfers[0]
	if tr.Value.Cmp(big.NewInt(123)) != 0 || tr.Kind != 0xf1 || tr.To != common.HexToAddress("0x2") {
		t.Fatalf("bad transfer %+v", tr)
	}
	j, err := OpenNativeJournal(dir)
	if err != nil {
		t.Fatal(err)
	}
	if j.next.Load() != 2 {
		t.Fatalf("restart seq %d", j.next.Load())
	}
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	w, err = openNativeWriter(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.append(nativeTestFrame(3, 102, b.BlockHash)); err != nil {
		t.Fatal(err)
	}
	w.close()
	frames, gaps, err = ReadNativeJournal(dir)
	if err != nil || len(gaps) != 0 || len(frames) != 3 {
		t.Fatalf("restart frames=%d gaps=%v err=%v", len(frames), gaps, err)
	}
}

func TestNativeJournalDetectsGap(t *testing.T) {
	dir := t.TempDir()
	w, err := openNativeWriter(dir)
	if err != nil {
		t.Fatal(err)
	}
	a := nativeTestFrame(1, 100, common.Hash{})
	if err := w.append(a); err != nil {
		t.Fatal(err)
	}
	if err := w.append(nativeTestFrame(3, 102, a.BlockHash)); err != nil {
		t.Fatal(err)
	}
	w.close()
	_, gaps, err := ReadNativeJournal(dir)
	if err != nil || len(gaps) < 2 {
		t.Fatalf("gaps=%v err=%v", gaps, err)
	}
}

func TestNativeJournalTruncatedTailAndCorruption(t *testing.T) {
	dir := t.TempDir()
	w, err := openNativeWriter(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := w.append(nativeTestFrame(1, 100, common.Hash{})); err != nil {
		t.Fatal(err)
	}
	w.close()
	p := filepath.Join(dir, "native-000000.ntx")
	f, err := os.OpenFile(p, os.O_APPEND|os.O_WRONLY, 0)
	if err != nil {
		t.Fatal(err)
	}
	var h [8]byte
	binary.BigEndian.PutUint32(h[:4], 100)
	if _, err := f.Write(h[:]); err != nil {
		t.Fatal(err)
	}
	if _, err := f.Write([]byte{1, 2, 3}); err != nil {
		t.Fatal(err)
	}
	f.Close()
	frames, gaps, err := ReadNativeJournal(dir)
	if err != nil || len(frames) != 1 || len(gaps) != 1 || !strings.Contains(gaps[0], "truncated") {
		t.Fatalf("frames=%d gaps=%v err=%v", len(frames), gaps, err)
	}
	j, err := OpenNativeJournal(dir)
	if err != nil {
		t.Fatal(err)
	}
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	_, gaps, err = ReadNativeJournal(dir)
	if err != nil || len(gaps) != 1 || !strings.Contains(gaps[0], "truncated") {
		t.Fatalf("truncation marker not durable: gaps=%v err=%v", gaps, err)
	}
	data, err := os.ReadFile(p)
	if err != nil {
		t.Fatal(err)
	}
	data[len(data)-1] ^= 0x01
	if err := os.WriteFile(p, data, 0o600); err != nil {
		t.Fatal(err)
	}
	if _, _, err := ReadNativeJournal(dir); err == nil || !strings.Contains(err.Error(), "CRC") {
		t.Fatalf("expected CRC failure, got %v", err)
	}
	if _, err := OpenNativeJournal(dir); err == nil || !strings.Contains(err.Error(), "CRC") {
		t.Fatalf("configured journal must reject corruption: %v", err)
	}
}

func TestNativeJournalRefusesActiveOfflineReader(t *testing.T) {
	dir := t.TempDir()
	j, err := OpenNativeJournal(dir)
	if err != nil {
		t.Fatal(err)
	}
	if _, _, err := ReadNativeJournal(dir); err == nil || !strings.Contains(err.Error(), "active") {
		t.Fatalf("reader should refuse writer: %v", err)
	}
	if _, err := WalkNativeJournal(dir, func(NativeJournalFrame) error { return nil }); err == nil || !strings.Contains(err.Error(), "active") {
		t.Fatalf("walker should refuse writer: %v", err)
	}
	if _, err := WalkCompleteNativeJournal(dir, func(NativeJournalFrame) error { return nil }); err == nil || !strings.Contains(err.Error(), "active") {
		t.Fatalf("complete walker should refuse writer: %v", err)
	}
	if err := j.Close(); err != nil {
		t.Fatal(err)
	}
	if _, _, err := ReadNativeJournal(dir); err != nil {
		t.Fatal(err)
	}
}

func TestNativeJournalMarksReorgBeforeAttribution(t *testing.T) {
	dir := t.TempDir()
	w, err := openNativeWriter(dir)
	if err != nil {
		t.Fatal(err)
	}
	a := nativeTestFrame(1, 100, common.Hash{})
	b := nativeTestFrame(2, 101, a.BlockHash)
	fork := nativeTestFrame(3, 100, common.Hash{})
	fork.BlockHash = common.HexToHash("0xf0f0")
	for _, frame := range []NativeJournalFrame{a, b, fork} {
		if err := w.append(frame); err != nil {
			t.Fatal(err)
		}
	}
	w.close()
	_, gaps, err := ReadNativeJournal(dir)
	if err != nil || len(gaps) == 0 || !strings.Contains(strings.Join(gaps, " "), "rewound") {
		t.Fatalf("reorg not surfaced: gaps=%v err=%v", gaps, err)
	}
	called := 0
	gaps, err = WalkCompleteNativeJournal(dir, func(NativeJournalFrame) error { called++; return nil })
	if err != nil || len(gaps) == 0 || called != 0 {
		t.Fatalf("ambiguous fork emitted frames: called=%d gaps=%v err=%v", called, gaps, err)
	}
}

func TestNativeJournalCapAndPersistentFailureMarker(t *testing.T) {
	dir := t.TempDir()
	w, err := openNativeWriter(dir)
	if err != nil {
		t.Fatal(err)
	}
	w.total = nativeJournalMax
	if err := w.append(nativeTestFrame(1, 100, common.Hash{})); err == nil || !strings.Contains(err.Error(), "cap") {
		t.Fatalf("expected cap failure, got %v", err)
	}
	w.close()
	if err := recordNativeJournalFailure(dir, "cap reached; backfill required"); err != nil {
		t.Fatal(err)
	}
	_, gaps, err := ReadNativeJournal(dir)
	if err != nil || len(gaps) != 1 || !strings.Contains(gaps[0], "backfill") {
		t.Fatalf("gaps=%v err=%v", gaps, err)
	}
}
