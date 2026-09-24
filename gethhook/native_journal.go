// Copyright 2026 Offchain Labs, Inc.
//go:build !wasm

package gethhook

import (
	"bytes"
	"context"
	"encoding/binary"
	"errors"
	"fmt"
	"hash/crc32"
	"io"
	"math/big"
	"os"
	"path/filepath"
	"sort"
	"strings"
	"sync/atomic"
	"syscall"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/log"
	"github.com/offchainlabs/nitro/arbos"
)

const (
	nativeJournalMagic = "NTX1"
	nativeFrameMax     = 4 << 20
	nativeSegmentMax   = 16 << 20
	nativeJournalMax   = 256 << 20
	nativeQueueMax     = 128
)

type NativeJournalFrame struct {
	Sequence    uint64
	BlockNumber uint64
	ParentHash  common.Hash
	BlockHash   common.Hash
	Complete    bool
	Txs         []arbos.NativeTxEvidence
}

type nativePendingFrame struct {
	frame NativeJournalFrame
}

// NativeJournal is opt-in via RH_NATIVE_JOURNAL_DIR. Canonical block production
// only enqueues a pointer; the disk writer is bounded and never blocks the node.
// A dropped block is visible as a sequence or block-number gap in later frames.
type NativeJournal struct {
	dir       string
	queue     chan nativePendingFrame
	lastFrame NativeJournalFrame // fixed at startup, before Run
	next      atomic.Uint64
	dropped   atomic.Uint64
	failed    atomic.Bool
	lock      *os.File
}

func recordNativeJournalFailure(dir, reason string) error {
	path := filepath.Join(dir, "native-gap.txt")
	tmp := path + ".tmp"
	f, err := os.OpenFile(tmp, os.O_CREATE|os.O_TRUNC|os.O_WRONLY, 0o600)
	if err != nil {
		return err
	}
	if _, err = f.WriteString(reason + "\n"); err == nil {
		err = f.Sync()
	}
	closeErr := f.Close()
	if err == nil {
		err = closeErr
	}
	if err != nil {
		return err
	}
	if err = os.Rename(tmp, path); err != nil {
		return err
	}
	d, err := os.Open(dir)
	if err != nil {
		return err
	}
	err = d.Sync()
	_ = d.Close()
	return err
}

func OpenNativeJournalFromEnv() (*NativeJournal, error) {
	dir := os.Getenv("RH_NATIVE_JOURNAL_DIR")
	if dir == "" {
		return nil, nil
	}
	return OpenNativeJournal(dir)
}

func OpenNativeJournal(dir string) (*NativeJournal, error) {
	if err := os.MkdirAll(dir, 0o700); err != nil {
		return nil, err
	}
	if err := os.Chmod(dir, 0o700); err != nil {
		return nil, err
	}
	lock, err := os.OpenFile(filepath.Join(dir, "native.lock"), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(lock.Fd()), syscall.LOCK_EX|syscall.LOCK_NB); err != nil {
		_ = lock.Close()
		return nil, fmt.Errorf("native journal already active: %w", err)
	}
	var last NativeJournalFrame
	_, gaps, err := scanNativeJournal(dir, true, func(frame NativeJournalFrame) error { last = frame; return nil })
	if err != nil {
		_ = lock.Close()
		return nil, err
	}
	for _, gap := range gaps {
		log.Warn("native journal has gap; trace backfill required", "gap", gap)
	}
	j := &NativeJournal{dir: dir, queue: make(chan nativePendingFrame, nativeQueueMax), lastFrame: last, lock: lock}
	j.next.Store(last.Sequence)
	return j, nil
}

func (j *NativeJournal) Close() error {
	if j == nil || j.lock == nil {
		return nil
	}
	return j.lock.Close()
}

// CheckCanonicalHead makes a crash window at the journal tail visible before
// new blocks arrive. An empty journal starts at the current head by design.
func (j *NativeJournal) CheckCanonicalHead(number uint64, hash common.Hash) error {
	if j == nil || j.lastFrame.Sequence == 0 {
		return nil
	}
	last := j.lastFrame
	if number == last.BlockNumber && hash == last.BlockHash {
		return nil
	}
	reason := fmt.Sprintf("journal tail block %d %s differs from canonical head %d %s; inspect/replay missing or reorged blocks", last.BlockNumber, last.BlockHash, number, hash)
	return recordNativeJournalFailure(j.dir, reason)
}

// Submit must be called only after canonical insertion. It does not wait for
// disk I/O. Gaps are expected if the bounded queue fills or the journal fails.
func (j *NativeJournal) Submit(block *types.Block, evidence *arbos.NativeBlockEvidence) bool {
	if j == nil || block == nil {
		return false
	}
	seq := j.next.Add(1)
	if evidence == nil || evidence.BlockHash != block.Hash() {
		j.dropped.Add(1)
		return false
	}
	frame := NativeJournalFrame{Sequence: seq, BlockNumber: block.NumberU64(), ParentHash: block.ParentHash(), BlockHash: block.Hash(), Complete: true, Txs: evidence.Txs}
	if j.failed.Load() {
		j.dropped.Add(1)
		return false
	}
	select {
	case j.queue <- nativePendingFrame{frame: frame}:
		return true
	default:
		j.dropped.Add(1)
		return false
	}
}

func (j *NativeJournal) Dropped() uint64 { return j.dropped.Load() }

// Run fsyncs each complete frame before taking the next queued block. A disk
// error disables writes and leaves an explicit sequence/height gap for replay.
func (j *NativeJournal) Run(ctx context.Context) {
	defer j.Close()
	w, err := openNativeWriter(j.dir)
	if err != nil {
		j.failed.Store(true)
		_ = recordNativeJournalFailure(j.dir, err.Error())
		log.Error("native journal disabled", "err", err)
		return
	}
	defer w.close()
	var reportedDrops uint64
	for {
		select {
		case p := <-j.queue:
			if drops := j.dropped.Load(); drops > reportedDrops {
				reportedDrops = drops
				if err := recordNativeJournalFailure(j.dir, fmt.Sprintf("queue overflow: %d canonical blocks dropped; backfill required", drops)); err != nil {
					log.Error("native journal gap marker failed", "err", err)
				}
			}
			if err := w.append(p.frame); err != nil {
				j.failed.Store(true)
				if markErr := recordNativeJournalFailure(j.dir, fmt.Sprintf("write failed at block %d seq %d: %v", p.frame.BlockNumber, p.frame.Sequence, err)); markErr != nil {
					log.Error("native journal gap marker failed", "err", markErr)
				}
				log.Error("native journal disabled; frames require block trace backfill", "block", p.frame.BlockNumber, "seq", p.frame.Sequence, "err", err)
				return
			}
		case <-ctx.Done():
			// Drain the bounded queue on orderly shutdown. A hard crash is recovered
			// by scanning the last complete, checksummed frame at next startup.
			for {
				select {
				case p := <-j.queue:
					if err := w.append(p.frame); err != nil {
						_ = recordNativeJournalFailure(j.dir, fmt.Sprintf("shutdown write failed at block %d seq %d: %v", p.frame.BlockNumber, p.frame.Sequence, err))
						log.Error("native journal shutdown write failed", "err", err)
						return
					}
				default:
					return
				}
			}
		}
	}
}

type nativeWriter struct {
	dir   string
	file  *os.File
	index int
	size  int64
	total int64
}

func nativeSegments(dir string) ([]string, error) {
	paths, err := filepath.Glob(filepath.Join(dir, "native-*.ntx"))
	if err != nil {
		return nil, err
	}
	sort.Strings(paths)
	return paths, nil
}

func openNativeWriter(dir string) (*nativeWriter, error) {
	paths, err := nativeSegments(dir)
	if err != nil {
		return nil, err
	}
	w := &nativeWriter{dir: dir}
	for _, p := range paths {
		info, err := os.Stat(p)
		if err != nil {
			return nil, err
		}
		w.total += info.Size()
	}
	if len(paths) != 0 {
		last := paths[len(paths)-1]
		if _, err := fmt.Sscanf(filepath.Base(last), "native-%06d.ntx", &w.index); err != nil {
			return nil, err
		}
		w.file, err = os.OpenFile(last, os.O_RDWR|os.O_APPEND, 0o600)
		if err != nil {
			return nil, err
		}
		info, err := w.file.Stat()
		if err != nil {
			w.file.Close()
			return nil, err
		}
		w.size = info.Size()
	}
	return w, nil
}

func (w *nativeWriter) close() {
	if w.file != nil {
		_ = w.file.Close()
	}
}

func (w *nativeWriter) append(frame NativeJournalFrame) error {
	payload, err := EncodeNativeFrame(frame)
	if err != nil {
		return err
	}
	recordSize := int64(8 + len(payload))
	for w.total+recordSize > nativeJournalMax {
		paths, err := nativeSegments(w.dir)
		if err != nil {
			return err
		}
		if len(paths) < 2 {
			return fmt.Errorf("journal cap %d bytes reached without prunable segment", nativeJournalMax)
		}
		oldest := paths[0]
		info, err := os.Stat(oldest)
		if err != nil {
			return err
		}
		if err := recordNativeJournalFailure(w.dir, fmt.Sprintf("retention cap pruned %s; history gap before seq %d", filepath.Base(oldest), frame.Sequence)); err != nil {
			return err
		}
		if err := os.Remove(oldest); err != nil {
			return err
		}
		d, err := os.Open(w.dir)
		if err != nil {
			return err
		}
		err = d.Sync()
		_ = d.Close()
		if err != nil {
			return err
		}
		w.total -= info.Size()
	}
	if w.file == nil || w.size+recordSize > nativeSegmentMax {
		if w.file != nil {
			if err := w.file.Close(); err != nil {
				return err
			}
			w.file = nil
			w.index++
		}
		name := filepath.Join(w.dir, fmt.Sprintf("native-%06d.ntx", w.index))
		w.file, err = os.OpenFile(name, os.O_CREATE|os.O_EXCL|os.O_WRONLY, 0o600)
		if err != nil {
			return err
		}
		w.size = 0
		d, err := os.Open(w.dir)
		if err != nil {
			return err
		}
		err = d.Sync()
		_ = d.Close()
		if err != nil {
			return err
		}
	}
	var header [8]byte
	binary.BigEndian.PutUint32(header[:4], uint32(len(payload)))
	binary.BigEndian.PutUint32(header[4:], crc32.ChecksumIEEE(payload))
	if _, err := w.file.Write(header[:]); err != nil {
		return err
	}
	if _, err := w.file.Write(payload); err != nil {
		return err
	}
	if err := w.file.Sync(); err != nil {
		return err
	}
	w.size += recordSize
	w.total += recordSize
	return nil
}

// EncodeNativeFrame is the stable, standalone NTX1 payload. Every canonical
// block, including a block with zero native movements, has one complete frame.
func EncodeNativeFrame(f NativeJournalFrame) ([]byte, error) {
	if !f.Complete || f.Sequence == 0 || f.BlockHash == (common.Hash{}) {
		return nil, errors.New("incomplete native frame")
	}
	var b bytes.Buffer
	b.Grow(96 + len(f.Txs)*40)
	b.WriteString(nativeJournalMagic)
	_ = binary.Write(&b, binary.BigEndian, f.Sequence)
	_ = binary.Write(&b, binary.BigEndian, f.BlockNumber)
	b.Write(f.ParentHash[:])
	b.Write(f.BlockHash[:])
	b.WriteByte(1)
	if len(f.Txs) > 65535 {
		return nil, errors.New("too many txs")
	}
	_ = binary.Write(&b, binary.BigEndian, uint16(len(f.Txs)))
	for _, tx := range f.Txs {
		b.Write(tx.TxHash[:])
		b.WriteByte(tx.TxType)
		if len(tx.Transfers) > 65535 {
			return nil, errors.New("too many transfers")
		}
		_ = binary.Write(&b, binary.BigEndian, uint16(len(tx.Transfers)))
		for _, tr := range tx.Transfers {
			if tr.Value == nil || tr.Value.Sign() <= 0 || tr.Value.BitLen() > 256 {
				return nil, errors.New("invalid native amount")
			}
			b.Write(tr.From[:])
			b.Write(tr.To[:])
			b.WriteByte(tr.Kind)
			amount := tr.Value.FillBytes(make([]byte, 32))
			b.Write(amount)
		}
		if b.Len() > nativeFrameMax {
			return nil, errors.New("native frame exceeds 4 MiB")
		}
	}
	return b.Bytes(), nil
}

func DecodeNativeFrame(payload []byte) (NativeJournalFrame, error) {
	var f NativeJournalFrame
	if len(payload) < 87 || len(payload) > nativeFrameMax || string(payload[:4]) != nativeJournalMagic {
		return f, errors.New("invalid NTX1 frame")
	}
	r := bytes.NewReader(payload[4:])
	_ = binary.Read(r, binary.BigEndian, &f.Sequence)
	_ = binary.Read(r, binary.BigEndian, &f.BlockNumber)
	_, _ = io.ReadFull(r, f.ParentHash[:])
	_, _ = io.ReadFull(r, f.BlockHash[:])
	var complete [1]byte
	_, _ = io.ReadFull(r, complete[:])
	if complete[0] != 1 {
		return f, errors.New("incomplete NTX1 frame")
	}
	f.Complete = true
	var n uint16
	_ = binary.Read(r, binary.BigEndian, &n)
	f.Txs = make([]arbos.NativeTxEvidence, 0, int(n))
	for i := 0; i < int(n); i++ {
		var tx arbos.NativeTxEvidence
		if _, err := io.ReadFull(r, tx.TxHash[:]); err != nil {
			return f, err
		}
		var kind [1]byte
		if _, err := io.ReadFull(r, kind[:]); err != nil {
			return f, err
		}
		tx.TxType = kind[0]
		var m uint16
		if err := binary.Read(r, binary.BigEndian, &m); err != nil {
			return f, err
		}
		for j := 0; j < int(m); j++ {
			var tr arbos.NativeTransfer
			if _, err := io.ReadFull(r, tr.From[:]); err != nil {
				return f, err
			}
			if _, err := io.ReadFull(r, tr.To[:]); err != nil {
				return f, err
			}
			if _, err := io.ReadFull(r, kind[:]); err != nil {
				return f, err
			}
			tr.Kind = kind[0]
			var amount [32]byte
			if _, err := io.ReadFull(r, amount[:]); err != nil {
				return f, err
			}
			tr.Value = new(big.Int).SetBytes(amount[:])
			if tr.Value.Sign() <= 0 {
				return f, errors.New("zero native amount")
			}
			tx.Transfers = append(tx.Transfers, tr)
		}
		f.Txs = append(f.Txs, tx)
	}
	if r.Len() != 0 || f.Sequence == 0 || f.BlockHash == (common.Hash{}) {
		return f, errors.New("invalid NTX1 trailer or identity")
	}
	return f, nil
}

// ReadNativeJournal is the small/offline convenience API. Node startup uses the
// streaming scanner directly, so a full 256 MiB journal is never retained in
// memory during startup.
func ReadNativeJournal(dir string) ([]NativeJournalFrame, []string, error) {
	lock, err := lockNativeJournalOffline(dir)
	if err != nil {
		return nil, nil, err
	}
	defer lock.Close()
	var frames []NativeJournalFrame
	_, gaps, err := scanNativeJournal(dir, false, func(frame NativeJournalFrame) error {
		frames = append(frames, frame)
		return nil
	})
	return frames, gaps, err
}

func WalkNativeJournal(dir string, accept func(NativeJournalFrame) error) ([]string, error) {
	lock, err := lockNativeJournalOffline(dir)
	if err != nil {
		return nil, err
	}
	defer lock.Close()
	_, gaps, err := scanNativeJournal(dir, false, accept)
	return gaps, err
}

// WalkCompleteNativeJournal holds the offline lock across both scans. It does
// not emit any transfer if a gap or reorg is present anywhere in the journal.
func WalkCompleteNativeJournal(dir string, accept func(NativeJournalFrame) error) ([]string, error) {
	lock, err := lockNativeJournalOffline(dir)
	if err != nil {
		return nil, err
	}
	defer lock.Close()
	_, gaps, err := scanNativeJournal(dir, false, func(NativeJournalFrame) error { return nil })
	if err != nil || len(gaps) != 0 {
		return gaps, err
	}
	_, gaps, err = scanNativeJournal(dir, false, accept)
	return gaps, err
}

func lockNativeJournalOffline(dir string) (*os.File, error) {
	f, err := os.OpenFile(filepath.Join(dir, "native.lock"), os.O_CREATE|os.O_RDWR, 0o600)
	if err != nil {
		return nil, err
	}
	if err := syscall.Flock(int(f.Fd()), syscall.LOCK_SH|syscall.LOCK_NB); err != nil {
		_ = f.Close()
		return nil, fmt.Errorf("native journal is active; stop writer before offline read: %w", err)
	}
	return f, nil
}

// scanNativeJournal validates every record. A torn final record is truncated
// only after a persistent gap marker is committed, so recovery is visible even
// if no further block is ever appended.
func scanNativeJournal(dir string, recoverTail bool, accept func(NativeJournalFrame) error) (NativeJournalFrame, []string, error) {
	var last NativeJournalFrame
	var seen bool
	var gaps []string
	paths, err := nativeSegments(dir)
	if err != nil {
		return last, nil, err
	}
	if marker, err := os.ReadFile(filepath.Join(dir, "native-gap.txt")); err == nil {
		gaps = append(gaps, strings.TrimSpace(string(marker)))
	} else if !os.IsNotExist(err) {
		return last, nil, err
	}
	if len(paths) > 0 && filepath.Base(paths[0]) != "native-000000.ntx" {
		gaps = append(gaps, "journal starts after segment 0")
	}
	for pi, p := range paths {
		mode := os.O_RDONLY
		if recoverTail {
			mode = os.O_RDWR
		}
		file, err := os.OpenFile(p, mode, 0)
		if err != nil {
			return last, nil, err
		}
		var offset int64
		for {
			var hdr [8]byte
			n, e := io.ReadFull(file, hdr[:])
			if e == io.EOF {
				break
			}
			if e != nil {
				if pi != len(paths)-1 {
					file.Close()
					return last, nil, fmt.Errorf("truncated nonfinal segment %s", p)
				}
				reason := fmt.Sprintf("truncated final header at %s:%d (%d bytes)", p, offset, n)
				if recoverTail {
					if err := recordNativeJournalFailure(dir, reason); err != nil {
						file.Close()
						return last, nil, err
					}
				}
				gaps = append(gaps, reason)
				if recoverTail {
					if err := file.Truncate(offset); err != nil {
						file.Close()
						return last, nil, err
					}
				}
				break
			}
			length := binary.BigEndian.Uint32(hdr[:4])
			if length < 87 || length > nativeFrameMax {
				file.Close()
				return last, nil, fmt.Errorf("invalid record size at %s:%d", p, offset)
			}
			payload := make([]byte, int(length))
			_, e = io.ReadFull(file, payload)
			if e != nil {
				if pi != len(paths)-1 {
					file.Close()
					return last, nil, fmt.Errorf("truncated nonfinal segment %s", p)
				}
				reason := fmt.Sprintf("truncated final payload at %s:%d", p, offset)
				if recoverTail {
					if err := recordNativeJournalFailure(dir, reason); err != nil {
						file.Close()
						return last, nil, err
					}
				}
				gaps = append(gaps, reason)
				if recoverTail {
					if err := file.Truncate(offset); err != nil {
						file.Close()
						return last, nil, err
					}
				}
				break
			}
			if crc32.ChecksumIEEE(payload) != binary.BigEndian.Uint32(hdr[4:]) {
				file.Close()
				return last, nil, fmt.Errorf("CRC mismatch at %s:%d", p, offset)
			}
			f, e := DecodeNativeFrame(payload)
			if e != nil {
				file.Close()
				return last, nil, fmt.Errorf("%s:%d: %w", p, offset, e)
			}
			if seen {
				if f.Sequence <= last.Sequence {
					file.Close()
					return last, nil, fmt.Errorf("nonmonotonic sequence at %s:%d", p, offset)
				}
				if f.Sequence != last.Sequence+1 {
					gaps = append(gaps, fmt.Sprintf("sequence gap %d to %d", last.Sequence, f.Sequence))
				}
				if f.BlockNumber > last.BlockNumber+1 {
					gaps = append(gaps, fmt.Sprintf("block gap %d to %d", last.BlockNumber, f.BlockNumber))
				}
				if f.BlockNumber == last.BlockNumber+1 && f.ParentHash != last.BlockHash {
					gaps = append(gaps, fmt.Sprintf("parent mismatch at %d", f.BlockNumber))
				}
				if f.BlockNumber <= last.BlockNumber {
					gaps = append(gaps, fmt.Sprintf("canonical head rewound from %d to %d; apply block-hash reorg", last.BlockNumber, f.BlockNumber))
				}
			}
			if !seen && f.Sequence > 1 {
				gaps = append(gaps, fmt.Sprintf("journal starts at sequence %d", f.Sequence))
			}
			if err := accept(f); err != nil {
				file.Close()
				return last, nil, err
			}
			last = f
			seen = true
			offset += 8 + int64(length)
		}
		if err := file.Close(); err != nil {
			return last, nil, err
		}
	}
	return last, gaps, nil
}
