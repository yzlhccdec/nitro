//go:build !wasm

package gethhook

import (
	"bytes"
	"context"
	"crypto/rand"
	"encoding/binary"
	"errors"
	"fmt"
	"io"
	"math/big"
	"net"
	"os"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/log"
	"github.com/offchainlabs/nitro/arbos"
)

const (
	nativeStreamHello            = "NTXR1" // legacy receiver requests NTX1
	nativeStreamReady            = "NTXS1"
	nativeStreamHelloV2          = "NTXR2" // facts-aware receiver requests NTX2
	nativeStreamReadyV2          = "NTXS2"
	nativeStreamQueue            = 64
	nativeStreamRetry            = 100 * time.Millisecond
	nativeStreamDialTimeout      = 25 * time.Millisecond
	nativeStreamHandshakeTimeout = 250 * time.Millisecond
	nativeStreamWriteTimeout     = 50 * time.Millisecond
	nativeStreamFactMagic        = "NTX2"
)

// NativeStream is an optional, lossy live notification channel. The receiver
// first sends NTXR1; the producer replies NTXS1 plus a random 16-byte process
// epoch. Each subsequent message is a 4-byte big-endian length followed by
// one complete NTX2 payload (an NTX1 frame plus versioned call/log facts).
// The socket must be independent from RHSD. There
// is no ACK or replay: Sequence, BlockNumber and ParentHash expose gaps/reorgs.
// NTX1 has no checksum; integrity relies on local Unix sockets and the
// independently configured TLS 1.3 tunnel (AEAD). The decoder checks the
// 4 MiB length cap, frame identity, completeness, and exact payload length.
type NativeStream struct {
	path       string
	epoch      [16]byte
	queue      chan NativeJournalFrame
	connected  atomic.Bool
	forceClose atomic.Bool
	next       atomic.Uint64
	dropped    atomic.Uint64
}

func NewNativeStreamFromEnv() (*NativeStream, error) {
	path := os.Getenv("NITRO_NATIVE_STREAM_SOCKET")
	if path == "" {
		return nil, nil
	}
	if path == os.Getenv("NITRO_STORAGE_DELTA_SOCKET") {
		return nil, errors.New("native stream socket must differ from RHSD socket")
	}
	return NewNativeStream(path)
}

func NewNativeStream(path string) (*NativeStream, error) {
	if path == "" {
		return nil, errors.New("native stream socket is empty")
	}
	s := &NativeStream{path: path, queue: make(chan NativeJournalFrame, nativeStreamQueue)}
	if _, err := rand.Read(s.epoch[:]); err != nil {
		return nil, err
	}
	return s, nil
}

func (s *NativeStream) Connected() bool { return s != nil && s.connected.Load() }
func (s *NativeStream) Dropped() uint64 {
	if s == nil {
		return 0
	}
	return s.dropped.Load()
}

// Publish is called only after canonical block insertion. It performs no I/O
// and never waits for a slow receiver. A full queue fences the connection.
func (s *NativeStream) Publish(block *types.Block, evidence *arbos.NativeBlockEvidence) bool {
	if s == nil || !s.connected.Load() || block == nil {
		return false
	}
	seq := s.next.Add(1)
	if evidence == nil || evidence.BlockHash != block.Hash() {
		s.dropped.Add(1)
		s.forceClose.Store(true)
		s.connected.Store(false)
		return false
	}
	frame := NativeJournalFrame{Sequence: seq, BlockNumber: block.NumberU64(), ParentHash: block.ParentHash(), BlockHash: block.Hash(), Complete: true, Txs: evidence.Txs}
	select {
	case s.queue <- frame:
		return true
	default:
		s.dropped.Add(1)
		s.forceClose.Store(true)
		s.connected.Store(false)
		return false
	}
}

func (s *NativeStream) discardQueued() {
	for {
		select {
		case <-s.queue:
		default:
			return
		}
	}
}

func (s *NativeStream) Run(ctx context.Context) {
	ticker := time.NewTicker(nativeStreamRetry)
	defer ticker.Stop()
	var connection net.Conn
	var factsVersion bool
	defer func() {
		s.connected.Store(false)
		if connection != nil {
			_ = connection.Close()
		}
	}()
	var lastWarning time.Time
	warn := func(err error) {
		if time.Since(lastWarning) >= 10*time.Second {
			log.Warn("native stream disconnected", "err", err)
			lastWarning = time.Now()
		}
	}
	for {
		if connection == nil {
			s.connected.Store(false)
			s.discardQueued()
			select {
			case <-ctx.Done():
				return
			case <-ticker.C:
			}
			candidate, err := net.DialTimeout("unix", s.path, nativeStreamDialTimeout)
			if err != nil {
				continue
			}
			v2, err := s.handshake(candidate)
			if err != nil {
				_ = candidate.Close()
				warn(err)
				continue
			}
			connection = candidate
			factsVersion = v2
			s.discardQueued() // no pre-handshake frames belong to this session
			s.forceClose.Store(false)
			s.connected.Store(true)
		}
		select {
		case <-ctx.Done():
			return
		case frame := <-s.queue:
			if s.forceClose.Load() {
				s.connected.Store(false)
				_ = connection.Close()
				connection = nil
				continue
			}
			var payload []byte
			var err error
			if factsVersion {
				payload, err = EncodeNativeStreamFrame(frame)
			} else {
				payload, err = EncodeNativeFrame(frame)
			}
			if err == nil {
				err = writeNativeStreamFrame(connection, payload)
			}
			if err != nil {
				s.dropped.Add(1)
				s.connected.Store(false)
				_ = connection.Close()
				connection = nil
				warn(err)
			}
		case <-ticker.C:
			if s.forceClose.Load() {
				s.connected.Store(false)
				_ = connection.Close()
				connection = nil
			}
		}
	}
}

func (s *NativeStream) handshake(connection net.Conn) (bool, error) {
	if err := connection.SetDeadline(time.Now().Add(nativeStreamHandshakeTimeout)); err != nil {
		return false, err
	}
	var hello [len(nativeStreamHello)]byte
	if _, err := io.ReadFull(connection, hello[:]); err != nil {
		return false, err
	}
	readyTag := nativeStreamReady
	v2 := false
	switch string(hello[:]) {
	case nativeStreamHello:
	case nativeStreamHelloV2:
		readyTag = nativeStreamReadyV2
		v2 = true
	default:
		return false, fmt.Errorf("invalid native stream receiver hello %q", hello)
	}
	var ready [len(nativeStreamReady) + 16]byte
	copy(ready[:], readyTag)
	copy(ready[len(nativeStreamReady):], s.epoch[:])
	if _, err := io.Copy(connection, bytes.NewReader(ready[:])); err != nil {
		return false, err
	}
	return v2, connection.SetDeadline(time.Time{})
}

func writeNativeStreamFrame(connection net.Conn, payload []byte) error {
	if len(payload) > nativeFrameMax {
		return errors.New("native stream frame too large")
	}
	if err := connection.SetWriteDeadline(time.Now().Add(nativeStreamWriteTimeout)); err != nil {
		return err
	}
	var prefix [4]byte
	binary.BigEndian.PutUint32(prefix[:], uint32(len(payload)))
	buffers := net.Buffers{prefix[:], payload}
	n, err := buffers.WriteTo(connection)
	if err == nil && n != int64(4+len(payload)) {
		return io.ErrShortWrite
	}
	return err
}

// readNativeStreamFrame is shared by the test listener and CLI receiver.
func readNativeStreamFrame(connection net.Conn, expectedVersion uint8) (NativeJournalFrame, error) {
	var prefix [4]byte
	if _, err := io.ReadFull(connection, prefix[:]); err != nil {
		return NativeJournalFrame{}, err
	}
	length := binary.BigEndian.Uint32(prefix[:])
	if length < 87 || length > nativeFrameMax {
		return NativeJournalFrame{}, errors.New("invalid native stream length")
	}
	payload := make([]byte, length)
	if _, err := io.ReadFull(connection, payload); err != nil {
		return NativeJournalFrame{}, err
	}
	isV2 := string(payload[:4]) == nativeStreamFactMagic
	if expectedVersion == 1 && isV2 || expectedVersion == 2 && !isV2 {
		return NativeJournalFrame{}, errors.New("native stream frame version differs from handshake")
	}
	if isV2 {
		return DecodeNativeStreamFrame(payload)
	}
	return DecodeNativeFrame(payload)
}

// EncodeNativeStreamFrame leaves disk NTX1 unchanged. The stream-only NTX2
// envelope is magic(4), NTX1 length(4), NTX1 payload, then one fact section
// per transaction in NTX1 order. Paths are unsigned call-child indices.
func EncodeNativeStreamFrame(f NativeJournalFrame) ([]byte, error) {
	base, err := EncodeNativeFrame(f)
	if err != nil {
		return nil, err
	}
	var b bytes.Buffer
	b.Grow(len(base) + len(f.Txs)*8)
	b.WriteString(nativeStreamFactMagic)
	_ = binary.Write(&b, binary.BigEndian, uint32(len(base)))
	b.Write(base)
	for _, tx := range f.Txs {
		if tx.FactsComplete {
			b.WriteByte(1)
		} else {
			b.WriteByte(0)
		}
		if len(tx.Calls) > 65535 || len(tx.LogScopes) > 65535 {
			return nil, errors.New("too many call facts")
		}
		_ = binary.Write(&b, binary.BigEndian, uint16(len(tx.Transfers)))
		for _, tr := range tx.Transfers {
			if err := writeNativePath(&b, tr.TraceAddress); err != nil {
				return nil, err
			}
		}
		_ = binary.Write(&b, binary.BigEndian, uint16(len(tx.Calls)))
		for _, call := range tx.Calls {
			if err := writeNativePath(&b, call.TraceAddress); err != nil {
				return nil, err
			}
			if len(call.Input) > 324 {
				return nil, errors.New("call input exceeds 324-byte cap")
			}
			b.Write(call.From[:])
			b.Write(call.To[:])
			b.WriteByte(call.Kind)
			if call.Value != nil && (call.Value.Sign() < 0 || call.Value.BitLen() > 256) {
				return nil, errors.New("invalid call value")
			}
			var callValue [32]byte
			if call.Value != nil {
				call.Value.FillBytes(callValue[:])
			}
			b.Write(callValue[:])
			if call.Success {
				b.WriteByte(1)
			} else {
				b.WriteByte(0)
			}
			_ = binary.Write(&b, binary.BigEndian, uint16(len(call.Input)))
			b.Write(call.Input)
		}
		_ = binary.Write(&b, binary.BigEndian, uint16(len(tx.LogScopes)))
		for _, logScope := range tx.LogScopes {
			_ = binary.Write(&b, binary.BigEndian, logScope.Index)
			if err := writeNativePath(&b, logScope.TraceAddress); err != nil {
				return nil, err
			}
		}
		if b.Len() > nativeFrameMax {
			return nil, errors.New("NTX2 frame exceeds 4 MiB")
		}
	}
	return b.Bytes(), nil
}

func writeNativePath(b *bytes.Buffer, path []uint16) error {
	if len(path) > 64 {
		return errors.New("call path exceeds 64 levels")
	}
	b.WriteByte(byte(len(path)))
	for _, child := range path {
		_ = binary.Write(b, binary.BigEndian, child)
	}
	return nil
}

func readNativePath(r *bytes.Reader) ([]uint16, error) {
	count, err := r.ReadByte()
	if err != nil {
		return nil, err
	}
	if count > 64 {
		return nil, errors.New("call path exceeds 64 levels")
	}
	path := make([]uint16, int(count))
	for i := range path {
		if err := binary.Read(r, binary.BigEndian, &path[i]); err != nil {
			return nil, err
		}
	}
	return path, nil
}

func DecodeNativeStreamFrame(payload []byte) (NativeJournalFrame, error) {
	var empty NativeJournalFrame
	if len(payload) < 95 || len(payload) > nativeFrameMax || string(payload[:4]) != nativeStreamFactMagic {
		return empty, errors.New("invalid NTX2 frame")
	}
	baseLen := int(binary.BigEndian.Uint32(payload[4:8]))
	if baseLen < 87 || baseLen > len(payload)-8 {
		return empty, errors.New("invalid NTX2 base length")
	}
	f, err := DecodeNativeFrame(payload[8 : 8+baseLen])
	if err != nil {
		return empty, err
	}
	r := bytes.NewReader(payload[8+baseLen:])
	for i := range f.Txs {
		tx := &f.Txs[i]
		flag, err := r.ReadByte()
		if err != nil {
			return empty, err
		}
		if flag > 1 {
			return empty, errors.New("invalid facts completeness flag")
		}
		tx.FactsComplete = flag == 1
		var transferCount uint16
		if err := binary.Read(r, binary.BigEndian, &transferCount); err != nil {
			return empty, err
		}
		if int(transferCount) != len(tx.Transfers) {
			return empty, errors.New("NTX2 transfer count mismatch")
		}
		for j := range tx.Transfers {
			tx.Transfers[j].TraceAddress, err = readNativePath(r)
			if err != nil {
				return empty, err
			}
		}
		var callCount uint16
		if err := binary.Read(r, binary.BigEndian, &callCount); err != nil {
			return empty, err
		}
		for j := 0; j < int(callCount); j++ {
			var call arbos.NativeCallFact
			call.TraceAddress, err = readNativePath(r)
			if err != nil {
				return empty, err
			}
			if _, err := io.ReadFull(r, call.From[:]); err != nil {
				return empty, err
			}
			if _, err := io.ReadFull(r, call.To[:]); err != nil {
				return empty, err
			}
			call.Kind, err = r.ReadByte()
			if err != nil {
				return empty, err
			}
			var callValue [32]byte
			if _, err := io.ReadFull(r, callValue[:]); err != nil {
				return empty, err
			}
			call.Value = new(big.Int).SetBytes(callValue[:])
			success, err := r.ReadByte()
			if err != nil {
				return empty, err
			}
			if success > 1 {
				return empty, errors.New("invalid call success flag")
			}
			call.Success = success == 1
			var n uint16
			if err := binary.Read(r, binary.BigEndian, &n); err != nil {
				return empty, err
			}
			if n > 324 {
				return empty, errors.New("call input exceeds 324-byte cap")
			}
			call.Input = make([]byte, n)
			if _, err := io.ReadFull(r, call.Input); err != nil {
				return empty, err
			}
			tx.Calls = append(tx.Calls, call)
		}
		var logCount uint16
		if err := binary.Read(r, binary.BigEndian, &logCount); err != nil {
			return empty, err
		}
		for j := 0; j < int(logCount); j++ {
			var scope arbos.NativeLogScope
			if err := binary.Read(r, binary.BigEndian, &scope.Index); err != nil {
				return empty, err
			}
			scope.TraceAddress, err = readNativePath(r)
			if err != nil {
				return empty, err
			}
			tx.LogScopes = append(tx.LogScopes, scope)
		}
	}
	if r.Len() != 0 {
		return empty, errors.New("trailing NTX2 bytes")
	}
	return f, nil
}

// PerformNativeReceiverHandshake is used by independent Unix/TLS receivers.
func PerformNativeReceiverHandshake(connection net.Conn) ([16]byte, error) {
	return performNativeReceiverHandshake(connection, nativeStreamHello, nativeStreamReady)
}

// PerformNativeReceiverHandshakeV2 opts into NTX2 call/log facts. Legacy
// receivers continue to request NTX1 and never see an unknown frame version.
func PerformNativeReceiverHandshakeV2(connection net.Conn) ([16]byte, error) {
	return performNativeReceiverHandshake(connection, nativeStreamHelloV2, nativeStreamReadyV2)
}

func performNativeReceiverHandshake(connection net.Conn, hello, expectedReady string) ([16]byte, error) {
	var epoch [16]byte
	if err := connection.SetDeadline(time.Now().Add(nativeStreamHandshakeTimeout)); err != nil {
		return epoch, err
	}
	if _, err := io.Copy(connection, bytes.NewReader([]byte(hello))); err != nil {
		return epoch, err
	}
	var ready [len(nativeStreamReady) + 16]byte
	if _, err := io.ReadFull(connection, ready[:]); err != nil {
		return epoch, err
	}
	if !bytes.Equal(ready[:len(nativeStreamReady)], []byte(expectedReady)) {
		return epoch, errors.New("invalid native stream producer ready")
	}
	copy(epoch[:], ready[len(nativeStreamReady):])
	return epoch, connection.SetDeadline(time.Time{})
}

func ReadNativeStreamFrame(connection net.Conn) (NativeJournalFrame, error) {
	return readNativeStreamFrame(connection, 0)
}

// ReadNativeStreamFrameVersion rejects frames that do not match the negotiated
// handshake version. A receiver must not infer frame version from its request.
func ReadNativeStreamFrameVersion(connection net.Conn, version uint8) (NativeJournalFrame, error) {
	if version != 1 && version != 2 {
		return NativeJournalFrame{}, errors.New("unsupported native stream version")
	}
	return readNativeStreamFrame(connection, version)
}
