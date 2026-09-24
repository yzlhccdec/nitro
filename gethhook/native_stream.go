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
	"net"
	"os"
	"sync/atomic"
	"time"

	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/log"
	"github.com/offchainlabs/nitro/arbos"
)

const (
	nativeStreamHello            = "NTXR1" // receiver -> producer, proves a live downstream consumer
	nativeStreamReady            = "NTXS1" // producer -> receiver, followed by 16-byte process epoch
	nativeStreamQueue            = 64
	nativeStreamRetry            = 100 * time.Millisecond
	nativeStreamDialTimeout      = 25 * time.Millisecond
	nativeStreamHandshakeTimeout = 250 * time.Millisecond
	nativeStreamWriteTimeout     = 50 * time.Millisecond
)

// NativeStream is an optional, lossy live notification channel. The receiver
// first sends NTXR1; the producer replies NTXS1 plus a random 16-byte process
// epoch. Each subsequent message is a 4-byte big-endian length followed by
// one complete NTX1 payload. The socket must be independent from RHSD. There
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
			if err := s.handshake(candidate); err != nil {
				_ = candidate.Close()
				warn(err)
				continue
			}
			connection = candidate
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
			payload, err := EncodeNativeFrame(frame)
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

func (s *NativeStream) handshake(connection net.Conn) error {
	if err := connection.SetDeadline(time.Now().Add(nativeStreamHandshakeTimeout)); err != nil {
		return err
	}
	var hello [len(nativeStreamHello)]byte
	if _, err := io.ReadFull(connection, hello[:]); err != nil {
		return err
	}
	if string(hello[:]) != nativeStreamHello {
		return fmt.Errorf("invalid native stream receiver hello %q", hello)
	}
	var ready [len(nativeStreamReady) + 16]byte
	copy(ready[:], nativeStreamReady)
	copy(ready[len(nativeStreamReady):], s.epoch[:])
	if _, err := io.Copy(connection, bytes.NewReader(ready[:])); err != nil {
		return err
	}
	return connection.SetDeadline(time.Time{})
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
func readNativeStreamFrame(connection net.Conn) (NativeJournalFrame, error) {
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
	return DecodeNativeFrame(payload)
}

// PerformNativeReceiverHandshake is used by independent Unix/TLS receivers.
func PerformNativeReceiverHandshake(connection net.Conn) ([16]byte, error) {
	var epoch [16]byte
	if err := connection.SetDeadline(time.Now().Add(nativeStreamHandshakeTimeout)); err != nil {
		return epoch, err
	}
	if _, err := io.Copy(connection, bytes.NewReader([]byte(nativeStreamHello))); err != nil {
		return epoch, err
	}
	var ready [len(nativeStreamReady) + 16]byte
	if _, err := io.ReadFull(connection, ready[:]); err != nil {
		return epoch, err
	}
	if !bytes.Equal(ready[:len(nativeStreamReady)], []byte(nativeStreamReady)) {
		return epoch, errors.New("invalid native stream producer ready")
	}
	copy(epoch[:], ready[len(nativeStreamReady):])
	return epoch, connection.SetDeadline(time.Time{})
}

func ReadNativeStreamFrame(connection net.Conn) (NativeJournalFrame, error) {
	return readNativeStreamFrame(connection)
}
