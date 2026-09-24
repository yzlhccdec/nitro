//go:build !wasm

package main

import (
	"bytes"
	"encoding/hex"
	"encoding/json"
	"flag"
	"fmt"
	"io"
	"net"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"

	"github.com/ethereum/go-ethereum/common"
	"github.com/offchainlabs/nitro/gethhook"
)

type outputTransfer struct {
	From     string `json:"from"`
	To       string `json:"to"`
	ValueWei string `json:"value_wei"`
	Kind     uint8  `json:"kind"`
}
type outputTx struct {
	Hash      string           `json:"hash"`
	Type      uint8            `json:"type"`
	Transfers []outputTransfer `json:"transfers"`
}
type outputBlock struct {
	Epoch             string     `json:"epoch"`
	Sequence          uint64     `json:"seq"`
	Number            uint64     `json:"block_number"`
	Hash              string     `json:"block_hash"`
	Parent            string     `json:"parent_hash"`
	Complete          bool       `json:"complete"`
	ContinuityUnknown bool       `json:"continuity_unknown,omitempty"`
	Gap               string     `json:"gap,omitempty"`
	Transactions      []outputTx `json:"transactions"`
}

type receiver struct {
	mu         sync.Mutex
	current    net.Conn
	epoch      [16]byte
	lastSeq    uint64
	lastNumber uint64
	lastHash   common.Hash
	output     io.Writer
	fatal      chan error
	failed     atomic.Bool
}

func (r *receiver) accept(conn net.Conn) {
	if r.failed.Load() {
		_ = conn.Close()
		return
	}
	epoch, err := gethhook.PerformNativeReceiverHandshake(conn)
	if err != nil {
		fmt.Fprintln(os.Stderr, "native receiver handshake:", err)
		_ = conn.Close()
		return
	}
	r.mu.Lock()
	if r.failed.Load() {
		r.mu.Unlock()
		_ = conn.Close()
		return
	}
	if r.current != nil {
		_ = r.current.Close()
	}
	r.current = conn
	r.mu.Unlock()
	go r.consume(conn, epoch)
}

func (r *receiver) reportFatal(err error) {
	if r.failed.CompareAndSwap(false, true) && r.fatal != nil {
		r.fatal <- err
	}
}

func (r *receiver) consume(conn net.Conn, epoch [16]byte) {
	defer conn.Close()
	for {
		frame, err := gethhook.ReadNativeStreamFrame(conn)
		if err != nil {
			fmt.Fprintln(os.Stderr, "native receiver disconnected:", err)
			return
		}
		r.mu.Lock()
		if r.current != conn {
			r.mu.Unlock()
			return
		}
		block := outputBlock{Epoch: hex.EncodeToString(epoch[:]), Sequence: frame.Sequence, Number: frame.BlockNumber, Hash: frame.BlockHash.Hex(), Parent: frame.ParentHash.Hex(), Complete: frame.Complete, ContinuityUnknown: r.lastSeq == 0, Transactions: make([]outputTx, 0, len(frame.Txs))}
		if r.lastSeq != 0 {
			switch {
			case !bytes.Equal(epoch[:], r.epoch[:]):
				block.Gap = "source_epoch_changed"
			case frame.Sequence != r.lastSeq+1:
				block.Gap = fmt.Sprintf("sequence_%d_to_%d", r.lastSeq, frame.Sequence)
			}
			if frame.BlockNumber != r.lastNumber+1 || frame.ParentHash != r.lastHash {
				if block.Gap != "" {
					block.Gap += ";"
				}
				block.Gap += fmt.Sprintf("chain_%d_to_%d_or_reorg", r.lastNumber, frame.BlockNumber)
			}
		}
		for _, tx := range frame.Txs {
			out := outputTx{Hash: tx.TxHash.Hex(), Type: tx.TxType}
			for _, tr := range tx.Transfers {
				out.Transfers = append(out.Transfers, outputTransfer{From: tr.From.Hex(), To: tr.To.Hex(), ValueWei: tr.Value.String(), Kind: tr.Kind})
			}
			block.Transactions = append(block.Transactions, out)
		}
		if err := json.NewEncoder(r.output).Encode(block); err != nil {
			r.mu.Unlock()
			fmt.Fprintln(os.Stderr, "native receiver output:", err)
			r.reportFatal(err)
			return
		}
		r.epoch = epoch
		r.lastSeq = frame.Sequence
		r.lastNumber = frame.BlockNumber
		r.lastHash = frame.BlockHash
		r.mu.Unlock()
	}
}

func main() {
	path := flag.String("listen", "/run/rh-native-transfer/receiver.sock", "Unix socket for the native stream stunnel server")
	flag.Parse()
	if err := os.MkdirAll(filepath.Dir(*path), 0o700); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	if _, err := os.Lstat(*path); err == nil {
		if c, dialErr := net.Dial("unix", *path); dialErr == nil {
			_ = c.Close()
			fmt.Fprintln(os.Stderr, "receiver socket is already active")
			os.Exit(1)
		}
		if err := os.Remove(*path); err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
	} else if !os.IsNotExist(err) {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	listener, err := net.Listen("unix", *path)
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	defer listener.Close()
	defer os.Remove(*path)
	if err := os.Chmod(*path, 0o660); err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	r := &receiver{output: os.Stdout, fatal: make(chan error, 1)}
	go func() {
		err := <-r.fatal
		fmt.Fprintln(os.Stderr, "native receiver fatal output error:", err)
		_ = listener.Close()
	}()
	for {
		conn, err := listener.Accept()
		if err != nil {
			fmt.Fprintln(os.Stderr, err)
			os.Exit(1)
		}
		r.accept(conn)
	}
}
