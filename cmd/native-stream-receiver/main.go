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
	From         string   `json:"from"`
	To           string   `json:"to"`
	ValueWei     string   `json:"value_wei"`
	Kind         uint8    `json:"kind"`
	TraceAddress []uint16 `json:"trace_address"`
}
type outputCall struct {
	TraceAddress []uint16 `json:"trace_address"`
	From         string   `json:"from"`
	To           string   `json:"to"`
	Kind         uint8    `json:"kind"`
	ValueWei     string   `json:"value_wei"`
	InputHex     string   `json:"input_hex"`
	Success      bool     `json:"success"`
}
type outputLogScope struct {
	LogIndex     uint32   `json:"log_index"`
	TraceAddress []uint16 `json:"trace_address"`
}
type outputTx struct {
	Hash          string           `json:"hash"`
	Index         uint32           `json:"transaction_index"`
	Type          uint8            `json:"type"`
	FactsComplete bool             `json:"facts_complete"`
	Transfers     []outputTransfer `json:"transfers"`
	Calls         []outputCall     `json:"calls"`
	LogScopes     []outputLogScope `json:"log_scopes"`
}
type outputBlock struct {
	Schema            string     `json:"schema"`
	FrameVersion      uint8      `json:"frame_version"`
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
	mu              sync.Mutex
	protocolVersion uint8
	current         net.Conn
	epoch           [16]byte
	lastSeq         uint64
	lastNumber      uint64
	lastHash        common.Hash
	output          io.Writer
	fatal           chan error
	failed          atomic.Bool
}

func (r *receiver) accept(conn net.Conn) {
	if r.failed.Load() {
		_ = conn.Close()
		return
	}
	var epoch [16]byte
	var err error
	if r.protocolVersion == 2 {
		epoch, err = gethhook.PerformNativeReceiverHandshakeV2(conn)
	} else {
		epoch, err = gethhook.PerformNativeReceiverHandshake(conn)
	}
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
	version := r.protocolVersion
	if version == 0 {
		version = 1
	} // tests and explicit legacy receiver construction
	for {
		frame, err := gethhook.ReadNativeStreamFrameVersion(conn, version)
		if err != nil {
			fmt.Fprintln(os.Stderr, "native receiver disconnected:", err)
			return
		}
		r.mu.Lock()
		if r.current != conn {
			r.mu.Unlock()
			return
		}
		block := outputBlock{Schema: fmt.Sprintf("native_stream_v%d", version), FrameVersion: version, Epoch: hex.EncodeToString(epoch[:]), Sequence: frame.Sequence, Number: frame.BlockNumber, Hash: frame.BlockHash.Hex(), Parent: frame.ParentHash.Hex(), Complete: frame.Complete, ContinuityUnknown: r.lastSeq == 0, Transactions: make([]outputTx, 0, len(frame.Txs))}
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
		for i, tx := range frame.Txs {
			factsComplete := version == 2 && tx.FactsComplete
			out := outputTx{Hash: tx.TxHash.Hex(), Index: uint32(i), Type: tx.TxType, FactsComplete: factsComplete, Transfers: make([]outputTransfer, 0, len(tx.Transfers)), Calls: make([]outputCall, 0), LogScopes: make([]outputLogScope, 0)}
			for _, tr := range tx.Transfers {
				var path []uint16 // V1 has no trace path; JSON null means unknown.
				if factsComplete {
					path = append([]uint16{}, tr.TraceAddress...)
				}
				out.Transfers = append(out.Transfers, outputTransfer{From: tr.From.Hex(), To: tr.To.Hex(), ValueWei: tr.Value.String(), Kind: tr.Kind, TraceAddress: path})
			}
			if factsComplete {
				for _, call := range tx.Calls {
					value := "0"
					if call.Value != nil {
						value = call.Value.String()
					}
					out.Calls = append(out.Calls, outputCall{TraceAddress: append([]uint16{}, call.TraceAddress...), From: call.From.Hex(), To: call.To.Hex(), Kind: call.Kind, ValueWei: value, InputHex: "0x" + hex.EncodeToString(call.Input), Success: call.Success})
				}
				for _, scope := range tx.LogScopes {
					out.LogScopes = append(out.LogScopes, outputLogScope{LogIndex: scope.Index, TraceAddress: append([]uint16{}, scope.TraceAddress...)})
				}
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
	protocol := flag.String("protocol", "v2", "native stream protocol: v2 includes call/log facts; v1 is legacy transfers only")
	flag.Parse()
	var version uint8
	switch *protocol {
	case "v1":
		version = 1
	case "v2":
		version = 2
	default:
		fmt.Fprintln(os.Stderr, "invalid -protocol (expected v1 or v2)")
		os.Exit(2)
	}
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
	r := &receiver{output: os.Stdout, fatal: make(chan error, 1), protocolVersion: version}
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
