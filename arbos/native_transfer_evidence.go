package arbos

import (
	"bytes"
	"math/big"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/tracing"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
)

// NativeTransfer is an EVM value movement, excluding the outer transaction
// value and ArbOS bookkeeping such as gas, deposits, and refunds.
type NativeTransfer struct {
	From, To     common.Address
	Value        *big.Int
	Kind         byte
	TraceAddress []uint16
}

// NativeCallFact carries only fixed-size calldata needed to bind a Swap log to
// its PoolKey (or a V2/V3 swap or unwrap). Arbitrary router calldata is omitted.
type NativeCallFact struct {
	TraceAddress []uint16
	From, To     common.Address
	Kind         byte
	Value        *big.Int
	Input        []byte
	Success      bool
}

type NativeLogScope struct {
	Index        uint32
	TraceAddress []uint16
}

type NativeTxEvidence struct {
	TxHash        common.Hash
	TxType        uint8
	Transfers     []NativeTransfer
	FactsComplete bool
	Calls         []NativeCallFact
	LogScopes     []NativeLogScope
}

type NativeBlockEvidence struct {
	BlockHash common.Hash
	Txs       []NativeTxEvidence
}

// NativeTransferCollector is opt-in. It receives a complete block result only
// after ProduceBlockAdvanced has finished. The caller must still publish only
// after canonical block insertion and handle reorgs using BlockHash.
type NativeTransferCollector struct {
	Block *NativeBlockEvidence
}

func nativeUserEvmTx(txType uint8) bool {
	switch txType {
	case types.LegacyTxType, types.AccessListTxType, types.DynamicFeeTxType,
		types.BlobTxType, types.SetCodeTxType, types.ArbitrumLegacyTxType,
		types.ArbitrumUnsignedTxType, types.ArbitrumContractTxType, types.ArbitrumRetryTxType:
		return true
	default:
		return false
	}
}

type nativeFrame struct {
	depth     int
	start     int
	callStart int
	logStart  int
	candidate int
	debit     bool
	credit    bool
	path      [64]uint16
	pathLen   uint8
	children  uint32
	callIndex int
}

type nativeEvent struct {
	transfer NativeTransfer
	valid    bool
}

type nativeTxCollector struct {
	frames        []nativeFrame
	events        []nativeEvent
	calls         []NativeCallFact
	logs          []NativeLogScope
	invalid       bool
	factsOverflow bool
}

const nativeFactLimit = 4096

func nativePath(f *nativeFrame) []uint16 {
	return append([]uint16(nil), f.path[:f.pathLen]...)
}

func nativeFactInput(input []byte) bool {
	if len(input) < 4 {
		return false
	}
	return bytes.Equal(input[:4], []byte{0xf3, 0xcd, 0x91, 0x4c}) || // V4 PoolManager.swap
		bytes.Equal(input[:4], []byte{0x02, 0x2c, 0x0d, 0x9f}) || // V2 pair.swap
		bytes.Equal(input[:4], []byte{0x12, 0x8a, 0xcb, 0x08}) || // V3 pool.swap
		bytes.Equal(input[:4], []byte{0x2e, 0x1a, 0x7d, 0x4d}) || // WETH.withdraw
		bytes.Equal(input[:4], []byte{0x48, 0xc8, 0x94, 0x91}) // V4 unlock
}

func (c *nativeTxCollector) hooks() *tracing.Hooks {
	return &tracing.Hooks{
		OnEnter: func(depth int, kind byte, from, to common.Address, input []byte, _ uint64, value *big.Int) {
			frame := nativeFrame{depth: depth, start: len(c.events), callStart: len(c.calls), logStart: len(c.logs), candidate: -1, callIndex: -1}
			if depth != len(c.frames) || depth > len(frame.path) {
				c.invalid = true
				c.factsOverflow = true
			}
			if depth > 0 && len(c.frames) != 0 {
				parent := &c.frames[len(c.frames)-1]
				if parent.children > 65535 || parent.pathLen >= uint8(len(frame.path)) {
					c.factsOverflow = true
				} else {
					copy(frame.path[:], parent.path[:parent.pathLen])
					frame.path[parent.pathLen] = uint16(parent.children)
					frame.pathLen = parent.pathLen + 1
				}
				parent.children++
			}
			if nativeFactInput(input) && !c.factsOverflow {
				if len(c.calls) >= nativeFactLimit {
					c.factsOverflow = true
				} else {
					max := len(input)
					if max > 324 {
						max = 324
					}
					frame.callIndex = len(c.calls)
					callValue := new(big.Int)
					if value != nil {
						callValue.Set(value)
					}
					c.calls = append(c.calls, NativeCallFact{TraceAddress: nativePath(&frame), From: from, To: to, Kind: kind, Value: callValue, Input: bytes.Clone(input[:max])})
				}
			}
			if depth > 0 && from != to && value != nil && value.Sign() > 0 {
				switch vm.OpCode(kind) {
				case vm.CALL, vm.CREATE, vm.CREATE2:
					frame.candidate = len(c.events)
					c.events = append(c.events, nativeEvent{transfer: NativeTransfer{From: from, To: to, Value: new(big.Int).Set(value), Kind: kind, TraceAddress: nativePath(&frame)}})
				case vm.SELFDESTRUCT:
					// The opcode transfers before emitting OnEnter. Unlike CALL,
					// Nitro's synthetic MockCall never has this kind.
					c.events = append(c.events, nativeEvent{transfer: NativeTransfer{From: from, To: to, Value: new(big.Int).Set(value), Kind: kind, TraceAddress: nativePath(&frame)}, valid: true})
				}
			}
			c.frames = append(c.frames, frame)
		},
		OnLog: func(log *types.Log) {
			if log == nil || len(c.frames) == 0 || c.factsOverflow {
				return
			}
			if len(c.logs) >= nativeFactLimit {
				c.factsOverflow = true
				return
			}
			c.logs = append(c.logs, NativeLogScope{Index: uint32(log.Index), TraceAddress: nativePath(&c.frames[len(c.frames)-1])})
		},
		OnBalanceChange: func(addr common.Address, prev, next *big.Int, reason tracing.BalanceChangeReason) {
			if reason != tracing.BalanceChangeTransfer || len(c.frames) == 0 {
				return
			}
			frame := &c.frames[len(c.frames)-1]
			if frame.candidate < 0 {
				return
			}
			transfer := &c.events[frame.candidate].transfer
			delta := new(big.Int).Sub(prev, next)
			if addr == transfer.From && delta.Cmp(transfer.Value) == 0 {
				frame.debit = true
			} else if addr == transfer.To && delta.Neg(delta).Cmp(transfer.Value) == 0 {
				frame.credit = true
			}
		},
		OnExit: func(depth int, _ []byte, _ uint64, err error, reverted bool) {
			if len(c.frames) == 0 {
				c.invalid = true
				return
			}
			last := len(c.frames) - 1
			frame := c.frames[last]
			c.frames = c.frames[:last]
			if frame.depth != depth {
				c.invalid = true
				return
			}
			if reverted || err != nil {
				c.events = c.events[:frame.start]
				c.calls = c.calls[:frame.callStart]
				c.logs = c.logs[:frame.logStart]
				return
			}
			if frame.callIndex >= 0 {
				c.calls[frame.callIndex].Success = true
			}
			if frame.candidate >= 0 && frame.debit && frame.credit {
				c.events[frame.candidate].valid = true
			}
		},
	}
}

func (c *nativeTxCollector) facts(receipt *types.Receipt) (bool, []NativeCallFact, []NativeLogScope) {
	if receipt == nil || receipt.Status != types.ReceiptStatusSuccessful || len(c.frames) != 0 || c.invalid || c.factsOverflow {
		return false, nil, nil
	}
	return true, c.calls, c.logs
}

func (c *nativeTxCollector) transfers(receipt *types.Receipt) []NativeTransfer {
	if receipt == nil || receipt.Status != types.ReceiptStatusSuccessful || len(c.frames) != 0 || c.invalid {
		return nil
	}
	var transfers []NativeTransfer
	for _, event := range c.events {
		if event.valid {
			transfers = append(transfers, event.transfer)
		}
	}
	return transfers
}
