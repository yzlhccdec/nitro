package arbos

import (
	"math/big"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/tracing"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
)

// NativeTransfer is an EVM value movement, excluding the outer transaction
// value and ArbOS bookkeeping such as gas, deposits, and refunds.
type NativeTransfer struct {
	From, To common.Address
	Value    *big.Int
	Kind     byte
}

type NativeTxEvidence struct {
	TxHash    common.Hash
	TxType    uint8
	Transfers []NativeTransfer
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
	candidate int
	debit     bool
	credit    bool
}

type nativeEvent struct {
	transfer NativeTransfer
	valid    bool
}

type nativeTxCollector struct {
	frames  []nativeFrame
	events  []nativeEvent
	invalid bool
}

func (c *nativeTxCollector) hooks() *tracing.Hooks {
	return &tracing.Hooks{
		OnEnter: func(depth int, kind byte, from, to common.Address, _ []byte, _ uint64, value *big.Int) {
			frame := nativeFrame{depth: depth, start: len(c.events), candidate: -1}
			if depth > 0 && from != to && value != nil && value.Sign() > 0 {
				switch vm.OpCode(kind) {
				case vm.CALL, vm.CREATE, vm.CREATE2:
					frame.candidate = len(c.events)
					c.events = append(c.events, nativeEvent{transfer: NativeTransfer{From: from, To: to, Value: new(big.Int).Set(value), Kind: kind}})
				case vm.SELFDESTRUCT:
					// The opcode transfers before emitting OnEnter. Unlike CALL,
					// Nitro's synthetic MockCall never has this kind.
					c.events = append(c.events, nativeEvent{transfer: NativeTransfer{From: from, To: to, Value: new(big.Int).Set(value), Kind: kind}, valid: true})
				}
			}
			c.frames = append(c.frames, frame)
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
				return
			}
			if frame.candidate >= 0 && frame.debit && frame.credit {
				c.events[frame.candidate].valid = true
			}
		},
	}
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
