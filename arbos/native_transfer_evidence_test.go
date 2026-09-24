package arbos

import (
	"errors"
	"math/big"
	"testing"

	"github.com/ethereum/go-ethereum/common"
	"github.com/ethereum/go-ethereum/core/tracing"
	"github.com/ethereum/go-ethereum/core/types"
	"github.com/ethereum/go-ethereum/core/vm"
	"github.com/ethereum/go-ethereum/core/vm/runtime"
)

func TestNativeTransferCollectorRevertAndKinds(t *testing.T) {
	a, b, c := common.HexToAddress("0x1"), common.HexToAddress("0x2"), common.HexToAddress("0x3")
	collector := new(nativeTxCollector)
	h := collector.hooks()
	enter := func(depth int, op vm.OpCode, from, to common.Address, value int64) {
		h.OnEnter(depth, byte(op), from, to, nil, 0, big.NewInt(value))
	}
	balance := func(from, to common.Address, value int64) {
		h.OnBalanceChange(from, big.NewInt(value), big.NewInt(0), tracing.BalanceChangeTransfer)
		h.OnBalanceChange(to, big.NewInt(0), big.NewInt(value), tracing.BalanceChangeTransfer)
	}
	exit := func(depth int, failed bool) {
		if failed {
			h.OnExit(depth, nil, 0, errors.New("reverted"), true)
		} else {
			h.OnExit(depth, nil, 0, nil, false)
		}
	}
	enter(0, vm.CALL, a, b, 3) // outer tx value is intentionally excluded
	enter(1, vm.CALL, b, c, 10)
	balance(b, c, 10)
	enter(2, vm.CALL, c, a, 4)
	balance(c, a, 4)
	exit(2, false)
	exit(1, true)                   // parent revert removes its successful descendant
	enter(1, vm.CALLCODE, b, c, 99) // CALLCODE does not move funds to c
	exit(1, false)
	enter(1, vm.DELEGATECALL, b, c, 99)
	exit(1, false)
	enter(1, vm.CALL, b, c, 7)
	balance(b, c, 7)
	exit(1, false)
	enter(1, vm.SELFDESTRUCT, b, a, 5)
	exit(1, false)
	exit(0, false)
	got := collector.transfers(&types.Receipt{Status: types.ReceiptStatusSuccessful})
	if len(got) != 2 || got[0].Value.Int64() != 7 || got[1].Value.Int64() != 5 {
		t.Fatalf("unexpected transfers: %+v", got)
	}
	if got[0].From != b || got[0].To != c || got[1].Kind != byte(vm.SELFDESTRUCT) {
		t.Fatalf("incorrect transfer endpoints or kind: %+v", got)
	}
}

func TestNativeTransferCollectorIgnoresMockCallBeforeInsufficientBalance(t *testing.T) {
	a, b := common.HexToAddress("0x1"), common.HexToAddress("0x2")
	collector := new(nativeTxCollector)
	h := collector.hooks()
	h.OnEnter(0, byte(vm.CALL), a, b, nil, 0, big.NewInt(0))
	// Nitro TransferBalance emits this successful synthetic frame before its
	// balance check. Insufficient balance then returns without any state change.
	h.OnEnter(1, byte(vm.CALL), a, b, nil, 0, big.NewInt(10))
	h.OnExit(1, nil, 0, nil, false)
	h.OnExit(0, nil, 0, nil, false)
	if got := collector.transfers(&types.Receipt{Status: types.ReceiptStatusSuccessful}); len(got) != 0 {
		t.Fatalf("synthetic call became transfer: %+v", got)
	}
}

func TestNativeTransferCollectorExcludesArbOSSystemTxs(t *testing.T) {
	for _, txType := range []uint8{types.ArbitrumDepositTxType, types.ArbitrumInternalTxType, types.ArbitrumSubmitRetryableTxType} {
		if nativeUserEvmTx(txType) {
			t.Fatalf("ArbOS system transaction type %x enabled", txType)
		}
	}
	if !nativeUserEvmTx(types.DynamicFeeTxType) || !nativeUserEvmTx(types.ArbitrumLegacyTxType) ||
		!nativeUserEvmTx(types.ArbitrumUnsignedTxType) || !nativeUserEvmTx(types.ArbitrumContractTxType) || !nativeUserEvmTx(types.ArbitrumRetryTxType) {
		t.Fatal("ordinary EVM transaction disabled")
	}
}

func TestNativeTransferCollectorRequiresBothExactTransferBalances(t *testing.T) {
	a, b := common.HexToAddress("0x1"), common.HexToAddress("0x2")
	c := new(nativeTxCollector)
	h := c.hooks()
	h.OnEnter(0, byte(vm.CALL), a, b, nil, 0, big.NewInt(0))
	h.OnEnter(1, byte(vm.CALL), a, b, nil, 0, big.NewInt(10))
	h.OnBalanceChange(a, big.NewInt(10), big.NewInt(0), tracing.BalanceDecreaseGasBuy)
	h.OnBalanceChange(b, big.NewInt(0), big.NewInt(10), tracing.BalanceChangeTransfer)
	h.OnExit(1, nil, 0, nil, false)
	h.OnExit(0, nil, 0, nil, false)
	if got := c.transfers(&types.Receipt{Status: types.ReceiptStatusSuccessful}); len(got) != 0 {
		t.Fatalf("one-sided/fee balance change confirmed call: %+v", got)
	}
}

func TestNativeTransferCollectorFailedRootAndReceipt(t *testing.T) {
	a, b := common.HexToAddress("0x1"), common.HexToAddress("0x2")
	for _, failedRoot := range []bool{false, true} {
		collector := new(nativeTxCollector)
		h := collector.hooks()
		h.OnEnter(0, byte(vm.CALL), a, b, nil, 0, big.NewInt(0))
		h.OnEnter(1, byte(vm.CALL), a, b, nil, 0, big.NewInt(10))
		h.OnBalanceChange(a, big.NewInt(10), big.NewInt(0), tracing.BalanceChangeTransfer)
		h.OnBalanceChange(b, big.NewInt(0), big.NewInt(10), tracing.BalanceChangeTransfer)
		h.OnExit(1, nil, 0, nil, false)
		if failedRoot {
			h.OnExit(0, nil, 0, errors.New("reverted"), true)
		} else {
			h.OnExit(0, nil, 0, nil, false)
		}
		status := types.ReceiptStatusSuccessful
		if !failedRoot {
			status = types.ReceiptStatusFailed
		}
		if got := collector.transfers(&types.Receipt{Status: status}); len(got) != 0 {
			t.Fatalf("failed root/receipt kept transfers: %+v", got)
		}
	}
}

// Same EVM bytecode and fresh state on both paths. This benchmark includes
// the geth interpreter's per-opcode tracer branch, not merely hook callbacks.
func BenchmarkNativeTransferHookExecution(b *testing.B) {
	code := make([]byte, 0, 769)
	for i := 0; i < 256; i++ {
		code = append(code, byte(vm.PUSH1), 1, byte(vm.POP))
	}
	code = append(code, byte(vm.STOP))
	for _, enabled := range []bool{false, true} {
		name := "off"
		if enabled {
			name = "on"
		}
		b.Run(name, func(b *testing.B) {
			b.ReportAllocs()
			for i := 0; i < b.N; i++ {
				cfg := &runtime.Config{GasLimit: 1_000_000}
				if enabled {
					cfg.EVMConfig.Tracer = new(nativeTxCollector).hooks()
				}
				if _, _, err := runtime.Execute(code, nil, cfg); err != nil {
					b.Fatal(err)
				}
			}
		})
	}
}
