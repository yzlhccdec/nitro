//go:build !wasm

package main

import (
	"flag"
	"fmt"
	"os"

	"github.com/offchainlabs/nitro/gethhook"
)

func main() {
	dir := flag.String("dir", "", "NTX1 journal directory")
	flag.Parse()
	if *dir == "" {
		fmt.Fprintln(os.Stderr, "-dir is required")
		os.Exit(2)
	}
	gaps, err := gethhook.WalkCompleteNativeJournal(*dir, func(f gethhook.NativeJournalFrame) error {
		fmt.Printf("block=%d seq=%d hash=%s parent=%s txs=%d\n", f.BlockNumber, f.Sequence, f.BlockHash, f.ParentHash, len(f.Txs))
		for i, tx := range f.Txs {
			for _, tr := range tx.Transfers {
				fmt.Printf("  tx_index=%d tx=%s type=%d from=%s to=%s value_wei=%s call_kind=0x%x\n", i, tx.TxHash, tx.TxType, tr.From, tr.To, tr.Value, tr.Kind)
			}
		}
		return nil
	})
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(1)
	}
	for _, gap := range gaps {
		fmt.Fprintln(os.Stderr, "GAP:", gap)
	}
	if len(gaps) != 0 {
		os.Exit(3)
	}
}
