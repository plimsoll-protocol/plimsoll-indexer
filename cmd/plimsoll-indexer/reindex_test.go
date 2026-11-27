package main

import "testing"

func TestRunReindexCommandValidatesFlags(t *testing.T) {
	for name, args := range map[string][]string{
		"missing from ledger": nil,
		"zero from ledger":    {"--from-ledger", "0"},
		"outside uint32":      {"--from-ledger", "4294967296"},
	} {
		t.Run(name, func(t *testing.T) {
			if code := runReindexCommand(args); code != 1 {
				t.Fatalf("exit code = %d, want 1", code)
			}
		})
	}
}
