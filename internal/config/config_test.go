package config

import "testing"

func TestSetExtraTokensIgnoresBuiltinAndLegacyContracts(t *testing.T) {
	c := DefaultConfig()
	c.SetExtraTokens([]string{"", c.KoinContract, c.VhpContract, c.OldKoinContract, c.OldVhpContract, "1TKNxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"})
	if len(c.ExtraTokens) != 1 || !c.IsExtra("1TKNxxxxxxxxxxxxxxxxxxxxxxxxxxxxx") {
		t.Fatalf("extra tokens = %v", c.ExtraTokens)
	}
	for _, a := range []string{c.KoinContract, c.VhpContract, c.OldKoinContract, c.OldVhpContract} {
		if c.IsExtra(a) {
			t.Fatalf("%s must never be an extra token", a)
		}
	}
}
