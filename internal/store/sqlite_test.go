package store

import (
	"path/filepath"
	"testing"
)

func TestGetRecentTransfersFiltered(t *testing.T) {
	const koin, vhp = "1KOINxxxxxxxxxxxxxxxxxxxxxxxxxxxx", "1VHPxxxxxxxxxxxxxxxxxxxxxxxxxxxxx"
	s, err := Open(filepath.Join(t.TempDir(), "t.db"), koin, vhp)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()

	ins := func(h uint64, token, typ string) {
		t.Helper()
		if err := s.InsertTransfer(h, "tx", token, "from", "to", "1", typ, h*1000); err != nil {
			t.Fatal(err)
		}
	}
	// every block: a KOIN mint and a VHP burn; a KOIN transfer every 10th,
	// a VHP transfer every 25th block — the mainnet shape in miniature
	for h := uint64(1); h <= 50; h++ {
		ins(h, koin, "mint")
		ins(h, vhp, "burn")
		if h%10 == 0 {
			ins(h, koin, "transfer")
		}
		if h%25 == 0 {
			ins(h, vhp, "transfer")
		}
	}

	got, err := s.GetRecentTransfersFiltered(koin, "transfer", 3)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 3 || got[0].Height != 50 || got[1].Height != 40 || got[2].Height != 30 {
		t.Fatalf("koin transfers: got %+v", got)
	}
	for _, tr := range got {
		if tr.Token != koin || tr.EventType != "transfer" {
			t.Fatalf("unexpected row %+v", tr)
		}
	}

	got, err = s.GetRecentTransfersFiltered("", "burn", 2)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].Height != 50 || got[1].Height != 49 || got[0].Token != vhp {
		t.Fatalf("burns: got %+v", got)
	}

	got, err = s.GetRecentTransfersFiltered(vhp, "", 3)
	if err != nil {
		t.Fatal(err)
	}
	// height 50 has a burn and a transfer for VHP (transfer inserted last → higher id first)
	if len(got) != 3 || got[0].Height != 50 || got[0].EventType != "transfer" || got[1].EventType != "burn" || got[2].Height != 49 {
		t.Fatalf("vhp newest first: got %+v", got)
	}

	got, err = s.GetRecentTransfersFiltered("1NOPExxxxxxxxxxxxxxxxxxxxxxxxxxxx", "transfer", 5)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("unknown token: expected no rows, got %d", len(got))
	}

	all, err := s.GetRecentTransfers(4)
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 4 || all[0].Height != 50 {
		t.Fatalf("unfiltered feed changed: %+v", all)
	}
}
