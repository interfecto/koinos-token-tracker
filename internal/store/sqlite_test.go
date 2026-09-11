package store

import (
	"path/filepath"
	"testing"
)

func seedTransfers(t *testing.T, s *SQLiteStore, koin, vhp string) {
	t.Helper()
	ins := func(h uint64, token, typ string) {
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
	if err := s.SetSyncState(50, "head"); err != nil {
		t.Fatal(err)
	}
}

func TestGetRecentTransfersFiltered(t *testing.T) {
	const koin, vhp = "1KNxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx", "1VHPyyyyyyyyyyyyyyyyyyyyyyyyyyyyy"
	s, err := Open(filepath.Join(t.TempDir(), "t.db"), koin, vhp)
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	seedTransfers(t, s, koin, vhp)

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

	got, err = s.GetRecentTransfersFiltered(vhp, "", 3)
	if err != nil {
		t.Fatal(err)
	}
	// height 50 has a burn and a transfer for VHP (transfer inserted last → higher id first)
	if len(got) != 3 || got[0].Height != 50 || got[0].EventType != "transfer" || got[1].EventType != "burn" || got[2].Height != 49 {
		t.Fatalf("vhp newest first: got %+v", got)
	}

	got, err = s.GetRecentTransfersFiltered("1NPExxxxxxxxxxxxxxxxxxxxxxxxxxxxx", "transfer", 5)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 0 {
		t.Fatalf("unknown token: expected no rows, got %d", len(got))
	}

	if _, err := s.GetRecentTransfersFiltered("", "transfer", 5); err == nil {
		t.Fatal("type without token must be rejected")
	}

	// the window: only blocks >= head - window are searched
	old := RecentFilterWindow
	RecentFilterWindow = 15
	defer func() { RecentFilterWindow = old }()
	got, err = s.GetRecentTransfersFiltered(koin, "transfer", 10)
	if err != nil {
		t.Fatal(err)
	}
	if len(got) != 2 || got[0].Height != 50 || got[1].Height != 40 { // 30 is below 50-15
		t.Fatalf("window: got %+v", got)
	}

	all, err := s.GetRecentTransfers(4)
	if err != nil {
		t.Fatal(err)
	}
	if len(all) != 4 || all[0].Height != 50 {
		t.Fatalf("unfiltered feed changed: %+v", all)
	}
}

func TestOpenReadOnly(t *testing.T) {
	const koin, vhp = "1KNxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx", "1VHPyyyyyyyyyyyyyyyyyyyyyyyyyyyyy"
	path := filepath.Join(t.TempDir(), "ro.db")
	w, err := Open(path, koin, vhp)
	if err != nil {
		t.Fatal(err)
	}
	seedTransfers(t, w, koin, vhp)

	ro, err := OpenReadOnly(path, koin, vhp)
	if err != nil {
		t.Fatal(err)
	}
	defer ro.Close()
	got, err := ro.GetRecentTransfersFiltered(koin, "transfer", 2)
	if err != nil || len(got) != 2 {
		t.Fatalf("read through ro store: %v %+v", err, got)
	}
	if err := ro.InsertTransfer(99, "tx", koin, "a", "b", "1", "transfer", 1); err == nil {
		t.Fatal("write through the read-only store must fail")
	}
	w.Close()

	if _, err := OpenReadOnly(filepath.Join(t.TempDir(), "missing.db"), koin, vhp); err == nil {
		t.Fatal("missing database must not be created by the read-only opener")
	}
}

func TestMigrateAddsBackfillColumns(t *testing.T) {
	path := filepath.Join(t.TempDir(), "m.db")
	s, err := Open(path, "1KNxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx", "1VHPyyyyyyyyyyyyyyyyyyyyyyyyyyyyy")
	if err != nil {
		t.Fatal(err)
	}
	// simulate a database created by the first build of the feature
	for _, col := range []string{"verified", "mismatches", "failures"} {
		if _, err := s.db.Exec("ALTER TABLE token_backfill DROP COLUMN " + col); err != nil {
			t.Fatal(err)
		}
	}
	s.Close()
	s, err = Open(path, "1KNxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx", "1VHPyyyyyyyyyyyyyyyyyyyyyyyyyyyyy")
	if err != nil {
		t.Fatal(err)
	}
	defer s.Close()
	if err := s.UpsertBackfill(&Backfill{Token: "1TKNxxxxxxxxxxxxxxxxxxxxxxxxxxxxx", NextSeq: 3, CutoffHeight: 9, Done: true, Verified: true, Mismatches: 1, Failures: 2}); err != nil {
		t.Fatalf("columns not migrated: %v", err)
	}
	b, err := s.GetBackfill("1TKNxxxxxxxxxxxxxxxxxxxxxxxxxxxxx")
	if err != nil || b == nil || !b.Verified || b.Mismatches != 1 || b.Failures != 2 {
		t.Fatalf("round trip %+v %v", b, err)
	}
}
