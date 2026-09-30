package ledger

import (
	"context"
	"errors"
	"fmt"
	"math/rand/v2"
	"os"
	"path/filepath"
	"sync"
	"testing"
	"time"
)

func mustOpen(t *testing.T, l *Ledger, name string, overdraft bool) AccountView {
	t.Helper()
	a, err := l.OpenAccount(OpenAccountRequest{Name: name, Currency: "ZAR", AllowOverdraft: overdraft})
	if err != nil {
		t.Fatalf("open %s: %v", name, err)
	}
	return a
}

func fund(t *testing.T, l *Ledger, from, to string, amount int64) {
	t.Helper()
	if _, _, err := l.Transfer(context.Background(), TransferRequest{FromAccountID: from, ToAccountID: to, AmountMinor: amount, Currency: "ZAR"}); err != nil {
		t.Fatalf("fund: %v", err)
	}
}

func TestTransferMovesMoneyBothLegs(t *testing.T) {
	l := New()
	bank := mustOpen(t, l, "funding", true)
	alice := mustOpen(t, l, "alice", false)
	bob := mustOpen(t, l, "bob", false)
	fund(t, l, bank.ID, alice.ID, 10_000)

	if _, _, err := l.Transfer(context.Background(), TransferRequest{FromAccountID: alice.ID, ToAccountID: bob.ID, AmountMinor: 2_550, Currency: "ZAR"}); err != nil {
		t.Fatal(err)
	}
	a, _ := l.Account(alice.ID)
	b, _ := l.Account(bob.ID)
	if a.BalanceMinor != 7_450 || b.BalanceMinor != 2_550 {
		t.Fatalf("got alice=%d bob=%d", a.BalanceMinor, b.BalanceMinor)
	}
	if tb := l.TrialBalance()["ZAR"]; tb != 0 {
		t.Fatalf("trial balance %d, want 0", tb)
	}
}

func TestValidationErrors(t *testing.T) {
	l := New()
	a := mustOpen(t, l, "a", false)
	b := mustOpen(t, l, "b", false)
	usd, _ := l.OpenAccount(OpenAccountRequest{Name: "usd", Currency: "USD"})
	cases := []struct {
		name string
		req  TransferRequest
		want error
	}{
		{"zero", TransferRequest{FromAccountID: a.ID, ToAccountID: b.ID, AmountMinor: 0, Currency: "ZAR"}, ErrInvalidAmount},
		{"negative", TransferRequest{FromAccountID: a.ID, ToAccountID: b.ID, AmountMinor: -5, Currency: "ZAR"}, ErrInvalidAmount},
		{"too large", TransferRequest{FromAccountID: a.ID, ToAccountID: b.ID, AmountMinor: MaxTransferMinor + 1, Currency: "ZAR"}, ErrInvalidAmount},
		{"same account", TransferRequest{FromAccountID: a.ID, ToAccountID: a.ID, AmountMinor: 1, Currency: "ZAR"}, ErrSameAccount},
		{"bad currency", TransferRequest{FromAccountID: a.ID, ToAccountID: b.ID, AmountMinor: 1, Currency: "zar"}, ErrInvalidCurrency},
		{"mismatch", TransferRequest{FromAccountID: a.ID, ToAccountID: usd.ID, AmountMinor: 1, Currency: "ZAR"}, ErrCurrencyMismatch},
		{"insufficient", TransferRequest{FromAccountID: a.ID, ToAccountID: b.ID, AmountMinor: 1, Currency: "ZAR"}, ErrInsufficientFunds},
		{"unknown", TransferRequest{FromAccountID: "acc_nope", ToAccountID: b.ID, AmountMinor: 1, Currency: "ZAR"}, ErrNotFound},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if _, _, err := l.Transfer(context.Background(), tc.req); !errors.Is(err, tc.want) {
				t.Fatalf("got %v, want %v", err, tc.want)
			}
		})
	}
	if n := len(l.events); n != 3 {
		t.Fatalf("rejected transfers must not be journaled; events=%d", n)
	}
}

func TestIdempotentReplayAndConflict(t *testing.T) {
	l := New()
	bank := mustOpen(t, l, "funding", true)
	a := mustOpen(t, l, "a", false)
	req := TransferRequest{FromAccountID: bank.ID, ToAccountID: a.ID, AmountMinor: 500, Currency: "ZAR", IdempotencyKey: "k1"}

	t1, replayed, err := l.Transfer(context.Background(), req)
	if err != nil || replayed {
		t.Fatalf("first: err=%v replayed=%v", err, replayed)
	}
	t2, replayed, err := l.Transfer(context.Background(), req)
	if err != nil || !replayed || t2.ID != t1.ID {
		t.Fatalf("replay: err=%v replayed=%v id=%s want %s", err, replayed, t2.ID, t1.ID)
	}
	if v, _ := l.Account(a.ID); v.BalanceMinor != 500 {
		t.Fatalf("replay double-posted: balance=%d", v.BalanceMinor)
	}
	req.AmountMinor = 501
	if _, _, err := l.Transfer(context.Background(), req); !errors.Is(err, ErrIdempotencyConflict) {
		t.Fatalf("got %v, want conflict", err)
	}
}

func TestConcurrentDuplicatesPostOnce(t *testing.T) {
	l := New()
	bank := mustOpen(t, l, "funding", true)
	a := mustOpen(t, l, "a", false)
	req := TransferRequest{FromAccountID: bank.ID, ToAccountID: a.ID, AmountMinor: 100, Currency: "ZAR", IdempotencyKey: "dup"}

	var wg sync.WaitGroup
	ids := make(chan string, 50)
	for range 50 {
		wg.Add(1)
		go func() {
			defer wg.Done()
			tr, _, err := l.Transfer(context.Background(), req)
			if err != nil {
				t.Error(err)
				return
			}
			ids <- tr.ID
		}()
	}
	wg.Wait()
	close(ids)
	first := ""
	for id := range ids {
		if first == "" {
			first = id
		} else if id != first {
			t.Fatalf("duplicate requests produced different transfers %s and %s", first, id)
		}
	}
	if v, _ := l.Account(a.ID); v.BalanceMinor != 100 {
		t.Fatalf("balance=%d, want 100", v.BalanceMinor)
	}
}

func TestFailedAttemptIsNotCached(t *testing.T) {
	l := New()
	bank := mustOpen(t, l, "funding", true)
	a := mustOpen(t, l, "a", false)
	b := mustOpen(t, l, "b", false)
	req := TransferRequest{FromAccountID: a.ID, ToAccountID: b.ID, AmountMinor: 100, Currency: "ZAR", IdempotencyKey: "retry-me"}
	if _, _, err := l.Transfer(context.Background(), req); !errors.Is(err, ErrInsufficientFunds) {
		t.Fatalf("got %v", err)
	}
	fund(t, l, bank.ID, a.ID, 100)
	if _, replayed, err := l.Transfer(context.Background(), req); err != nil || replayed {
		t.Fatalf("retry after funding: err=%v replayed=%v", err, replayed)
	}
}

// TestConcurrentTransfersConserveMoney hammers random transfers, including
// opposing pairs that would deadlock without ordered locking. Run with -race.
func TestConcurrentTransfersConserveMoney(t *testing.T) {
	l := New()
	bank := mustOpen(t, l, "funding", true)
	const n = 8
	ids := make([]string, n)
	for i := range n {
		ids[i] = mustOpen(t, l, fmt.Sprintf("acct-%d", i), false).ID
		fund(t, l, bank.ID, ids[i], 1_000)
	}

	done := make(chan struct{})
	var wg sync.WaitGroup
	for w := range 16 {
		wg.Add(1)
		go func(seed uint64) {
			defer wg.Done()
			r := rand.New(rand.NewPCG(seed, seed))
			for range 500 {
				from, to := ids[r.IntN(n)], ids[r.IntN(n)]
				if from == to {
					continue
				}
				_, _, err := l.Transfer(context.Background(), TransferRequest{FromAccountID: from, ToAccountID: to, AmountMinor: int64(r.IntN(300) + 1), Currency: "ZAR"})
				if err != nil && !errors.Is(err, ErrInsufficientFunds) {
					t.Error(err)
				}
			}
		}(uint64(w))
	}
	go func() { wg.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(20 * time.Second):
		t.Fatal("deadlock: transfers did not finish")
	}

	if tb := l.TrialBalance()["ZAR"]; tb != 0 {
		t.Fatalf("money created or destroyed: trial balance %d", tb)
	}
	var total int64
	for _, id := range ids {
		v, _ := l.Account(id)
		if v.BalanceMinor < 0 {
			t.Fatalf("%s overdrawn: %d", id, v.BalanceMinor)
		}
		total += v.BalanceMinor
	}
	if total != n*1_000 {
		t.Fatalf("customer total %d, want %d", total, n*1_000)
	}
	if err := l.VerifyChain(); err != nil {
		t.Fatal(err)
	}
}

func TestStatementPaginationAndRunningBalance(t *testing.T) {
	l := New()
	bank := mustOpen(t, l, "funding", true)
	a := mustOpen(t, l, "a", false)
	for i := 1; i <= 5; i++ {
		fund(t, l, bank.ID, a.ID, int64(i*100))
	}
	p1, _ := l.Statement(a.ID, 0, 3)
	if len(p1) != 3 || p1[2].BalanceAfterMinor != 600 {
		t.Fatalf("page1=%+v", p1)
	}
	p2, _ := l.Statement(a.ID, p1[2].Seq, 3)
	if len(p2) != 2 || p2[1].BalanceAfterMinor != 1_500 || p2[0].CounterpartyID != bank.ID {
		t.Fatalf("page2=%+v", p2)
	}
}

func TestTamperingIsDetected(t *testing.T) {
	l := New()
	bank := mustOpen(t, l, "funding", true)
	a := mustOpen(t, l, "a", false)
	fund(t, l, bank.ID, a.ID, 100)
	l.events[2].Transfer.AmountMinor = 1_000_000 // an insider edits history
	if err := l.VerifyChain(); !errors.Is(err, ErrJournalCorrupt) {
		t.Fatalf("got %v, want ErrJournalCorrupt", err)
	}
}

func TestFileJournalSurvivesRestartAndTornWrite(t *testing.T) {
	path := filepath.Join(t.TempDir(), "journal.jsonl")
	j, evs, err := OpenFileJournal(path)
	if err != nil || len(evs) != 0 {
		t.Fatalf("open: %v %d", err, len(evs))
	}
	l := New(WithJournal(j))
	bank := mustOpen(t, l, "funding", true)
	a := mustOpen(t, l, "a", false)
	req := TransferRequest{FromAccountID: bank.ID, ToAccountID: a.ID, AmountMinor: 4_200, Currency: "ZAR", IdempotencyKey: "pay-1"}
	first, _, err := l.Transfer(context.Background(), req)
	if err != nil {
		t.Fatal(err)
	}
	j.Close()

	// Simulate a crash mid-write: a partial line with no newline.
	f, _ := os.OpenFile(path, os.O_APPEND|os.O_WRONLY, 0o600)
	f.WriteString(`{"seq":4,"type":"transfer_po`)
	f.Close()

	j2, evs, err := OpenFileJournal(path)
	if err != nil {
		t.Fatal(err)
	}
	defer j2.Close()
	l2, err := Replay(evs, WithJournal(j2))
	if err != nil {
		t.Fatal(err)
	}
	if v, _ := l2.Account(a.ID); v.BalanceMinor != 4_200 {
		t.Fatalf("balance after restart %d", v.BalanceMinor)
	}
	got, replayed, err := l2.Transfer(context.Background(), req)
	if err != nil || !replayed || got.ID != first.ID {
		t.Fatalf("idempotency lost across restart: err=%v replayed=%v", err, replayed)
	}
	if _, _, err := l2.Transfer(context.Background(), TransferRequest{FromAccountID: bank.ID, ToAccountID: a.ID, AmountMinor: 1, Currency: "ZAR"}); err != nil {
		t.Fatal(err)
	}
	if err := l2.VerifyChain(); err != nil {
		t.Fatalf("chain broken after restart+append: %v", err)
	}
}

func TestPruneIdempotency(t *testing.T) {
	now := time.Date(2026, 1, 1, 0, 0, 0, 0, time.UTC)
	l := New(WithClock(func() time.Time { return now }))
	bank := mustOpen(t, l, "funding", true)
	a := mustOpen(t, l, "a", false)
	l.Transfer(context.Background(), TransferRequest{FromAccountID: bank.ID, ToAccountID: a.ID, AmountMinor: 1, Currency: "ZAR", IdempotencyKey: "old"})
	now = now.Add(25 * time.Hour)
	if n := l.PruneIdempotency(24 * time.Hour); n != 1 {
		t.Fatalf("pruned %d, want 1", n)
	}
}

func BenchmarkTransfer(b *testing.B) {
	l := New()
	bank, _ := l.OpenAccount(OpenAccountRequest{Name: "f", Currency: "ZAR", AllowOverdraft: true})
	a, _ := l.OpenAccount(OpenAccountRequest{Name: "a", Currency: "ZAR"})
	req := TransferRequest{FromAccountID: bank.ID, ToAccountID: a.ID, AmountMinor: 1, Currency: "ZAR"}
	b.ResetTimer()
	for b.Loop() {
		l.Transfer(context.Background(), req)
	}
}
