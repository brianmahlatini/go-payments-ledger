// Package ledger implements an event-sourced, double-entry ledger.
//
// Design notes:
//   - Money is int64 minor units (cents). Floats never touch balances.
//   - Every state change is an Event in a hash-chained, append-only journal.
//     Balances are derived state and can be rebuilt by replaying the journal.
//   - The journal is written (and fsynced, for FileJournal) BEFORE in-memory
//     state changes: write-ahead logging, so a crash never loses an
//     acknowledged transfer.
//   - Concurrency: each account has its own mutex. A transfer locks both
//     accounts in ascending ID order, so opposing transfers (A->B, B->A)
//     cannot deadlock. Lock order is always: account locks, then journal lock.
//   - Idempotency keys are recorded inside the transfer event, so replays are
//     detected even across process restarts.
package ledger

import (
	"context"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"math"
	"regexp"
	"slices"
	"sync"
	"time"
)

var (
	ErrNotFound            = errors.New("not found")
	ErrInsufficientFunds   = errors.New("insufficient funds")
	ErrCurrencyMismatch    = errors.New("currency mismatch between accounts and transfer")
	ErrSameAccount         = errors.New("source and destination accounts must differ")
	ErrInvalidAmount       = errors.New("amount must be a positive integer of minor units")
	ErrInvalidCurrency     = errors.New("currency must be a 3-letter ISO 4217 code")
	ErrInvalidName         = errors.New("name must be 1-120 characters")
	ErrIdempotencyConflict = errors.New("idempotency key was already used with a different request")
	ErrBalanceOverflow     = errors.New("balance would overflow int64")
	ErrJournalCorrupt      = errors.New("journal hash chain is broken")
)

// MaxTransferMinor caps a single transfer (100 billion major units) as a
// guard against fat-finger and overflow attacks.
const MaxTransferMinor int64 = 100_000_000_000_00

var currencyRe = regexp.MustCompile(`^[A-Z]{3}$`)

type EventType string

const (
	EventAccountOpened EventType = "account_opened"
	EventTransfer      EventType = "transfer_posted"
)

type Account struct {
	ID             string    `json:"id"`
	Name           string    `json:"name"`
	Currency       string    `json:"currency"`
	AllowOverdraft bool      `json:"allow_overdraft"`
	CreatedAt      time.Time `json:"created_at"`
}

type AccountView struct {
	Account
	BalanceMinor int64  `json:"balance_minor"`
	Version      uint64 `json:"version"`
}

type Transfer struct {
	ID             string    `json:"id"`
	FromAccountID  string    `json:"from_account_id"`
	ToAccountID    string    `json:"to_account_id"`
	AmountMinor    int64     `json:"amount_minor"`
	Currency       string    `json:"currency"`
	Reference      string    `json:"reference,omitempty"`
	IdempotencyKey string    `json:"idempotency_key,omitempty"`
	Fingerprint    string    `json:"fingerprint,omitempty"`
	CreatedAt      time.Time `json:"created_at"`
}

type Event struct {
	Seq      uint64    `json:"seq"`
	Type     EventType `json:"type"`
	Account  *Account  `json:"account,omitempty"`
	Transfer *Transfer `json:"transfer,omitempty"`
	PrevHash string    `json:"prev_hash"`
	Hash     string    `json:"hash"`
}

// StatementLine is one leg of a transfer as seen from a single account.
type StatementLine struct {
	Seq               uint64    `json:"seq"`
	TransferID        string    `json:"transfer_id"`
	AmountMinor       int64     `json:"amount_minor"` // signed: negative = debit
	BalanceAfterMinor int64     `json:"balance_after_minor"`
	CounterpartyID    string    `json:"counterparty_account_id"`
	Reference         string    `json:"reference,omitempty"`
	CreatedAt         time.Time `json:"created_at"`
}

type OpenAccountRequest struct {
	Name           string
	Currency       string
	AllowOverdraft bool
}

type TransferRequest struct {
	FromAccountID  string
	ToAccountID    string
	AmountMinor    int64
	Currency       string
	Reference      string
	IdempotencyKey string
}

func (r TransferRequest) validate() error {
	switch {
	case r.AmountMinor <= 0 || r.AmountMinor > MaxTransferMinor:
		return ErrInvalidAmount
	case !currencyRe.MatchString(r.Currency):
		return ErrInvalidCurrency
	case r.FromAccountID == r.ToAccountID:
		return ErrSameAccount
	case len(r.Reference) > 140:
		return fmt.Errorf("reference must be at most 140 characters")
	case len(r.IdempotencyKey) > 255:
		return fmt.Errorf("idempotency key must be at most 255 characters")
	}
	return nil
}

// fingerprint identifies the semantic content of a request so a reused
// idempotency key with a different payload is rejected rather than replayed.
func (r TransferRequest) fingerprint() string {
	h := sha256.New()
	fmt.Fprintf(h, "%s|%s|%d|%s|%s", r.FromAccountID, r.ToAccountID, r.AmountMinor, r.Currency, r.Reference)
	return hex.EncodeToString(h.Sum(nil))
}

// Journal is the durable, append-only event store.
type Journal interface {
	Append(Event) error
}

type ledgerEntry struct {
	seq          uint64
	amount       int64
	balanceAfter int64
}

type accountState struct {
	mu      sync.Mutex
	acct    Account
	balance int64
	version uint64
	entries []ledgerEntry
}

type idemRecord struct {
	fingerprint string
	done        chan struct{}
	transfer    Transfer
	err         error
	createdAt   time.Time
}

type Ledger struct {
	mu       sync.RWMutex // guards the accounts map (not account contents)
	accounts map[string]*accountState

	imu  sync.Mutex // guards idem
	idem map[string]*idemRecord

	jmu      sync.Mutex // guards events, lastHash and journal writes
	events   []Event
	lastHash string
	journal  Journal

	now   func() time.Time
	newID func(prefix string) string
}

type Option func(*Ledger)

func WithJournal(j Journal) Option          { return func(l *Ledger) { l.journal = j } }
func WithClock(now func() time.Time) Option { return func(l *Ledger) { l.now = now } }

func New(opts ...Option) *Ledger {
	l := &Ledger{
		accounts: make(map[string]*accountState),
		idem:     make(map[string]*idemRecord),
		now:      time.Now,
		newID:    randomID,
	}
	for _, o := range opts {
		o(l)
	}
	return l
}

func randomID(prefix string) string {
	var b [12]byte
	if _, err := rand.Read(b[:]); err != nil {
		panic(fmt.Sprintf("crypto/rand failed: %v", err)) // unrecoverable on a healthy OS
	}
	return prefix + hex.EncodeToString(b[:])
}

func (l *Ledger) timestamp() time.Time { return l.now().UTC().Truncate(time.Microsecond) }

func hashEvent(ev Event) (string, error) {
	ev.Hash = ""
	b, err := json.Marshal(ev)
	if err != nil {
		return "", err
	}
	sum := sha256.Sum256(append([]byte(ev.PrevHash+"\n"), b...))
	return hex.EncodeToString(sum[:]), nil
}

// appendLocked assigns sequence and hash, persists, then records the event.
// Caller must hold l.jmu. If persistence fails nothing is recorded.
func (l *Ledger) appendLocked(ev *Event) error {
	ev.Seq = uint64(len(l.events)) + 1
	ev.PrevHash = l.lastHash
	h, err := hashEvent(*ev)
	if err != nil {
		return fmt.Errorf("hash event: %w", err)
	}
	ev.Hash = h
	if l.journal != nil {
		if err := l.journal.Append(*ev); err != nil {
			return fmt.Errorf("journal append: %w", err)
		}
	}
	l.events = append(l.events, *ev)
	l.lastHash = ev.Hash
	return nil
}

func (l *Ledger) OpenAccount(req OpenAccountRequest) (AccountView, error) {
	if n := len(req.Name); n == 0 || n > 120 {
		return AccountView{}, ErrInvalidName
	}
	if !currencyRe.MatchString(req.Currency) {
		return AccountView{}, ErrInvalidCurrency
	}
	acct := Account{
		ID:             l.newID("acc_"),
		Name:           req.Name,
		Currency:       req.Currency,
		AllowOverdraft: req.AllowOverdraft,
		CreatedAt:      l.timestamp(),
	}
	l.mu.Lock()
	defer l.mu.Unlock()
	l.jmu.Lock()
	err := l.appendLocked(&Event{Type: EventAccountOpened, Account: &acct})
	l.jmu.Unlock()
	if err != nil {
		return AccountView{}, err
	}
	l.accounts[acct.ID] = &accountState{acct: acct}
	return AccountView{Account: acct}, nil
}

func (l *Ledger) lookup(id string) (*accountState, error) {
	l.mu.RLock()
	defer l.mu.RUnlock()
	st, ok := l.accounts[id]
	if !ok {
		return nil, fmt.Errorf("account %q: %w", id, ErrNotFound)
	}
	return st, nil
}

func (l *Ledger) Account(id string) (AccountView, error) {
	st, err := l.lookup(id)
	if err != nil {
		return AccountView{}, err
	}
	st.mu.Lock()
	defer st.mu.Unlock()
	return AccountView{Account: st.acct, BalanceMinor: st.balance, Version: st.version}, nil
}

// Transfer posts a transfer. The bool result is true when the response was
// replayed from an earlier request with the same idempotency key.
func (l *Ledger) Transfer(ctx context.Context, req TransferRequest) (Transfer, bool, error) {
	if err := req.validate(); err != nil {
		return Transfer{}, false, err
	}
	if req.IdempotencyKey == "" {
		t, err := l.execute(req, "")
		return t, false, err
	}

	fp := req.fingerprint()
	l.imu.Lock()
	if rec, ok := l.idem[req.IdempotencyKey]; ok {
		l.imu.Unlock()
		if rec.fingerprint != fp {
			return Transfer{}, false, ErrIdempotencyConflict
		}
		select { // a concurrent duplicate waits for the first attempt's outcome
		case <-rec.done:
		case <-ctx.Done():
			return Transfer{}, false, ctx.Err()
		}
		if rec.err != nil {
			return Transfer{}, false, rec.err
		}
		return rec.transfer, true, nil
	}
	rec := &idemRecord{fingerprint: fp, done: make(chan struct{}), createdAt: l.now()}
	l.idem[req.IdempotencyKey] = rec
	l.imu.Unlock()

	t, err := l.execute(req, fp)
	rec.transfer, rec.err = t, err
	if err != nil {
		// Failed attempts are not cached: the client may fix the cause
		// (e.g. fund the account) and retry with the same key.
		l.imu.Lock()
		delete(l.idem, req.IdempotencyKey)
		l.imu.Unlock()
	}
	close(rec.done)
	return t, false, err
}

func (l *Ledger) execute(req TransferRequest, fp string) (Transfer, error) {
	from, err := l.lookup(req.FromAccountID)
	if err != nil {
		return Transfer{}, err
	}
	to, err := l.lookup(req.ToAccountID)
	if err != nil {
		return Transfer{}, err
	}

	first, second := from, to
	if second.acct.ID < first.acct.ID { // IDs are immutable, safe to read unlocked
		first, second = second, first
	}
	first.mu.Lock()
	defer first.mu.Unlock()
	second.mu.Lock()
	defer second.mu.Unlock()

	if from.acct.Currency != req.Currency || to.acct.Currency != req.Currency {
		return Transfer{}, ErrCurrencyMismatch
	}
	if !from.acct.AllowOverdraft && from.balance < req.AmountMinor {
		return Transfer{}, ErrInsufficientFunds
	}
	if from.balance < math.MinInt64+req.AmountMinor || to.balance > math.MaxInt64-req.AmountMinor {
		return Transfer{}, ErrBalanceOverflow
	}

	t := Transfer{
		ID:             l.newID("tr_"),
		FromAccountID:  req.FromAccountID,
		ToAccountID:    req.ToAccountID,
		AmountMinor:    req.AmountMinor,
		Currency:       req.Currency,
		Reference:      req.Reference,
		IdempotencyKey: req.IdempotencyKey,
		Fingerprint:    fp,
		CreatedAt:      l.timestamp(),
	}
	ev := Event{Type: EventTransfer, Transfer: &t}
	l.jmu.Lock()
	err = l.appendLocked(&ev)
	l.jmu.Unlock()
	if err != nil {
		return Transfer{}, err
	}
	applyTransfer(from, to, t.AmountMinor, ev.Seq)
	return t, nil
}

// applyTransfer mutates balances; caller holds both account locks.
func applyTransfer(from, to *accountState, amount int64, seq uint64) {
	from.balance -= amount
	from.version++
	from.entries = append(from.entries, ledgerEntry{seq: seq, amount: -amount, balanceAfter: from.balance})
	to.balance += amount
	to.version++
	to.entries = append(to.entries, ledgerEntry{seq: seq, amount: amount, balanceAfter: to.balance})
}

// Statement returns up to limit lines with seq > afterSeq (keyset pagination).
func (l *Ledger) Statement(accountID string, afterSeq uint64, limit int) ([]StatementLine, error) {
	st, err := l.lookup(accountID)
	if err != nil {
		return nil, err
	}
	if limit <= 0 || limit > 500 {
		limit = 100
	}
	st.mu.Lock()
	i, _ := slices.BinarySearchFunc(st.entries, afterSeq+1, func(e ledgerEntry, s uint64) int {
		return compare(e.seq, s)
	})
	page := slices.Clone(st.entries[i:min(len(st.entries), i+limit)])
	st.mu.Unlock()

	l.jmu.Lock()
	defer l.jmu.Unlock()
	out := make([]StatementLine, 0, len(page))
	for _, e := range page {
		t := l.events[e.seq-1].Transfer
		cp := t.ToAccountID
		if e.amount > 0 {
			cp = t.FromAccountID
		}
		out = append(out, StatementLine{
			Seq: e.seq, TransferID: t.ID, AmountMinor: e.amount, BalanceAfterMinor: e.balanceAfter,
			CounterpartyID: cp, Reference: t.Reference, CreatedAt: t.CreatedAt,
		})
	}
	return out, nil
}

func compare(a, b uint64) int {
	switch {
	case a < b:
		return -1
	case a > b:
		return 1
	}
	return 0
}

// TrialBalance sums balances per currency from a consistent snapshot. In a
// closed double-entry system every currency must sum to zero.
func (l *Ledger) TrialBalance() map[string]int64 {
	l.mu.RLock()
	states := make([]*accountState, 0, len(l.accounts))
	for _, st := range l.accounts {
		states = append(states, st)
	}
	l.mu.RUnlock()
	slices.SortFunc(states, func(a, b *accountState) int {
		switch {
		case a.acct.ID < b.acct.ID:
			return -1
		case a.acct.ID > b.acct.ID:
			return 1
		}
		return 0
	})
	for _, st := range states { // same global lock order as transfers
		st.mu.Lock()
	}
	sums := make(map[string]int64)
	for _, st := range states {
		sums[st.acct.Currency] += st.balance
	}
	for _, st := range states {
		st.mu.Unlock()
	}
	return sums
}

// VerifyChain recomputes every hash and reports the first broken link.
func (l *Ledger) VerifyChain() error {
	l.jmu.Lock()
	evs := slices.Clone(l.events)
	l.jmu.Unlock()
	return verifyEvents(evs)
}

func verifyEvents(evs []Event) error {
	prev := ""
	for i, ev := range evs {
		if ev.Seq != uint64(i)+1 || ev.PrevHash != prev {
			return fmt.Errorf("%w at seq %d: sequence or link mismatch", ErrJournalCorrupt, ev.Seq)
		}
		h, err := hashEvent(ev)
		if err != nil {
			return err
		}
		if h != ev.Hash {
			return fmt.Errorf("%w at seq %d: hash mismatch", ErrJournalCorrupt, ev.Seq)
		}
		prev = ev.Hash
	}
	return nil
}

// Replay rebuilds a ledger from journal events after verifying the chain.
// Pass the same journal via opts so new events continue the chain.
func Replay(events []Event, opts ...Option) (*Ledger, error) {
	if err := verifyEvents(events); err != nil {
		return nil, err
	}
	l := New(opts...)
	for _, ev := range events {
		switch ev.Type {
		case EventAccountOpened:
			l.accounts[ev.Account.ID] = &accountState{acct: *ev.Account}
		case EventTransfer:
			t := ev.Transfer
			from, fok := l.accounts[t.FromAccountID]
			to, tok := l.accounts[t.ToAccountID]
			if !fok || !tok {
				return nil, fmt.Errorf("%w at seq %d: unknown account", ErrJournalCorrupt, ev.Seq)
			}
			applyTransfer(from, to, t.AmountMinor, ev.Seq)
			if t.IdempotencyKey != "" {
				done := make(chan struct{})
				close(done)
				l.idem[t.IdempotencyKey] = &idemRecord{fingerprint: t.Fingerprint, done: done, transfer: *t, createdAt: t.CreatedAt}
			}
		default:
			return nil, fmt.Errorf("%w at seq %d: unknown event type %q", ErrJournalCorrupt, ev.Seq, ev.Type)
		}
		l.events = append(l.events, ev)
		l.lastHash = ev.Hash
	}
	return l, nil
}

// PruneIdempotency forgets completed keys older than ttl and returns how many
// were removed. Replays of pruned keys will execute again, so ttl must exceed
// the client's maximum retry window (24h is the common contract).
func (l *Ledger) PruneIdempotency(ttl time.Duration) int {
	cutoff := l.now().Add(-ttl)
	l.imu.Lock()
	defer l.imu.Unlock()
	n := 0
	for k, rec := range l.idem {
		select {
		case <-rec.done:
			if rec.createdAt.Before(cutoff) {
				delete(l.idem, k)
				n++
			}
		default: // in flight, keep
		}
	}
	return n
}
