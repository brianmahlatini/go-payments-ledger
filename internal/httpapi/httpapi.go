// Package httpapi exposes the ledger over JSON/HTTP with RFC 9457 problem
// responses, strict request decoding and mandatory idempotency keys.
package httpapi

import (
	"context"
	"crypto/rand"
	"crypto/subtle"
	"encoding/hex"
	"encoding/json"
	"errors"
	"fmt"
	"io"
	"log/slog"
	"net/http"
	"strconv"
	"time"

	"github.com/brianmahlatini/go-payments-ledger/internal/ledger"
)

const maxBodyBytes = 64 << 10

type Server struct {
	L          *ledger.Ledger
	Log        *slog.Logger
	AdminToken string // empty disables /v1/admin/*
}

func (s *Server) Routes() http.Handler {
	mux := http.NewServeMux()
	mux.HandleFunc("GET /healthz", func(w http.ResponseWriter, _ *http.Request) { w.WriteHeader(http.StatusNoContent) })
	mux.HandleFunc("POST /v1/accounts", s.openAccount)
	mux.HandleFunc("GET /v1/accounts/{id}", s.getAccount)
	mux.HandleFunc("GET /v1/accounts/{id}/statement", s.statement)
	mux.HandleFunc("POST /v1/transfers", s.transfer)
	mux.Handle("GET /v1/admin/trial-balance", s.admin(http.HandlerFunc(s.trialBalance)))
	mux.Handle("GET /v1/admin/verify", s.admin(http.HandlerFunc(s.verify)))
	return s.recoverer(s.requestLog(mux))
}

type problem struct {
	Type   string `json:"type"`
	Title  string `json:"title"`
	Status int    `json:"status"`
	Detail string `json:"detail,omitempty"`
}

func writeJSON(w http.ResponseWriter, status int, v any) {
	w.Header().Set("Content-Type", "application/json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(v)
}

func writeProblem(w http.ResponseWriter, status int, typ, detail string) {
	w.Header().Set("Content-Type", "application/problem+json")
	w.WriteHeader(status)
	_ = json.NewEncoder(w).Encode(problem{Type: "https://errors.ledger.example/" + typ, Title: http.StatusText(status), Status: status, Detail: detail})
}

// writeError maps domain errors to HTTP. Unknown errors become 500 with a
// generic body; the real cause is only logged, never leaked to clients.
func (s *Server) writeError(w http.ResponseWriter, r *http.Request, err error) {
	switch {
	case errors.Is(err, ledger.ErrNotFound):
		writeProblem(w, http.StatusNotFound, "not-found", err.Error())
	case errors.Is(err, ledger.ErrInsufficientFunds):
		writeProblem(w, http.StatusUnprocessableEntity, "insufficient-funds", err.Error())
	case errors.Is(err, ledger.ErrIdempotencyConflict):
		writeProblem(w, http.StatusUnprocessableEntity, "idempotency-conflict", err.Error())
	case errors.Is(err, ledger.ErrInvalidAmount), errors.Is(err, ledger.ErrInvalidCurrency),
		errors.Is(err, ledger.ErrSameAccount), errors.Is(err, ledger.ErrCurrencyMismatch),
		errors.Is(err, ledger.ErrInvalidName), errors.Is(err, ledger.ErrBalanceOverflow):
		writeProblem(w, http.StatusUnprocessableEntity, "validation", err.Error())
	case errors.Is(err, context.Canceled), errors.Is(err, context.DeadlineExceeded):
		writeProblem(w, http.StatusServiceUnavailable, "timeout", "request cancelled")
	default:
		s.Log.ErrorContext(r.Context(), "internal error", "err", err, "request_id", requestID(r.Context()))
		writeProblem(w, http.StatusInternalServerError, "internal", "internal error")
	}
}

// decode enforces size limit, JSON content, unknown-field rejection and a
// single JSON value per body.
func decode(w http.ResponseWriter, r *http.Request, dst any) error {
	r.Body = http.MaxBytesReader(w, r.Body, maxBodyBytes)
	dec := json.NewDecoder(r.Body)
	dec.DisallowUnknownFields()
	if err := dec.Decode(dst); err != nil {
		return fmt.Errorf("invalid JSON body: %w", err)
	}
	if err := dec.Decode(&struct{}{}); !errors.Is(err, io.EOF) {
		return errors.New("body must contain a single JSON object")
	}
	return nil
}

func (s *Server) openAccount(w http.ResponseWriter, r *http.Request) {
	var in struct {
		Name           string `json:"name"`
		Currency       string `json:"currency"`
		AllowOverdraft bool   `json:"allow_overdraft"`
	}
	if err := decode(w, r, &in); err != nil {
		writeProblem(w, http.StatusBadRequest, "bad-request", err.Error())
		return
	}
	a, err := s.L.OpenAccount(ledger.OpenAccountRequest{Name: in.Name, Currency: in.Currency, AllowOverdraft: in.AllowOverdraft})
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	w.Header().Set("Location", "/v1/accounts/"+a.ID)
	writeJSON(w, http.StatusCreated, a)
}

func (s *Server) getAccount(w http.ResponseWriter, r *http.Request) {
	a, err := s.L.Account(r.PathValue("id"))
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	w.Header().Set("ETag", strconv.Quote(strconv.FormatUint(a.Version, 10)))
	writeJSON(w, http.StatusOK, a)
}

func (s *Server) statement(w http.ResponseWriter, r *http.Request) {
	q := r.URL.Query()
	after, err1 := parseUint(q.Get("after"))
	limit, err2 := parseUint(q.Get("limit"))
	if err1 != nil || err2 != nil {
		writeProblem(w, http.StatusBadRequest, "bad-request", "after and limit must be non-negative integers")
		return
	}
	lines, err := s.L.Statement(r.PathValue("id"), after, int(min(limit, 500)))
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	resp := map[string]any{"data": lines, "next_after": nil}
	if len(lines) > 0 {
		resp["next_after"] = lines[len(lines)-1].Seq
	}
	writeJSON(w, http.StatusOK, resp)
}

func parseUint(s string) (uint64, error) {
	if s == "" {
		return 0, nil
	}
	return strconv.ParseUint(s, 10, 64)
}

func (s *Server) transfer(w http.ResponseWriter, r *http.Request) {
	key := r.Header.Get("Idempotency-Key")
	if key == "" || len(key) > 255 {
		writeProblem(w, http.StatusBadRequest, "idempotency-key-required", "Idempotency-Key header (1-255 chars) is required for money movement")
		return
	}
	var in struct {
		From      string `json:"from_account_id"`
		To        string `json:"to_account_id"`
		Amount    int64  `json:"amount_minor"`
		Currency  string `json:"currency"`
		Reference string `json:"reference"`
	}
	if err := decode(w, r, &in); err != nil {
		writeProblem(w, http.StatusBadRequest, "bad-request", err.Error())
		return
	}
	ctx, cancel := context.WithTimeout(r.Context(), 5*time.Second)
	defer cancel()
	t, replayed, err := s.L.Transfer(ctx, ledger.TransferRequest{
		FromAccountID: in.From, ToAccountID: in.To, AmountMinor: in.Amount,
		Currency: in.Currency, Reference: in.Reference, IdempotencyKey: key,
	})
	if err != nil {
		s.writeError(w, r, err)
		return
	}
	t.Fingerprint = "" // internal detail
	if replayed {
		w.Header().Set("Idempotent-Replayed", "true")
		writeJSON(w, http.StatusOK, t)
		return
	}
	writeJSON(w, http.StatusCreated, t)
}

func (s *Server) trialBalance(w http.ResponseWriter, _ *http.Request) {
	writeJSON(w, http.StatusOK, map[string]any{"balances_by_currency": s.L.TrialBalance()})
}

func (s *Server) verify(w http.ResponseWriter, _ *http.Request) {
	if err := s.L.VerifyChain(); err != nil {
		writeJSON(w, http.StatusConflict, map[string]any{"ok": false, "error": err.Error()})
		return
	}
	writeJSON(w, http.StatusOK, map[string]any{"ok": true})
}

func (s *Server) admin(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		want := "Bearer " + s.AdminToken
		got := r.Header.Get("Authorization")
		if s.AdminToken == "" || subtle.ConstantTimeCompare([]byte(got), []byte(want)) != 1 {
			writeProblem(w, http.StatusUnauthorized, "unauthorized", "admin token required")
			return
		}
		next.ServeHTTP(w, r)
	})
}

type ctxKey struct{}

func requestID(ctx context.Context) string { s, _ := ctx.Value(ctxKey{}).(string); return s }

type statusRecorder struct {
	http.ResponseWriter
	status int
}

func (s *statusRecorder) WriteHeader(code int) { s.status = code; s.ResponseWriter.WriteHeader(code) }

func (s *Server) requestLog(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		id := r.Header.Get("X-Request-ID")
		if id == "" || len(id) > 64 {
			var b [8]byte
			_, _ = rand.Read(b[:])
			id = hex.EncodeToString(b[:])
		}
		w.Header().Set("X-Request-ID", id)
		rec := &statusRecorder{ResponseWriter: w, status: http.StatusOK}
		start := time.Now()
		next.ServeHTTP(rec, r.WithContext(context.WithValue(r.Context(), ctxKey{}, id)))
		s.Log.Info("http", "method", r.Method, "path", r.URL.Path, "status", rec.status,
			"duration_ms", time.Since(start).Milliseconds(), "request_id", id)
	})
}

func (s *Server) recoverer(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		defer func() {
			if v := recover(); v != nil {
				if v == http.ErrAbortHandler {
					panic(v)
				}
				s.Log.Error("panic", "value", fmt.Sprint(v), "path", r.URL.Path)
				writeProblem(w, http.StatusInternalServerError, "internal", "internal error")
			}
		}()
		next.ServeHTTP(w, r)
	})
}
