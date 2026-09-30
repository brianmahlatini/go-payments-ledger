package httpapi

import (
	"bytes"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/brianmahlatini/go-payments-ledger/internal/ledger"
)

func newTestServer(t *testing.T) *httptest.Server {
	t.Helper()
	s := &Server{L: ledger.New(), Log: slog.New(slog.NewTextHandler(io.Discard, nil)), AdminToken: "secret"}
	ts := httptest.NewServer(s.Routes())
	t.Cleanup(ts.Close)
	return ts
}

func do(t *testing.T, method, url, body string, hdr map[string]string) (*http.Response, map[string]any) {
	t.Helper()
	req, _ := http.NewRequest(method, url, bytes.NewBufferString(body))
	for k, v := range hdr {
		req.Header.Set(k, v)
	}
	resp, err := http.DefaultClient.Do(req)
	if err != nil {
		t.Fatal(err)
	}
	defer resp.Body.Close()
	var out map[string]any
	_ = json.NewDecoder(resp.Body).Decode(&out)
	return resp, out
}

func open(t *testing.T, base, name string, overdraft bool) string {
	b, _ := json.Marshal(map[string]any{"name": name, "currency": "ZAR", "allow_overdraft": overdraft})
	resp, out := do(t, "POST", base+"/v1/accounts", string(b), nil)
	if resp.StatusCode != 201 {
		t.Fatalf("open: %d %v", resp.StatusCode, out)
	}
	return out["id"].(string)
}

func TestTransferFlowOverHTTP(t *testing.T) {
	ts := newTestServer(t)
	bank := open(t, ts.URL, "funding", true)
	alice := open(t, ts.URL, "alice", false)
	body := `{"from_account_id":"` + bank + `","to_account_id":"` + alice + `","amount_minor":1999,"currency":"ZAR"}`

	resp, out := do(t, "POST", ts.URL+"/v1/transfers", body, map[string]string{"Idempotency-Key": "abc"})
	if resp.StatusCode != 201 {
		t.Fatalf("create: %d %v", resp.StatusCode, out)
	}
	if _, leaked := out["fingerprint"]; leaked {
		t.Fatal("fingerprint must not be exposed")
	}
	resp, out2 := do(t, "POST", ts.URL+"/v1/transfers", body, map[string]string{"Idempotency-Key": "abc"})
	if resp.StatusCode != 200 || resp.Header.Get("Idempotent-Replayed") != "true" || out2["id"] != out["id"] {
		t.Fatalf("replay: %d %v", resp.StatusCode, out2)
	}
	_, acct := do(t, "GET", ts.URL+"/v1/accounts/"+alice, "", nil)
	if acct["balance_minor"].(float64) != 1999 {
		t.Fatalf("balance %v", acct["balance_minor"])
	}
}

func TestRequestValidation(t *testing.T) {
	ts := newTestServer(t)
	cases := []struct {
		name   string
		body   string
		hdr    map[string]string
		status int
	}{
		{"missing key", `{}`, nil, 400},
		{"unknown field", `{"amount_minor":1,"hack":true}`, map[string]string{"Idempotency-Key": "k"}, 400},
		{"float amount", `{"amount_minor":10.5}`, map[string]string{"Idempotency-Key": "k"}, 400},
		{"trailing data", `{} {}`, map[string]string{"Idempotency-Key": "k"}, 400},
		{"domain error", `{"from_account_id":"a","to_account_id":"b","amount_minor":0,"currency":"ZAR"}`, map[string]string{"Idempotency-Key": "k"}, 422},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			resp, out := do(t, "POST", ts.URL+"/v1/transfers", tc.body, tc.hdr)
			if resp.StatusCode != tc.status {
				t.Fatalf("got %d want %d: %v", resp.StatusCode, tc.status, out)
			}
			if ct := resp.Header.Get("Content-Type"); ct != "application/problem+json" {
				t.Fatalf("content-type %q", ct)
			}
		})
	}
}

func TestAdminRequiresToken(t *testing.T) {
	ts := newTestServer(t)
	if resp, _ := do(t, "GET", ts.URL+"/v1/admin/trial-balance", "", nil); resp.StatusCode != 401 {
		t.Fatalf("got %d", resp.StatusCode)
	}
	resp, out := do(t, "GET", ts.URL+"/v1/admin/verify", "", map[string]string{"Authorization": "Bearer secret"})
	if resp.StatusCode != 200 || out["ok"] != true {
		t.Fatalf("verify: %d %v", resp.StatusCode, out)
	}
}

func TestNotFoundIsProblem(t *testing.T) {
	ts := newTestServer(t)
	if resp, out := do(t, "GET", ts.URL+"/v1/accounts/acc_missing", "", nil); resp.StatusCode != 404 || out["status"].(float64) != 404 {
		t.Fatalf("%d %v", resp.StatusCode, out)
	}
}
