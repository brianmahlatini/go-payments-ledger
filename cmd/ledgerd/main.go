// Command ledgerd runs the payments ledger HTTP service.
package main

import (
	"context"
	"errors"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/brianmahlatini/go-payments-ledger/internal/httpapi"
	"github.com/brianmahlatini/go-payments-ledger/internal/ledger"
)

func main() {
	log := slog.New(slog.NewJSONHandler(os.Stdout, nil))
	if err := run(log); err != nil {
		log.Error("fatal", "err", err)
		os.Exit(1)
	}
}

func env(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

func run(log *slog.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	var l *ledger.Ledger
	if path := os.Getenv("LEDGER_JOURNAL_PATH"); path != "" {
		j, events, err := ledger.OpenFileJournal(path)
		if err != nil {
			return err
		}
		defer j.Close()
		if l, err = ledger.Replay(events, ledger.WithJournal(j)); err != nil {
			return err
		}
		log.Info("journal replayed", "events", len(events), "path", path)
	} else {
		l = ledger.New()
		log.Warn("LEDGER_JOURNAL_PATH not set: running in-memory, state is lost on exit")
	}

	go func() { // idempotency key retention: 24h contract
		t := time.NewTicker(time.Hour)
		defer t.Stop()
		for {
			select {
			case <-ctx.Done():
				return
			case <-t.C:
				log.Info("pruned idempotency keys", "count", l.PruneIdempotency(24*time.Hour))
			}
		}
	}()

	srv := &http.Server{
		Addr:              env("ADDR", ":8080"),
		Handler:           (&httpapi.Server{L: l, Log: log, AdminToken: os.Getenv("ADMIN_TOKEN")}).Routes(),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       10 * time.Second,
		WriteTimeout:      15 * time.Second,
		IdleTimeout:       60 * time.Second,
	}
	errc := make(chan error, 1)
	go func() {
		log.Info("listening", "addr", srv.Addr)
		errc <- srv.ListenAndServe()
	}()
	select {
	case err := <-errc:
		if !errors.Is(err, http.ErrServerClosed) {
			return err
		}
	case <-ctx.Done():
		log.Info("shutting down: draining in-flight requests")
		sctx, cancel := context.WithTimeout(context.Background(), 20*time.Second)
		defer cancel()
		return srv.Shutdown(sctx)
	}
	return nil
}
