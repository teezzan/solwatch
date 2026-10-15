// Package store keeps solwatch's state in SQLite: who watches what, which
// signatures were already alerted, cached token symbols, and a tiny
// key/value table for things like the Helius webhook ID.
package store

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"time"

	_ "modernc.org/sqlite" // pure Go driver, keeps the binary CGO-free
)

const schema = `
CREATE TABLE IF NOT EXISTS watches (
	chat_id    INTEGER NOT NULL,
	address    TEXT    NOT NULL,
	label      TEXT    NOT NULL DEFAULT '',
	created_at INTEGER NOT NULL,
	PRIMARY KEY (chat_id, address)
);
CREATE INDEX IF NOT EXISTS watches_address ON watches(address);

CREATE TABLE IF NOT EXISTS seen (
	signature TEXT PRIMARY KEY,
	seen_at   INTEGER NOT NULL
);
CREATE INDEX IF NOT EXISTS seen_at ON seen(seen_at);

CREATE TABLE IF NOT EXISTS tokens (
	mint   TEXT PRIMARY KEY,
	symbol TEXT NOT NULL
);

CREATE TABLE IF NOT EXISTS kv (
	key   TEXT PRIMARY KEY,
	value TEXT NOT NULL
);
`

type Store struct{ db *sql.DB }

// Watch is one chat's subscription to one address.
type Watch struct {
	ChatID  int64
	Address string
	Label   string
}

func Open(path string) (*Store, error) {
	dsn := fmt.Sprintf("file:%s?_pragma=journal_mode(WAL)&_pragma=busy_timeout(5000)&_pragma=foreign_keys(1)", path)
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		return nil, err
	}
	// One writer at a time is how SQLite works anyway; a single connection
	// avoids SQLITE_BUSY between our own goroutines.
	db.SetMaxOpenConns(1)
	if _, err := db.Exec(schema); err != nil {
		db.Close()
		return nil, fmt.Errorf("store: migrate: %w", err)
	}
	return &Store{db: db}, nil
}

func (s *Store) Close() error { return s.db.Close() }

// AddWatch inserts or relabels a watch. It reports whether the address was
// new for this chat.
func (s *Store) AddWatch(ctx context.Context, w Watch) (created bool, err error) {
	res, err := s.db.ExecContext(ctx,
		`INSERT INTO watches (chat_id, address, label, created_at) VALUES (?, ?, ?, ?)
		 ON CONFLICT(chat_id, address) DO NOTHING`,
		w.ChatID, w.Address, w.Label, time.Now().Unix())
	if err != nil {
		return false, err
	}
	if n, _ := res.RowsAffected(); n == 1 {
		return true, nil
	}
	_, err = s.db.ExecContext(ctx, `UPDATE watches SET label = ? WHERE chat_id = ? AND address = ?`,
		w.Label, w.ChatID, w.Address)
	return false, err
}

// RemoveWatch deletes a watch by address or by label. It reports whether
// anything was removed.
func (s *Store) RemoveWatch(ctx context.Context, chatID int64, addressOrLabel string) (bool, error) {
	res, err := s.db.ExecContext(ctx,
		`DELETE FROM watches WHERE chat_id = ? AND (address = ? OR (label != '' AND label = ?))`,
		chatID, addressOrLabel, addressOrLabel)
	if err != nil {
		return false, err
	}
	n, _ := res.RowsAffected()
	return n > 0, nil
}

func (s *Store) ListWatches(ctx context.Context, chatID int64) ([]Watch, error) {
	return s.queryWatches(ctx, `SELECT chat_id, address, label FROM watches WHERE chat_id = ? ORDER BY created_at, address`, chatID)
}

func (s *Store) CountWatches(ctx context.Context, chatID int64) (int, error) {
	var n int
	err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM watches WHERE chat_id = ?`, chatID).Scan(&n)
	return n, err
}

// WatchersOf returns every watch on any of the given addresses.
func (s *Store) WatchersOf(ctx context.Context, addresses map[string]struct{}) ([]Watch, error) {
	if len(addresses) == 0 {
		return nil, nil
	}
	// Transactions touch tens of accounts and we watch a handful, so pull
	// the (small) watch list and intersect in Go rather than building a
	// giant IN clause.
	all, err := s.queryWatches(ctx, `SELECT chat_id, address, label FROM watches ORDER BY chat_id, address`)
	if err != nil {
		return nil, err
	}
	var out []Watch
	for _, w := range all {
		if _, ok := addresses[w.Address]; ok {
			out = append(out, w)
		}
	}
	return out, nil
}

// Addresses returns the distinct set of watched addresses across all chats,
// sorted. This is exactly what the Helius webhook should cover.
func (s *Store) Addresses(ctx context.Context) ([]string, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT DISTINCT address FROM watches ORDER BY address`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []string
	for rows.Next() {
		var a string
		if err := rows.Scan(&a); err != nil {
			return nil, err
		}
		out = append(out, a)
	}
	return out, rows.Err()
}

func (s *Store) queryWatches(ctx context.Context, q string, args ...any) ([]Watch, error) {
	rows, err := s.db.QueryContext(ctx, q, args...)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []Watch
	for rows.Next() {
		var w Watch
		if err := rows.Scan(&w.ChatID, &w.Address, &w.Label); err != nil {
			return nil, err
		}
		out = append(out, w)
	}
	return out, rows.Err()
}

// MarkSeen records a signature and reports whether it was new. This is the
// idempotency gate: Helius retries and may deliver the same event twice.
func (s *Store) MarkSeen(ctx context.Context, signature string) (bool, error) {
	res, err := s.db.ExecContext(ctx,
		`INSERT INTO seen (signature, seen_at) VALUES (?, ?) ON CONFLICT(signature) DO NOTHING`,
		signature, time.Now().Unix())
	if err != nil {
		return false, err
	}
	n, _ := res.RowsAffected()
	return n == 1, nil
}

// PruneSeen forgets signatures older than maxAge. Retries arrive within
// seconds, so a day of history is plenty.
func (s *Store) PruneSeen(ctx context.Context, maxAge time.Duration) (int64, error) {
	res, err := s.db.ExecContext(ctx, `DELETE FROM seen WHERE seen_at < ?`, time.Now().Add(-maxAge).Unix())
	if err != nil {
		return 0, err
	}
	return res.RowsAffected()
}

// Symbols returns cached symbols for the given mints.
func (s *Store) Symbols(ctx context.Context, mints []string) (map[string]string, error) {
	out := make(map[string]string, len(mints))
	for _, m := range mints {
		var sym string
		err := s.db.QueryRowContext(ctx, `SELECT symbol FROM tokens WHERE mint = ?`, m).Scan(&sym)
		if errors.Is(err, sql.ErrNoRows) {
			continue
		}
		if err != nil {
			return nil, err
		}
		out[m] = sym
	}
	return out, nil
}

func (s *Store) PutSymbols(ctx context.Context, syms map[string]string) error {
	for m, sym := range syms {
		if _, err := s.db.ExecContext(ctx,
			`INSERT INTO tokens (mint, symbol) VALUES (?, ?) ON CONFLICT(mint) DO UPDATE SET symbol = excluded.symbol`,
			m, sym); err != nil {
			return err
		}
	}
	return nil
}

// Get returns a kv value, or "" if unset.
func (s *Store) Get(ctx context.Context, key string) (string, error) {
	var v string
	err := s.db.QueryRowContext(ctx, `SELECT value FROM kv WHERE key = ?`, key).Scan(&v)
	if errors.Is(err, sql.ErrNoRows) {
		return "", nil
	}
	return v, err
}

func (s *Store) Set(ctx context.Context, key, value string) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO kv (key, value) VALUES (?, ?) ON CONFLICT(key) DO UPDATE SET value = excluded.value`, key, value)
	return err
}

func (s *Store) Delete(ctx context.Context, key string) error {
	_, err := s.db.ExecContext(ctx, `DELETE FROM kv WHERE key = ?`, key)
	return err
}
