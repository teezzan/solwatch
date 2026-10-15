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

-- One row per mint ever looked up via DAS, including mints DAS had no
-- symbol for (symbol '', decimals -1), so each mint costs at most one lookup.
CREATE TABLE IF NOT EXISTS token_meta (
	mint     TEXT PRIMARY KEY,
	symbol   TEXT NOT NULL,
	decimals INTEGER NOT NULL
);

CREATE TABLE IF NOT EXISTS blocked (
	address TEXT PRIMARY KEY,
	until   INTEGER NOT NULL,
	reason  TEXT NOT NULL DEFAULT ''
);

CREATE TABLE IF NOT EXISTS kv (
	key   TEXT PRIMARY KEY,
	value TEXT NOT NULL
);
`

type Store struct{ db *sql.DB }

// Watch is one chat's subscription to one address.
type Watch struct {
	ChatID    int64
	Address   string
	Label     string
	CreatedAt time.Time
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
	return s.queryWatches(ctx, `SELECT chat_id, address, label, created_at FROM watches WHERE chat_id = ? ORDER BY created_at, address`, chatID)
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
	all, err := s.queryWatches(ctx, `SELECT chat_id, address, label, created_at FROM watches ORDER BY chat_id, address`)
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
		var created int64
		if err := rows.Scan(&w.ChatID, &w.Address, &w.Label, &created); err != nil {
			return nil, err
		}
		w.CreatedAt = time.Unix(created, 0)
		out = append(out, w)
	}
	return out, rows.Err()
}

// CountAddresses returns how many distinct addresses are watched overall.
func (s *Store) CountAddresses(ctx context.Context) (int, error) {
	var n int
	err := s.db.QueryRowContext(ctx, `SELECT COUNT(DISTINCT address) FROM watches`).Scan(&n)
	return n, err
}

// AddressWatched reports whether any chat watches addr.
func (s *Store) AddressWatched(ctx context.Context, addr string) (bool, error) {
	var n int
	err := s.db.QueryRowContext(ctx, `SELECT COUNT(*) FROM watches WHERE address = ?`, addr).Scan(&n)
	return n > 0, err
}

// ExpireWatches deletes watches created before cutoff and returns them, so
// the caller can tell each chat.
func (s *Store) ExpireWatches(ctx context.Context, cutoff time.Time) ([]Watch, error) {
	ws, err := s.queryWatches(ctx,
		`SELECT chat_id, address, label, created_at FROM watches WHERE created_at < ? ORDER BY chat_id`, cutoff.Unix())
	if err != nil || len(ws) == 0 {
		return nil, err
	}
	_, err = s.db.ExecContext(ctx, `DELETE FROM watches WHERE created_at < ?`, cutoff.Unix())
	return ws, err
}

// RemoveAddress deletes every chat's watch on addr and returns them.
func (s *Store) RemoveAddress(ctx context.Context, addr string) ([]Watch, error) {
	ws, err := s.queryWatches(ctx,
		`SELECT chat_id, address, label, created_at FROM watches WHERE address = ? ORDER BY chat_id`, addr)
	if err != nil || len(ws) == 0 {
		return nil, err
	}
	_, err = s.db.ExecContext(ctx, `DELETE FROM watches WHERE address = ?`, addr)
	return ws, err
}

// Block refuses new watches on addr until the given time.
func (s *Store) Block(ctx context.Context, addr string, until time.Time, reason string) error {
	_, err := s.db.ExecContext(ctx,
		`INSERT INTO blocked (address, until, reason) VALUES (?, ?, ?)
		 ON CONFLICT(address) DO UPDATE SET until = excluded.until, reason = excluded.reason`,
		addr, until.Unix(), reason)
	return err
}

// BlockedUntil returns when the block on addr ends (possibly in the past),
// or the zero time if addr was never blocked.
func (s *Store) BlockedUntil(ctx context.Context, addr string) (time.Time, string, error) {
	var until int64
	var reason string
	err := s.db.QueryRowContext(ctx, `SELECT until, reason FROM blocked WHERE address = ?`, addr).Scan(&until, &reason)
	if errors.Is(err, sql.ErrNoRows) {
		return time.Time{}, "", nil
	}
	if err != nil {
		return time.Time{}, "", err
	}
	return time.Unix(until, 0), reason, nil
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

// TokenMeta is cached mint metadata. An empty Symbol means DAS was asked
// and had nothing; callers should not ask again.
type TokenMeta struct {
	Symbol   string
	Decimals int // -1 if unknown
}

// TokenMeta returns cached metadata for the given mints. Mints never
// looked up are absent from the map.
func (s *Store) TokenMeta(ctx context.Context, mints []string) (map[string]TokenMeta, error) {
	out := make(map[string]TokenMeta, len(mints))
	for _, m := range mints {
		var t TokenMeta
		err := s.db.QueryRowContext(ctx, `SELECT symbol, decimals FROM token_meta WHERE mint = ?`, m).Scan(&t.Symbol, &t.Decimals)
		if errors.Is(err, sql.ErrNoRows) {
			continue
		}
		if err != nil {
			return nil, err
		}
		out[m] = t
	}
	return out, nil
}

func (s *Store) PutTokenMeta(ctx context.Context, meta map[string]TokenMeta) error {
	for m, t := range meta {
		if _, err := s.db.ExecContext(ctx,
			`INSERT INTO token_meta (mint, symbol, decimals) VALUES (?, ?, ?)
			 ON CONFLICT(mint) DO UPDATE SET symbol = excluded.symbol, decimals = excluded.decimals`,
			m, t.Symbol, t.Decimals); err != nil {
			return err
		}
	}
	return nil
}

// ChatsWithWatches returns every chat that watches at least one address.
func (s *Store) ChatsWithWatches(ctx context.Context) ([]int64, error) {
	rows, err := s.db.QueryContext(ctx, `SELECT DISTINCT chat_id FROM watches ORDER BY chat_id`)
	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var out []int64
	for rows.Next() {
		var id int64
		if err := rows.Scan(&id); err != nil {
			return nil, err
		}
		out = append(out, id)
	}
	return out, rows.Err()
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
