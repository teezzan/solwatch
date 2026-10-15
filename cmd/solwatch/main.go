// Command solwatch is a Telegram bot that alerts you when watched Solana
// wallets transact, powered by a Helius enhanced-transaction webhook.
package main

import (
	"bufio"
	"context"
	"crypto/rand"
	"encoding/hex"
	"errors"
	"flag"
	"fmt"
	"log/slog"
	"net/http"
	"os"
	"os/signal"
	"strconv"
	"strings"
	"sync"
	"syscall"
	"time"

	"github.com/teezzan/solwatch/internal/bot"
	"github.com/teezzan/solwatch/internal/helius"
	"github.com/teezzan/solwatch/internal/notify"
	"github.com/teezzan/solwatch/internal/pipeline"
	"github.com/teezzan/solwatch/internal/reconcile"
	"github.com/teezzan/solwatch/internal/server"
	"github.com/teezzan/solwatch/internal/store"
	"github.com/teezzan/solwatch/internal/telegram"
	"github.com/teezzan/solwatch/internal/usage"
)

const webhookPath = "/webhooks/helius"

type config struct {
	HeliusAPIKey     string
	PublicURL        string
	WebhookSecret    string
	TelegramToken    string
	ListenAddr       string
	DBPath           string
	Explorer         string
	ConsoleChatID    int64
	CaptureDir       string
	DeleteOnExit     bool
	WebhookType      string
	LogLevel         slog.Level
	DigestInterval   time.Duration
	DigestMaxLines   int
	UsageLogInterval time.Duration
	Limits           bot.Limits
}

func main() {
	envFile := flag.String("env-file", ".env", "optional KEY=VALUE file; real environment variables win")
	flag.Parse()
	if err := loadEnvFile(*envFile); err != nil {
		fmt.Fprintln(os.Stderr, "solwatch:", err)
		os.Exit(1)
	}
	cfg, err := loadConfig()
	if err != nil {
		fmt.Fprintln(os.Stderr, "solwatch:", err)
		os.Exit(2)
	}
	log := slog.New(slog.NewJSONHandler(os.Stderr, &slog.HandlerOptions{Level: cfg.LogLevel}))
	if err := run(cfg, log); err != nil {
		log.Error("fatal", "err", err)
		os.Exit(1)
	}
}

func run(cfg config, log *slog.Logger) error {
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	st, err := store.Open(cfg.DBPath)
	if err != nil {
		return err
	}
	defer st.Close()

	if cfg.WebhookSecret == "" {
		if cfg.WebhookSecret, err = persistentSecret(ctx, st); err != nil {
			return err
		}
	}

	counter := &usage.Counter{Log: log.With("component", "usage")}
	hc := helius.NewClient(cfg.HeliusAPIKey)
	hc.OnCall = func(kind string) { counter.Add(kind, 1) }
	rec := &reconcile.Reconciler{
		API:         hc,
		State:       st,
		WebhookURL:  strings.TrimRight(cfg.PublicURL, "/") + webhookPath,
		AuthHeader:  cfg.WebhookSecret,
		WebhookType: cfg.WebhookType,
		Debounce:    2 * time.Second,
		Log:         log.With("component", "reconcile"),
	}

	var sink notify.Sink
	var tg *telegram.Client
	if cfg.TelegramToken != "" {
		tg = telegram.New(cfg.TelegramToken)
		sink = &notify.Telegram{Client: tg}
	} else {
		log.Warn("TELEGRAM_BOT_TOKEN not set: reading commands from stdin and printing alerts to stdout")
		sink = &notify.Writer{W: os.Stdout}
	}
	var notifier notify.Notifier = &notify.Immediate{Sink: sink, Explorer: cfg.Explorer}
	var digest *notify.Digest
	if cfg.DigestInterval > 0 {
		digest = &notify.Digest{Sink: sink, Explorer: cfg.Explorer, Interval: cfg.DigestInterval,
			MaxLines: cfg.DigestMaxLines, Log: log.With("component", "digest")}
		notifier = digest
	}

	b := &bot.Bot{
		Store:    st,
		Accounts: hc,
		Notes:    sink,
		OnChange: rec.Trigger,
		Limits:   cfg.Limits,
		Log:      log.With("component", "bot"),
	}
	queue := make(chan helius.EnhancedTransaction, 1024)
	proc := &pipeline.Processor{
		Store: st, Tokens: hc, Notifier: notifier, Log: log.With("component", "pipeline"),
		Breaker: &pipeline.Breaker{Max: cfg.Limits.HotPerMinute},
		OnHot:   b.Evict,
	}

	srv := &http.Server{
		Addr: cfg.ListenAddr,
		Handler: server.Mux(webhookPath, &server.WebhookHandler{
			Secret:     cfg.WebhookSecret,
			Queue:      queue,
			CaptureDir: cfg.CaptureDir,
			Log:        log.With("component", "http"),
			OnEvents:   func(n int) { counter.Add("webhook_event", n) },
		}),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
	}

	var procDone sync.WaitGroup
	procDone.Add(1)
	go func() { defer procDone.Done(); proc.Run(context.WithoutCancel(ctx), queue) }()

	go rec.Run(ctx)
	go prune(ctx, st, log)
	go counter.Run(ctx, cfg.UsageLogInterval)
	if cfg.Limits.TTL > 0 {
		go b.RunExpiry(ctx, time.Minute)
	}
	if digest != nil {
		go digest.Run(ctx)
	}
	if tg != nil {
		go b.RunTelegram(ctx, tg)
	} else {
		go b.RunConsole(ctx, os.Stdin, os.Stdout, cfg.ConsoleChatID)
	}

	srvErr := make(chan error, 1)
	go func() {
		log.Info("listening", "addr", cfg.ListenAddr, "webhook_url", rec.WebhookURL)
		srvErr <- srv.ListenAndServe()
	}()

	select {
	case <-ctx.Done():
		log.Info("shutting down")
	case err := <-srvErr:
		if !errors.Is(err, http.ErrServerClosed) {
			return err
		}
	}

	// Stop taking deliveries, drain what's queued, then (optionally) remove
	// the webhook so Helius stops spending credits on a dead endpoint.
	shutCtx, cancel := context.WithTimeout(context.Background(), 15*time.Second)
	defer cancel()
	if err := srv.Shutdown(shutCtx); err != nil {
		log.Warn("http shutdown", "err", err)
	}
	close(queue)
	procDone.Wait()
	if digest != nil {
		digest.Flush(shutCtx) // don't drop alerts buffered for the next digest
	}
	if cfg.DeleteOnExit {
		if err := rec.Teardown(shutCtx); err != nil {
			log.Error("webhook teardown failed", "err", err)
		}
	}
	log.Info("bye")
	return nil
}

func prune(ctx context.Context, st *store.Store, log *slog.Logger) {
	t := time.NewTicker(time.Hour)
	defer t.Stop()
	for {
		select {
		case <-ctx.Done():
			return
		case <-t.C:
			if n, err := st.PruneSeen(ctx, 24*time.Hour); err != nil {
				log.Warn("prune failed", "err", err)
			} else if n > 0 {
				log.Debug("pruned seen signatures", "rows", n)
			}
		}
	}
}

// persistentSecret generates the webhook auth secret once and keeps it in
// the database, so restarts don't need to re-push it to Helius.
func persistentSecret(ctx context.Context, st *store.Store) (string, error) {
	s, err := st.Get(ctx, "webhook_secret")
	if err != nil || s != "" {
		return s, err
	}
	b := make([]byte, 32)
	if _, err := rand.Read(b); err != nil {
		return "", err
	}
	s = hex.EncodeToString(b)
	return s, st.Set(ctx, "webhook_secret", s)
}

func loadConfig() (config, error) {
	c := config{
		HeliusAPIKey:  os.Getenv("HELIUS_API_KEY"),
		PublicURL:     os.Getenv("PUBLIC_URL"),
		WebhookSecret: os.Getenv("WEBHOOK_SECRET"),
		TelegramToken: os.Getenv("TELEGRAM_BOT_TOKEN"),
		ListenAddr:    envOr("LISTEN_ADDR", ":8080"),
		DBPath:        envOr("DB_PATH", "solwatch.db"),
		Explorer:      envOr("EXPLORER_TX_URL", "https://solscan.io/tx/%s"),
		CaptureDir:    os.Getenv("CAPTURE_DIR"),
		WebhookType:   envOr("HELIUS_WEBHOOK_TYPE", "enhanced"),
	}
	var missing []string
	if c.HeliusAPIKey == "" {
		missing = append(missing, "HELIUS_API_KEY")
	}
	if c.PublicURL == "" {
		missing = append(missing, "PUBLIC_URL")
	}
	if len(missing) > 0 {
		return c, fmt.Errorf("missing required config: %s", strings.Join(missing, ", "))
	}
	if !strings.HasPrefix(c.PublicURL, "https://") {
		return c, errors.New("PUBLIC_URL must be https:// (Helius delivers to public HTTPS endpoints)")
	}
	if !strings.Contains(c.Explorer, "%s") {
		return c, errors.New("EXPLORER_TX_URL must contain %s for the signature")
	}
	var err error
	p := envParser{}
	c.DigestInterval = p.duration("DIGEST_INTERVAL", "60s")
	c.DigestMaxLines = p.int("DIGEST_MAX_LINES", "5")
	c.UsageLogInterval = p.duration("USAGE_LOG_INTERVAL", "1h")
	c.Limits = bot.Limits{
		MaxPerChat:        p.int("MAX_WATCHES_PER_CHAT", "3"),
		MaxAddresses:      p.int("MAX_WATCHED_ADDRESSES", "50"),
		TTL:               p.duration("WATCH_TTL", "24h"),
		CommandsPerMinute: p.int("COMMANDS_PER_MINUTE", "10"),
		WalletsOnly:       p.bool("WALLETS_ONLY", "true"),
		HotPerMinute:      p.int("HOT_ADDRESS_MAX_PER_MIN", "30"),
		HotBlock:          p.duration("HOT_ADDRESS_BLOCK", "24h"),
		DigestInterval:    c.DigestInterval,
		RepoURL:           envOr("REPO_URL", "https://github.com/teezzan/solwatch"),
		WriteupURL:        envOr("WRITEUP_URL", "https://blog.taiwo.dev/posts/real-time-solana-wallet-alerts-in-go-with-helius-webhooks/"),
	}
	if p.err != nil {
		return c, p.err
	}
	if c.UsageLogInterval <= 0 {
		return c, errors.New("USAGE_LOG_INTERVAL must be positive")
	}
	if c.ConsoleChatID, err = strconv.ParseInt(envOr("CONSOLE_CHAT_ID", "1"), 10, 64); err != nil {
		return c, fmt.Errorf("CONSOLE_CHAT_ID: %w", err)
	}
	if c.DeleteOnExit, err = strconv.ParseBool(envOr("DELETE_WEBHOOK_ON_EXIT", "false")); err != nil {
		return c, fmt.Errorf("DELETE_WEBHOOK_ON_EXIT: %w", err)
	}
	if err := c.LogLevel.UnmarshalText([]byte(envOr("LOG_LEVEL", "info"))); err != nil {
		return c, fmt.Errorf("LOG_LEVEL: %w", err)
	}
	return c, nil
}

func envOr(k, def string) string {
	if v := os.Getenv(k); v != "" {
		return v
	}
	return def
}

// loadEnvFile reads simple KEY=VALUE lines. Variables already set in the
// environment take precedence. A missing file is not an error.
func loadEnvFile(path string) error {
	f, err := os.Open(path)
	if errors.Is(err, os.ErrNotExist) {
		return nil
	}
	if err != nil {
		return err
	}
	defer f.Close()
	sc := bufio.NewScanner(f)
	for sc.Scan() {
		line := strings.TrimSpace(sc.Text())
		if line == "" || strings.HasPrefix(line, "#") {
			continue
		}
		k, v, ok := strings.Cut(line, "=")
		if !ok {
			continue
		}
		k = strings.TrimSpace(strings.TrimPrefix(k, "export "))
		v = strings.Trim(strings.TrimSpace(v), `"'`)
		if _, set := os.LookupEnv(k); !set {
			os.Setenv(k, v)
		}
	}
	return sc.Err()
}

// envParser reads typed env vars, keeping the first error.
type envParser struct{ err error }

func (p *envParser) fail(k string, err error) {
	if p.err == nil {
		p.err = fmt.Errorf("%s: %w", k, err)
	}
}

func (p *envParser) int(k, def string) int {
	v, err := strconv.Atoi(envOr(k, def))
	if err != nil {
		p.fail(k, err)
	}
	return v
}

func (p *envParser) duration(k, def string) time.Duration {
	v, err := time.ParseDuration(envOr(k, def))
	if err != nil {
		p.fail(k, err)
	}
	return v
}

func (p *envParser) bool(k, def string) bool {
	v, err := strconv.ParseBool(envOr(k, def))
	if err != nil {
		p.fail(k, err)
	}
	return v
}
