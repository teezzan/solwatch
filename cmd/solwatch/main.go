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
)

const webhookPath = "/webhooks/helius"

type config struct {
	HeliusAPIKey      string
	PublicURL         string
	WebhookSecret     string
	TelegramToken     string
	ListenAddr        string
	DBPath            string
	Explorer          string
	MaxWatchesPerChat int
	ConsoleChatID     int64
	CaptureDir        string
	DeleteOnExit      bool
	WebhookType       string
	LogLevel          slog.Level
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

	hc := helius.NewClient(cfg.HeliusAPIKey)
	rec := &reconcile.Reconciler{
		API:         hc,
		State:       st,
		WebhookURL:  strings.TrimRight(cfg.PublicURL, "/") + webhookPath,
		AuthHeader:  cfg.WebhookSecret,
		WebhookType: cfg.WebhookType,
		Debounce:    2 * time.Second,
		Log:         log.With("component", "reconcile"),
	}

	var notifier notify.Notifier
	var tg *telegram.Client
	if cfg.TelegramToken != "" {
		tg = telegram.New(cfg.TelegramToken)
		notifier = &notify.Telegram{Client: tg, Explorer: cfg.Explorer}
	} else {
		log.Warn("TELEGRAM_BOT_TOKEN not set: reading commands from stdin and printing alerts to stdout")
		notifier = &notify.Writer{W: os.Stdout, Explorer: cfg.Explorer}
	}

	queue := make(chan helius.EnhancedTransaction, 1024)
	proc := &pipeline.Processor{Store: st, Tokens: hc, Notifier: notifier, Log: log.With("component", "pipeline")}
	b := &bot.Bot{Store: st, OnChange: rec.Trigger, MaxPerChat: cfg.MaxWatchesPerChat}

	srv := &http.Server{
		Addr: cfg.ListenAddr,
		Handler: server.Mux(webhookPath, &server.WebhookHandler{
			Secret:     cfg.WebhookSecret,
			Queue:      queue,
			CaptureDir: cfg.CaptureDir,
			Log:        log.With("component", "http"),
		}),
		ReadHeaderTimeout: 5 * time.Second,
		ReadTimeout:       15 * time.Second,
	}

	var procDone sync.WaitGroup
	procDone.Add(1)
	go func() { defer procDone.Done(); proc.Run(context.WithoutCancel(ctx), queue) }()

	go rec.Run(ctx)
	go prune(ctx, st, log)
	if tg != nil {
		go b.RunTelegram(ctx, tg, log.With("component", "telegram"))
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
	if c.MaxWatchesPerChat, err = strconv.Atoi(envOr("MAX_WATCHES_PER_CHAT", "25")); err != nil {
		return c, fmt.Errorf("MAX_WATCHES_PER_CHAT: %w", err)
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
