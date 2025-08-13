package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/openkcm/orbital"
	"github.com/openkcm/orbital/performance"
	"github.com/openkcm/orbital/store/sql"

	_ "github.com/lib/pq"

	stdsql "database/sql"
)

func main() {
	cfg := performance.NewEnvConfig()

	connStr := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=%s",
		cfg.Postgres.Host, cfg.Postgres.Port, cfg.Postgres.User,
		cfg.Postgres.Password, cfg.Postgres.DBName, cfg.Postgres.SSLMode)

	db, err := stdsql.Open("postgres", connStr)
	handleErr("Failed to create database handle", err)
	defer func() {
		if cerr := db.Close(); cerr != nil {
			log.Printf("DB close error: %v", cerr)
		}
	}()

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigs := make(chan os.Signal, 1)
	signal.Notify(sigs, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigs
		cancel()
	}()

	store, err := sql.New(ctx, db)
	handleErr("Failed to create store", err)
	repo := orbital.NewRepository(store)

	mux := http.NewServeMux()

	h := performance.NewHandler(db, repo)
	mux.HandleFunc("/orbital/start", h.StartTest)
	mux.HandleFunc("/orbital/stop", h.StopTest)
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		_, err := w.Write([]byte("ok"))
		if err != nil {
			log.Printf("Health response error: %v", err)
		}
	})
	mux.Handle("/metrics", promhttp.Handler())

	timeout := time.Minute * 20 // to ensure long-running tests can complete
	srv := &http.Server{
		Addr:              ":" + cfg.ServerPort,
		Handler:           mux,
		ReadTimeout:       timeout,
		ReadHeaderTimeout: timeout,
		WriteTimeout:      timeout,
		IdleTimeout:       timeout,
	}

	go func() {
		if err := srv.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("Server start failed: %v", err)
		}
	}()

	<-ctx.Done()
	shutdownCtx, shutdownCancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer shutdownCancel()
	if err := srv.Shutdown(shutdownCtx); err != nil {
		log.Printf("Server shutdown error: %v", err)
	}
}

func handleErr(msg string, err error) {
	if err != nil {
		log.Fatalf("%s: %s\n", msg, err)
	}
}
