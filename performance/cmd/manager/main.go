package main

import (
	"context"
	"fmt"
	"log"
	"net/http"
	"time"

	"github.com/prometheus/client_golang/prometheus/promhttp"

	"github.com/openkcm/orbital"
	"github.com/openkcm/orbital/performance"
	"github.com/openkcm/orbital/store/sql"

	_ "github.com/lib/pq"

	stdsql "database/sql"
)

func main() {
	log.Println("Starting orbital manager performance test...")
	envCfg := performance.NewEnvConfig()

	connStr := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=%s",
		envCfg.Postgres.Host, envCfg.Postgres.Port, envCfg.Postgres.User,
		envCfg.Postgres.Password, envCfg.Postgres.DBName, envCfg.Postgres.SSLMode)
	db, err := stdsql.Open("postgres", connStr)
	handleErr("Failed to create database handle", err)
	defer db.Close()

	ctx := context.Background()

	store, err := sql.New(ctx, db)
	handleErr("Failed to create store", err)
	repo := orbital.NewRepository(store)

	mux := http.NewServeMux()

	handler := performance.NewHandler(db, repo)
	mux.HandleFunc("/orbital/run", handler.StartTest)
	mux.HandleFunc("/orbital/stop", handler.StopTest)
	mux.HandleFunc("/health", func(w http.ResponseWriter, _ *http.Request) {
		w.WriteHeader(http.StatusOK)
		w.Write([]byte("ok"))
	})
	mux.Handle("/metrics", promhttp.Handler())

	timeout := time.Minute * 20
	server := &http.Server{
		ReadTimeout:       timeout,
		ReadHeaderTimeout: timeout,
		WriteTimeout:      timeout,
		IdleTimeout:       timeout,
		Addr:              ":" + envCfg.ServerPort,
		Handler:           mux,
	}
	err = server.ListenAndServe()
	handleErr("Failed to start http server", err)
}

func handleErr(msg string, err error) {
	if err != nil {
		log.Fatalf("%s: %s\n", msg, err)
	}
}
