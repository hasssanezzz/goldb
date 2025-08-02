package main

import (
	"context"
	"flag"
	"log"
	"net/http"
	_ "net/http/pprof"
	"os"
	"os/signal"
	"syscall"
	"time"

	"github.com/hasssanezzz/goldb/cmd/api"
)

func parseFlags() (string, string, bool) {
	addr := flag.String("a", ":3011", "Host to bind the server to")
	debug := flag.Bool("d", false, "Debug mode")
	source := flag.String("s", ".goldb", "Path to the source directory")
	flag.Parse()

	return *addr, *source, *debug
}

func main() {
	addr, source, debug := parseFlags()

	if debug {
		println("[DEBUG MODE]")
		go func() {
			http.ListenAndServe("localhost:6060", nil)
		}()
	}

	api, err := api.New(source)
	if err != nil {
		log.Fatalf("can not open db: %v", err)
	}
	defer func() {
		if err := api.DB.Close(); err != nil {
			panic(err)
		}
	}()

	mux := http.NewServeMux()
	api.SetupRoutes(mux)

	server := &http.Server{
		Addr:    addr,
		Handler: mux,
	}

	stop := make(chan os.Signal, 1)
	signal.Notify(stop, os.Interrupt, syscall.SIGTERM)

	go func() {
		log.Println("server is listening on", server.Addr)
		if err := server.ListenAndServe(); err != nil && err != http.ErrServerClosed {
			log.Fatalf("error starting server: %v", err)
		}
	}()

	<-stop
	log.Println("shutting down server...")
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	if err := server.Shutdown(ctx); err != nil {
		log.Fatalf("error during server shutdown: %v", err)
	}
	log.Println("server gracefully stopped.")
}
