package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"
	"time"

	"github.com/hasssanezzz/goldb/cmd/api"
)

func createHomeDir() (string, error) {
	homeDir, err := os.UserHomeDir()
	if err != nil {
		return "", fmt.Errorf("error getting home directory: %v", err)
	}

	dirPath := filepath.Join(homeDir, ".goldb")

	if _, err := os.Stat(dirPath); os.IsNotExist(err) {
		err := os.MkdirAll(dirPath, 0755)
		if err != nil {
			return "", fmt.Errorf("Error creating directory ~/.goldb: %v", err)
		}
	}
	return dirPath, nil
}

func main() {

	host := flag.String("h", "localhost", "Host to bind the server to")
	port := flag.String("p", "3011", "Port to listen on")
	source := flag.String("s", "~/.goldb", "Path to the source directory")

	if len(os.Args) > 1 && os.Args[1] == "--help" {
		fmt.Println(`Usage: program [options]

Options:
  -h, string        Host to bind the server to (default: "localhost")
  -p, string        Port to listen on (default: "3011")
  -s, string        Path to the source directory (default: "~/.goldb")
  --help            Show this help message and exit`)
		os.Exit(0)
	}

	flag.Parse()

	if *source == "~/.goldb" {
		path, err := createHomeDir()
		if err != err {
			log.Fatal(err)
		}
		*source = path
	}

	api, err := api.New(*source)
	if err != nil {
		log.Fatalf("can not open db: %v", err)
	}
	defer api.DB.Close()

	mux := http.NewServeMux()
	api.SetupRoutes(mux)

	server := &http.Server{
		Addr:    fmt.Sprintf("%s:%s", *host, *port),
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
