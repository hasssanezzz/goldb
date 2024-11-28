package main

import (
	"flag"
	"fmt"
	"log"
	"net/http"
	"os"
	"path/filepath"

	"github.com/hasssanezzz/goldb-engine/api"
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

	serverAddress := fmt.Sprintf("%s:%s", *host, *port)
	log.Println("server is listening on", serverAddress)
	if err := http.ListenAndServe(serverAddress, mux); err != nil {
		log.Println("Error starting server:", err)
	}
}
