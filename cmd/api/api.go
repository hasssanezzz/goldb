package api

import (
	"fmt"
	"io"
	"log"
	"net/http"
	"strings"

	"github.com/hasssanezzz/goldb/internal"
	"github.com/hasssanezzz/goldb/shared"
)

type API struct {
	DB *internal.Engine
}

func New(source string) (*API, error) {
	db, err := internal.NewEngine(source, *shared.DefaultConfig.WithMemtableSizeThreshold(1000)) // for debugging
	if err != nil {
		return nil, err
	}
	return &API{DB: db}, nil
}

func (api *API) getHandler(w http.ResponseWriter, r *http.Request) {
	// check if this is a prefix scan query
	prefix := r.Header.Get("prefix")
	if len(prefix) > 0 {
		if prefix == "*" {
			prefix = ""
		}

		results, err := api.DB.Scan(prefix)
		if err != nil {
			w.WriteHeader(http.StatusInternalServerError)
			return
		}

		stringResponse := new(strings.Builder)
		for _, key := range results {
			stringResponse.WriteString(key + "\n")
		}

		w.Header().Set("Content-Type", "text/plain")
		w.Write([]byte(stringResponse.String()))
		return
	}

	key := r.Header.Get("Key")
	if len([]byte(key)) > int(api.DB.Config.KeySize) {
		http.Error(w, fmt.Sprintf("Key size must be less than or equal %d bytes", api.DB.Config.KeySize), http.StatusBadRequest)
		return
	}

	data, err := api.DB.Get(key)
	if err != nil {
		if _, ok := err.(*shared.ErrKeyNotFound); ok {
			w.WriteHeader(http.StatusNotFound)
			return
		}
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write(data)
}

func (api *API) postHandler(w http.ResponseWriter, r *http.Request) {
	key := r.Header.Get("Key")
	if len([]byte(key)) > int(api.DB.Config.KeySize) {
		http.Error(w, fmt.Sprintf("Key size must be less than or equal %d bytes", api.DB.Config.KeySize), http.StatusBadRequest)
		return
	}

	body, err := io.ReadAll(r.Body)
	if err != nil {
		http.Error(w, "Unable to read body", http.StatusBadRequest)
		return
	}
	defer r.Body.Close()

	err = api.DB.Set(key, body)
	if err != nil {
		log.Printf("api: error setting (%q, %X): %v\n", key, body, err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}

	w.WriteHeader(http.StatusOK)
	w.Write(body)
}

func (api *API) deleteHandler(w http.ResponseWriter, r *http.Request) {
	key := r.Header.Get("Key")
	if len([]byte(key)) > int(api.DB.Config.KeySize) {
		http.Error(w, fmt.Sprintf("Key size must be less than or equal %d bytes", api.DB.Config.KeySize), http.StatusBadRequest)
		return
	}

	err := api.DB.Delete(key)
	if err != nil {
		log.Printf("api: error deleting (%q): %v\n", key, err)
		w.WriteHeader(http.StatusInternalServerError)
		return
	}
	w.WriteHeader(http.StatusOK)
}

func (api *API) SetupRoutes(mux *http.ServeMux) {
	mux.HandleFunc("GET /", api.getHandler)
	mux.HandleFunc("POST /", api.postHandler)
	mux.HandleFunc("PUT /", api.postHandler)
	mux.HandleFunc("DELETE /", api.deleteHandler)
}
