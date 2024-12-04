package api

import (
	"io/ioutil"
	"log"
	"net/http"
	"strings"

	"github.com/hasssanezzz/goldb-engine/engine"
	"github.com/hasssanezzz/goldb-engine/shared"
)

type API struct {
	DB *engine.Engine
}

func New(source string) (*API, error) {
	db, err := engine.New(source)
	if err != nil {
		return nil, err
	}
	return &API{DB: db}, nil
}

func (api *API) getHandler(w http.ResponseWriter, r *http.Request) {
	// check is this is a prefix scan query
	prefix := r.Header.Get("prefix")
	if len(prefix) > 0 {
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
	if len([]byte(key)) > shared.KeyByteLength {
		http.Error(w, "Key size must be less than or equal 256 bytes", http.StatusBadRequest)
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
	if len([]byte(key)) > shared.KeyByteLength {
		http.Error(w, "Key size must be less than or equal 256 bytes", http.StatusBadRequest)
		return
	}

	body, err := ioutil.ReadAll(r.Body)
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
	if len([]byte(key)) > shared.KeyByteLength {
		http.Error(w, "Key size must be less than or equal 256 bytes", http.StatusBadRequest)
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
