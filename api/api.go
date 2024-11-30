package api

import (
	"io/ioutil"
	"log"
	"net/http"

	"github.com/hasssanezzz/goldb-engine/engine"
	"github.com/hasssanezzz/goldb-engine/index_manager"
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
	key := r.Header.Get("Key")
	data, err := api.DB.Get(key)
	if err != nil {
		if _, ok := err.(*index_manager.ErrKeyNotFound); ok {
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
