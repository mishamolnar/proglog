package server

import (
	"encoding/json"
	"fmt"
	"github.com/go-chi/chi/v5"
	"github.com/mishamolnar/proglog/api/v1"
	"github.com/mishamolnar/proglog/internal/log"
	"net/http"
	"os"
)

func NewHTTPServer(addr string) *http.Server {
	httpsrc := newHTTPServer()
	r := chi.NewRouter()
	r.Post("/", httpsrc.handleProduce)
	r.Get("/", httpsrc.handleConsume)
	return &http.Server{
		Addr:    addr,
		Handler: r,
	}
}

type httpServer struct {
	Log *log.Log
}

func newHTTPServer() *httpServer {
	l, err := log.NewLog("/tmp/logs", log.Config{})
	if err != nil {
		fmt.Println(err)
		os.Exit(1)
	}
	return &httpServer{
		Log: l,
	}
}

type ProduceRequest struct {
	Record log_v1.Record `json:"record"`
}

type ProducerResponse struct {
	Offset uint64 `json:"offset"`
}

func (h *httpServer) handleProduce(w http.ResponseWriter, r *http.Request) {
	var req ProduceRequest
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		fmt.Println(err)
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	offset, err := h.Log.Append(&req.Record)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	err = json.NewEncoder(w).Encode(ProducerResponse{Offset: offset})
	if err != nil {
		fmt.Printf("Could not write to response body %v \n", err)
	}
}

type ConsumerRequest struct {
	Offset uint64 `json:"offset"`
}

type ConsumerResponse struct {
	Record *log_v1.Record `json:"record"`
}

func (h *httpServer) handleConsume(w http.ResponseWriter, r *http.Request) {
	var req ConsumerRequest
	err := json.NewDecoder(r.Body).Decode(&req)
	if err != nil {
		http.Error(w, err.Error(), http.StatusBadRequest)
		return
	}
	record, err := h.Log.Read(req.Offset)
	if e, ok := err.(log_v1.ErrOffsetOutOfRange); ok {
		http.Error(w, e.Error(), http.StatusBadRequest)
		return
	}
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
	res := ConsumerResponse{Record: record}
	err = json.NewEncoder(w).Encode(res)
	if err != nil {
		http.Error(w, err.Error(), http.StatusInternalServerError)
		return
	}
}
