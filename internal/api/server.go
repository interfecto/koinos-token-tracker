package api

import (
	"context"
	"fmt"
	"net/http"
	"time"

	"github.com/koinos/koinos-token-tracker/internal/store"
)

// Server serves the indexer HTTP REST API.
type Server struct {
	store  store.Store
	port   int
	server *http.Server
}

// NewServer creates a new API server backed by the given store.
func NewServer(s store.Store, port int) *Server {
	return &Server{store: s, port: port}
}

// Start begins serving HTTP requests. It blocks until the context is cancelled.
func (s *Server) Start(ctx context.Context) error {
	mux := http.NewServeMux()

	h := &handlers{store: s.store}
	mux.HandleFunc("/v1/token-tracker/status", h.handleStatus)
	mux.HandleFunc("/v1/token-tracker/stats", h.handleStats)
	mux.HandleFunc("/v1/token-tracker/addresses", h.handleAddresses)
	mux.HandleFunc("/v1/token-tracker/address/", h.handleAddress)
	mux.HandleFunc("/v1/token-tracker/holders/", h.handleHolders)
	mux.HandleFunc("/v1/token-tracker/blocks", h.handleBlocks)
	mux.HandleFunc("/v1/token-tracker/tokens", h.handleTokens)
	mux.HandleFunc("/v1/token-tracker/transfers/", h.handleTransfers)
	mux.HandleFunc("/openapi.json", h.handleOpenAPI)
	mux.HandleFunc("/docs", h.handleDocs)
	mux.HandleFunc("/", h.handleRoot)

	// CORS middleware
	handler := corsMiddleware(mux)

	s.server = &http.Server{
		Addr:         fmt.Sprintf(":%d", s.port),
		Handler:      handler,
		ReadTimeout:  15 * time.Second,
		WriteTimeout: 30 * time.Second,
		IdleTimeout:  60 * time.Second,
	}

	go func() {
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		s.server.Shutdown(shutdownCtx)
	}()

	if err := s.server.ListenAndServe(); err != http.ErrServerClosed {
		return err
	}
	return nil
}

func corsMiddleware(next http.Handler) http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		w.Header().Set("Access-Control-Allow-Origin", "*")
		w.Header().Set("Access-Control-Allow-Methods", "GET, OPTIONS")
		w.Header().Set("Access-Control-Allow-Headers", "Content-Type")
		if r.Method == "OPTIONS" {
			w.WriteHeader(http.StatusOK)
			return
		}
		next.ServeHTTP(w, r)
	})
}
