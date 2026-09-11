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
	// "/v1/indexer" is the legacy prefix from before the token-tracker rename;
	// deployed frontends still call it.
	for _, p := range []string{"/v1/token-tracker", "/v1/indexer"} {
		mux.HandleFunc(p+"/status", h.handleStatus)
		mux.HandleFunc(p+"/stats", h.handleStats)
		mux.HandleFunc(p+"/addresses", h.handleAddresses)
		mux.HandleFunc(p+"/address/", h.handleAddress)
		mux.HandleFunc(p+"/holders/", h.handleHolders)
		mux.HandleFunc(p+"/blocks", h.handleBlocks)
		mux.HandleFunc(p+"/producers", h.handleProducers)
		mux.HandleFunc(p+"/tokens", h.handleTokens)
		mux.HandleFunc(p+"/transfers/", h.handleTransfers)
	}
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

	shutdownDone := make(chan struct{})
	var shutdownErr error
	go func() {
		defer close(shutdownDone)
		<-ctx.Done()
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
		defer cancel()
		if err := s.server.Shutdown(shutdownCtx); err != nil {
			// Draining did not finish in time: close what is left so the caller
			// can safely release resources (the database) afterwards.
			s.server.Close()
			shutdownErr = fmt.Errorf("shutdown did not drain in time, remaining connections closed: %w", err)
		}
	}()

	if err := s.server.ListenAndServe(); err != http.ErrServerClosed {
		return err // listen failure: the caller cancels ctx, which releases the goroutine above
	}
	<-shutdownDone // ListenAndServe returns as soon as Shutdown starts; wait for the drain
	return shutdownErr
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
