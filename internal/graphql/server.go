package graphql

import (
	"net/http"
	"time"

	"github.com/99designs/gqlgen/graphql/handler"
	"github.com/99designs/gqlgen/graphql/handler/extension"
	"github.com/99designs/gqlgen/graphql/handler/lru"
	"github.com/99designs/gqlgen/graphql/handler/transport"
	"github.com/vektah/gqlparser/v2/ast"
)

// NewHandler builds the /graphql HTTP handler. Introspection is only enabled
// outside production, where it would expose the full schema to anyone.
func NewHandler(store Reader, production bool) http.Handler {
	return NewHandlerWithNow(store, production, time.Now)
}

// NewHandlerWithNow is NewHandler with an injectable clock, for tests.
func NewHandlerWithNow(store Reader, production bool, now func() time.Time) http.Handler {
	resolver := &Resolver{Store: store, Now: now}
	srv := handler.New(NewExecutableSchema(Config{Resolvers: resolver}))
	srv.AddTransport(transport.Options{})
	srv.AddTransport(transport.GET{})
	srv.AddTransport(transport.POST{})
	srv.SetQueryCache(lru.New[*ast.QueryDocument](1000))
	if !production {
		srv.Use(extension.Introspection{})
	}
	return srv
}
