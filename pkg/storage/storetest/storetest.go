// Package storetest provides a real Store backed by an in-memory SQLite database for tests.
package storetest

import (
	"context"
	"database/sql"
	"fmt"
	"sync/atomic"
	"testing"

	_ "modernc.org/sqlite"

	"github.com/ZeeingKajama/FreeJobScheduler/pkg/storage/sqlstore"
)

var seq atomic.Int64

// New returns a SQLStore over a private in-memory database with the production schema applied.
// The database is closed when the test finishes.
func New(t testing.TB) *sqlstore.SQLStore {
	t.Helper()

	dsn := fmt.Sprintf("file:storetest_%d?mode=memory&cache=shared&_pragma=busy_timeout(5000)", seq.Add(1))
	db, err := sql.Open("sqlite", dsn)
	if err != nil {
		t.Fatalf("storetest: open sqlite: %v", err)
	}
	t.Cleanup(func() { _ = db.Close() })

	store := sqlstore.NewSQLStore(db)
	if err := store.InitializeSchema(context.Background(), sqlstore.SchemaDDL); err != nil {
		t.Fatalf("storetest: initialize schema: %v", err)
	}
	return store
}
