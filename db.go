package main

import (
	"context"
	"database/sql"
	_ "embed"
	"fmt"
)

//go:embed create_db.sql
var createDBSQL string

func CreateTables(ctx context.Context, db *sql.DB) error {
	if _, err := db.ExecContext(ctx, createDBSQL); err != nil {
		return fmt.Errorf("failed to create tables in main DB: %w", err)
	}
	return nil
}
