package cransaction

import (
	"context"
	"database/sql"
	"fmt"
	"gorm.io/gorm"
)

var supportedSQLDrivers = []string{"postgres", "mysql"}

type dbKey struct{}

func isSupportedSQLDriver(driver string) bool {
	for _, d := range supportedSQLDrivers {
		if d == driver {
			return true
		}
	}
	return false
}

// ITransaction Interface for transaction
type ITransaction interface {
	// Transaction Start transaction
	Transaction(ctx context.Context, fn func(context.Context) error) error

	// ExecQuery Execute query
	ExecQuery(ctx context.Context, query string, args ...interface{}) (interface{}, error)

	// QueryRow Query single row
	QueryRow(ctx context.Context, query string, args ...interface{}) interface{}

	// QueryRows Query multiple rows
	QueryRows(ctx context.Context, query string, args ...interface{}) (interface{}, error)
}

// RDMSSession Transaction struct for PostgresSQL
type RDMSSession struct {
	db        *sql.DB
	txOptions *sql.TxOptions
	ctx       context.Context
}

// GormSession Transaction struct for Gorm
type GormSession struct {
	db        *gorm.DB
	txOptions *sql.TxOptions
	ctx       context.Context
}

// NewSession Create new session
func NewSession(driverType string, db interface{}, txOptions *sql.TxOptions, ctx context.Context) ITransaction {
	if isSupportedSQLDriver(driverType) {
		return &RDMSSession{
			db:        db.(*sql.DB),
			txOptions: txOptions,
			ctx:       ctx,
		}
	} else if driverType == "gorm" {
		return &GormSession{
			db:        db.(*gorm.DB),
			txOptions: txOptions,
			ctx:       ctx,
		}
	} else {
		panic(fmt.Sprintf("Unsupported driver: %s", driverType))
	}
	return nil
}

func (r *RDMSSession) Transaction(ctx context.Context, fn func(context.Context) error) error {
	tx, err := r.db.BeginTx(ctx, r.txOptions)
	if err != nil {
		return err
	}
	err = fn(context.WithValue(ctx, dbKey{}, tx))
	if err != nil {
		_ = tx.Rollback()
		return err
	}
	return tx.Commit()
}

func (r *RDMSSession) ExecQuery(ctx context.Context, query string, args ...interface{}) (interface{}, error) {
	if tx, ok := ctx.Value(dbKey{}).(*sql.Tx); ok {
		result, err := tx.ExecContext(ctx, query, args...)
		if err != nil {
			return nil, err
		}
		return result, nil
	}
	result, err := r.db.ExecContext(ctx, query, args...)
	if err != nil {
		return nil, err
	}
	return result, nil
}

func (r *RDMSSession) QueryRow(ctx context.Context, query string, args ...interface{}) interface{} {
	if tx, ok := ctx.Value(dbKey{}).(*sql.Tx); ok {
		return tx.QueryRowContext(ctx, query, args...)
	}
	return r.db.QueryRowContext(ctx, query, args...)
}

func (r *RDMSSession) QueryRows(ctx context.Context, query string, args ...interface{}) (interface{}, error) {
	if tx, ok := ctx.Value(dbKey{}).(*sql.Tx); ok {
		return tx.QueryContext(ctx, query, args...)
	}
	return r.db.QueryContext(ctx, query, args...)
}

func (g *GormSession) Transaction(ctx context.Context, fn func(context.Context) error) error {
	tx := g.db.Begin()
	if tx.Error != nil {
		return tx.Error
	}
	err := fn(context.WithValue(ctx, dbKey{}, tx))
	if err != nil {
		tx.Rollback()
		return err
	}
	return tx.Commit().Error
}

func (g *GormSession) ExecQuery(ctx context.Context, query string, args ...interface{}) (interface{}, error) {
	if tx, ok := ctx.Value(dbKey{}).(*gorm.DB); ok {
		return tx.Exec(query, args...), nil
	}
	return g.db.Exec(query, args...), nil
}

func (g *GormSession) QueryRow(ctx context.Context, query string, args ...interface{}) interface{} {
	if tx, ok := ctx.Value(dbKey{}).(*gorm.DB); ok {
		return tx.Raw(query, args...).Row()
	}
	return g.db.Raw(query, args...).Row()
}

func (g *GormSession) QueryRows(ctx context.Context, query string, args ...interface{}) (interface{}, error) {
	if tx, ok := ctx.Value(dbKey{}).(*gorm.DB); ok {
		return tx.Raw(query, args...).Rows()
	}
	return g.db.Raw(query, args...).Rows()
}
