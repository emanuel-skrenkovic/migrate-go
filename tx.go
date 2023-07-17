package migrate

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
)

func tx(
	ctx context.Context,
	db *sql.DB,
	opts *sql.TxOptions,
	f func(context.Context, *sql.Tx) error,
) (err error) {
	var tx *sql.Tx

	defer func() {
		p := recover()
		if p == nil {
			return
		}

		err = errors.Join(fmt.Errorf("migrate-go panic: %v", p), err)

		if rollbackErr := tx.Rollback(); rollbackErr != nil {
			err = errors.Join(rollbackErr, err)
		}
	}()

	tx, err = db.BeginTx(ctx, opts)
	if err != nil {
		return err
	}

	if err = f(ctx, tx); err != nil {
		if rollbackErr := tx.Rollback(); rollbackErr != nil {
			err = errors.Join(err, rollbackErr)
		}
	}

	return err
}
