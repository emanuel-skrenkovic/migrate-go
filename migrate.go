package migrate

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"os"
	"path"
	"path/filepath"
	"sort"
	"strconv"
	"strings"

	"github.com/eskrenkovic/tql"
)

type Migration struct {
	ID         int    `db:"id"`
	Version    int    `db:"version"`
	Name       string `db:"name"`
	UpScript   string
	DownScript string
}

func Run(ctx context.Context, db *sql.DB, migrationsPath string) error {
	if _, err := os.Stat(migrationsPath); err != nil {
		return err
	}

	entries, err := os.ReadDir(migrationsPath)
	if err != nil {
		return err
	}

	if len(entries) == 0 {
		return nil
	}

	migrations := make(map[int]Migration, 0)

	for _, entry := range entries {
		// Sanity checks - only root directory, needs to have a name by convention
		// Name convention - migrationnumber.name.up.sql
		//                   migrationnumber.name.down.sql
		// Needs to have both up and down!
		filePath := entry.Name()

		if filepath.Ext(filePath) != ".sql" {
			continue
		}

		parts := strings.Split(filePath, ".")
		if len(parts) != 4 {
			// Doesn't match the naming convention.
			continue
		}

		migrationNumber, err := strconv.Atoi(parts[0])
		if err != nil {
			return err
		}

		m := migrations[migrationNumber]
		m.Version = migrationNumber
		m.Name = parts[1]

		// TODO: relative paths
		migrationContent, err := os.ReadFile(path.Join(migrationsPath, filePath))
		if err != nil {
			return err
		}

		migrationScriptType := parts[2]
		switch migrationScriptType {
		case "up":
			m.UpScript = string(migrationContent)
		case "down":
			m.DownScript = string(migrationContent)
		default:
			return fmt.Errorf("uncrecognized script type: '%s'", migrationScriptType)
		}

		migrations[migrationNumber] = m
	}

	if err := validateFoundMigrationFiles(migrations); err != nil {
		return err
	}

	if err := ensureMigrationsSchema(ctx, db); err != nil {
		return err
	}

	const q = `
		SELECT 
		    version
		FROM 
		    schema_migration
		ORDER BY 
		    version DESC
		LIMIT 1;`
	lastAppliedMigrationVersion, err := tql.QueryFirstOrDefault[int](ctx, db, 0, q)
	if err != nil {
		return fmt.Errorf("failed fetching last applied version: %w", err)
	}

	var migrationsToApply []Migration
	for migrationVersion, migration := range migrations {
		if migrationVersion <= lastAppliedMigrationVersion {
			continue
		}

		migrationsToApply = append(migrationsToApply, migration)
	}

	if len(migrationsToApply) == 0 {
		return nil
	}

	sort.Slice(migrationsToApply, func(i, j int) bool {
		return migrationsToApply[i].Version < migrationsToApply[j].Version
	})

	var newlyAppliedMigrations []Migration

	var migrationErr error
	for _, migration := range migrationsToApply {
		txFunc := func(ctx context.Context, tx *sql.Tx) error {
			if _, err = tql.Exec(ctx, tx, migration.UpScript); err != nil {
				return fmt.Errorf("failed running migration '%s' up script: %w", migration.Name, err)
			}

			const stmt = `
			INSERT INTO
				schema_migration (version, name)
			VALUES 
			    ($1, $2);`
			_, err = tql.Exec(ctx, tx, stmt, migration.Version, migration.Name)
			if err != nil {
				return fmt.Errorf("failed inserting migration '%s' into 'schema_migration': %w", migration.Name, err)
			}
			return nil
		}

		txOpts := sql.TxOptions{Isolation: sql.LevelSerializable}
		if migrationErr = tx(ctx, db, &txOpts, txFunc); migrationErr != nil {
			break
		}

		newlyAppliedMigrations = append(newlyAppliedMigrations, migration)
	}

	if migrationErr != nil {
		if err := revertState(ctx, db, newlyAppliedMigrations); err != nil {
			return errors.Join(err, migrationErr)
		}

		return migrationErr
	}

	return nil
}

func validateFoundMigrationFiles(migrations map[int]Migration) error {
	var missingScriptsErr error
	for _, migration := range migrations {
		if migration.DownScript == "" {
			missingScriptsErr = errors.Join(missingScriptsErr, fmt.Errorf("failed to find 'down' script for '%s'", migration.Name))
		}

		if migration.UpScript == "" {
			missingScriptsErr = errors.Join(missingScriptsErr, fmt.Errorf("failed to find 'down' script for '%s'", migration.Name))
		}
	}
	return missingScriptsErr
}

func revertState(ctx context.Context, db *sql.DB, appliedMigrations []Migration) error {
	var revertErr error

	for i := len(appliedMigrations) - 1; i >= 0; i-- {
		migration := appliedMigrations[i]

		txFunc := func(ctx context.Context, tx *sql.Tx) error {
			if _, err := tx.Exec(migration.DownScript); err != nil {
				return err
			}

			_, err := tx.Exec("DELETE FROM schema_migration WHERE version = $1", migration.Version)
			return err
		}

		if revertErr = tx(ctx, db, nil, txFunc); revertErr != nil {
			break
		}
	}

	return revertErr
}

func ensureMigrationsSchema(ctx context.Context, db *sql.DB) error {
	const checkIfSchemaExistsQuery = `
		SELECT 
		    count(table_name)
		FROM 
		    information_schema.tables
		WHERE 
		    table_name = $1;`

	schemas, err := tql.QueryFirst[int](ctx, db, checkIfSchemaExistsQuery, "schema_migration")
	if err != nil {
		return fmt.Errorf("failed fetching if 'schema_migration' table exists: %w", err)
	}

	if schemas > 0 {
		return nil
	}

	const stmt = `
		CREATE TABLE schema_migration (
			id serial PRIMARY KEY,
			name text NOT NULL,
			version integer NOT NULL
		)`

	_, err = tql.Exec(ctx, db, stmt)
	if err != nil {
		return fmt.Errorf("failed creating 'schema_migration' table: %w", err)
	}

	return nil
}
