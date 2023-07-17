package migrate

import (
	"context"
	"database/sql"
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

		migrationName := parts[1]
		m.Name = migrationName

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
			return fmt.Errorf("uncrecognized script type: %s", migrationScriptType)
		}

		migrations[migrationNumber] = m
	}

	if err := validateFoundMigrationFiles(migrations); err != nil {
		return err
	}

	if err := ensureMigrationsSchema(ctx, db); err != nil {
		return err
	}

	// TODO: find diff between DB and file-defined migrations
	const q = `
		SELECT 
		    *
		FROM 
		    schema_migration
		ORDER BY 
		    version DESC;`
	alreadyAppliedMigrations, err := tql.Query[Migration](ctx, db, q)
	if err != nil {
		return err
	}

	lastAppliedMigrationVersion := 0
	if len(alreadyAppliedMigrations) > 0 {
		lastAppliedMigrationVersion = alreadyAppliedMigrations[0].Version
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
		tx, err := db.BeginTx(ctx, &sql.TxOptions{Isolation: sql.LevelSerializable})
		if err != nil {
			return err
		}

		if _, err = tql.Exec(ctx, tx, migration.UpScript); err != nil {
			migrationErr = err
			func() {
				if err := tx.Rollback(); err != nil {
					migrationErr = fmt.Errorf("failed to roll back transaction: %w", err)
				}
			}()
			break
		}

		const stmt = `
			INSERT INTO
				schema_migration (version, name)
			VALUES 
			    ($1, $2);`
		if _, err = tql.Exec(ctx, tx, stmt, migration.Version, migration.Name); err != nil {
			migrationErr = err
			func() {
				if err := tx.Rollback(); err != nil {
					migrationErr = fmt.Errorf("failed to roll back transaction: %w", err)
				}
			}()
			break
		}

		if err := tx.Commit(); err != nil {
			migrationErr = err
			func() {
				if err := tx.Rollback(); err != nil {
					migrationErr = fmt.Errorf("failed to roll back transaction: %w", err)
				}
			}()
			break
		}

		newlyAppliedMigrations = append(newlyAppliedMigrations, migration)
	}

	if migrationErr != nil {
		if err := revertState(ctx, db, newlyAppliedMigrations); err != nil {
			return fmt.Errorf("%s: %w", err.Error(), migrationErr)
		}
		return migrationErr
	}

	return nil
}

func validateFoundMigrationFiles(migrations map[int]Migration) error {
	var missingScriptsErr error
	for _, migration := range migrations {
		if migration.DownScript == "" {
			return fmt.Errorf("failed to find 'down' script for %s", migration.Name)
		}

		if migration.UpScript == "" {
			return fmt.Errorf("failed to find 'up' script for %s", migration.Name)
		}
	}
	return missingScriptsErr
}

func revertState(ctx context.Context, db *sql.DB, appliedMigrations []Migration) error {
	var rollbackErr error
	for i := len(appliedMigrations) - 1; i >= 0; i-- {
		migration := appliedMigrations[i]

		tx, err := db.BeginTx(ctx, nil)
		if err != nil {
			return err
		}

		if _, err = tx.Exec(migration.DownScript); err != nil {
			func() {
				if err := tx.Rollback(); err != nil {
					rollbackErr = fmt.Errorf("failed to roll back transaction: %w", err)
				}
			}()
			break
		}

		if _, err = tx.Exec("DELETE FROM schema_migration WHERE version = $1", migration.Version); err != nil {
			func() {
				if err := tx.Rollback(); err != nil {
					rollbackErr = fmt.Errorf("failed to roll back transaction: %w", err)
				}
			}()
			break
		}

		if err := tx.Commit(); err != nil {
			func() {
				if err := tx.Rollback(); err != nil {
					rollbackErr = fmt.Errorf("failed to roll back transaction: %w", err)
				}
			}()
			break
		}
	}

	return rollbackErr
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
		return err
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
	return err
}
