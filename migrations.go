package idempotency

import (
	"io/fs"

	"github.com/fin-assist/go-idempotency/migrations"
)

// The library's tables come with their own migration source. The service lists it in
// MigrationSources() before its own (docs/migrations.md of fin-assist/go-service-template):
//
//	{Name: idempotency.MigrationsName, Table: idempotency.MigrationsTable, FS: idempotency.Migrations},
//	{Name: "service", Table: "schema_migrations", FS: migrations.FS},
//
// The version table is the library's own, so its version numbers never collide with the
// service's. A library without tables deletes this file and the migrations directory.
const (
	// MigrationsName names the source in logs and on the command line
	// (`<service> migrate down library 1`).
	MigrationsName = "idempotency"
	// MigrationsTable is the version table of the source: MigrationsName + "_schema_migrations".
	MigrationsTable = MigrationsName + "_schema_migrations"
)

// Migrations holds the library's NNN_name.up.sql / NNN_name.down.sql files at its root.
var Migrations fs.FS = migrations.FS
