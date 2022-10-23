package postgres

import (
	"database/sql"
	"fmt"

	_ "github.com/lib/pq"
)

const (
	DB_HOST     = "localhost"
	DB_PORT     = "5432"
	DB_NAME     = "oogeo_samples"
	DB_USER     = "postgres"
	DB_PASSWORD = "strange_brew"

	GC_PROJECT = "glossy-chimera-366014"
	GC_DATASET = "gogeo"
	GC_TABLE   = "al_counties"
)

// DB set up
func connect() *sql.DB {
	dbinfo := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable", DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME)
	db, err := sql.Open("postgres", dbinfo)

	checkErr(err)

	return db
}
