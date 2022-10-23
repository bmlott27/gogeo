package postgres

import (
	"database/sql"
	"fmt"

	"github.com/bmlott27/gogeo/utilities"
	_ "github.com/lib/pq"
)

const (
	DB_HOST     = "localhost"
	DB_PORT     = "5432"
	DB_NAME     = "oogeo_samples"
	DB_USER     = "postgres"
	DB_PASSWORD = "strange_brew"
)

// DB set up
func Connect() *sql.DB {
	dbinfo := fmt.Sprintf("host=%s port=%s user=%s password=%s dbname=%s sslmode=disable", DB_HOST, DB_PORT, DB_USER, DB_PASSWORD, DB_NAME)
	db, err := sql.Open("postgres", dbinfo)

	utilities.CheckErr(err)

	return db
}
