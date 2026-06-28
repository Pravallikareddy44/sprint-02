package db

import (
	"context"

	"github.com/jackc/pgx/v5"
)

func Connect() (*pgx.Conn, error) {

	connString := "host=localhost port=5432 user=medapati123DB password=medapati_cloud dbname=medapati_cloud sslmode=disable"

	return pgx.Connect(context.Background(), connString)
}