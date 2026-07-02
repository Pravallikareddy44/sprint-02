package services

import (
    "context"

    "telemetry-collector/db"
    "telemetry-collector/models"
)

func InsertLog(log models.Log) error {

	conn, err := db.Connect()
	if err != nil {
		return err
	}
	defer conn.Close(context.Background())

	_, err = conn.Exec(
		context.Background(),
		`INSERT INTO logs
		(service_name, log_level, message, failure_type, event_time)
		VALUES ($1, $2, $3, $4, $5)`,
		log.ServiceName,
		log.LogLevel,
		log.Message,
		log.FailureType,
		log.EventTime,
	)

	return err
}