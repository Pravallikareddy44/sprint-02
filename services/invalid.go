package services

import (
	"context"

	"telemetry-collector/db"
	"telemetry-collector/models"
)

func InsertInvalidTelemetry(event models.TelemetryEvent, validationError string) error {

	conn, err := db.Connect()
	if err != nil {
		return err
	}
	defer conn.Close(context.Background())

	_, err = conn.Exec(
		context.Background(),
		`
		INSERT INTO invalid_telemetry
		(
			"EventID",
			"EventType",
			"Source",
			"CorrelationID",
			"Timestamp",
			"FailureType",
			"Service",
			"CPUUsage",
			"MemoryUsage",
			"ResponseTime",
			"ErrorCount",
			"ServiceStatus",
			"ValidationError"
		)
		VALUES
		($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12,$13)
		`,
		event.EventID,
		event.EventType,
		event.Source,
		event.CorrelationID,
		event.Timestamp,
		event.Payload.FailureType,
		event.Payload.Service,
		event.Payload.CPUUsage,
		event.Payload.MemoryUsage,
		event.Payload.ResponseTime,
		event.Payload.ErrorCount,
		event.Payload.ServiceStatus,
		validationError,
	)

	return err
}