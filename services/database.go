package services

import (
	"context"

	"github.com/jackc/pgx/v5"

	"telemetry-collector/models"
)

func InsertTelemetry(conn *pgx.Conn, event *models.TelemetryEvent) error {

	_, err := conn.Exec(
		context.Background(),
		`
		INSERT INTO telemetry
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
			"ServiceStatus"
		)
		VALUES
		($1,$2,$3,$4,$5,$6,$7,$8,$9,$10,$11,$12)
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
	)

	return err
}