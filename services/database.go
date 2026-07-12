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
func GetAllTelemetry(conn *pgx.Conn) ([]models.TelemetryEvent, error) {
	rows, err := conn.Query(
		context.Background(),
		`SELECT
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
    FROM telemetry`,
	)

	if err != nil {
		return nil, err
	}
	defer rows.Close()
	var events []models.TelemetryEvent
	for rows.Next() {
		var event models.TelemetryEvent
		var failureType string
		var service string
		var cpu float64
		var memory float64
		var response float64
		var errorCount int
		var status string
		err := rows.Scan(
			&event.EventID,
			&event.EventType,
			&event.Source,
			&event.CorrelationID,
			&event.Timestamp,
			&failureType,
			&service,
			&cpu,
			&memory,
			&response,
			&errorCount,
			&status,
		)

		if err != nil {
			return nil, err
		}
		event.Payload = models.Payload{
			FailureType:   failureType,
			Service:       service,
			CPUUsage:      cpu,
			MemoryUsage:   memory,
			ResponseTime:  response,
			ErrorCount:    errorCount,
			ServiceStatus: status,
		}

		events = append(events, event)
	}
	return events, nil
}
func GetTelemetryByID(conn *pgx.Conn, id string) (*models.TelemetryEvent, error) {
	row := conn.QueryRow(
		context.Background(),
		`
	SELECT
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
	FROM telemetry
	WHERE "EventID"=$1
	`,
		id,
	)
	var event models.TelemetryEvent

	var failureType string
	var service string
	var cpu float64
	var memory float64
	var response float64
	var errorCount int
	var status string
	err := row.Scan(
		&event.EventID,
		&event.EventType,
		&event.Source,
		&event.CorrelationID,
		&event.Timestamp,
		&failureType,
		&service,
		&cpu,
		&memory,
		&response,
		&errorCount,
		&status,
	)

	if err != nil {
		return nil, err
	}
	event.Payload = models.Payload{
		FailureType:   failureType,
		Service:       service,
		CPUUsage:      cpu,
		MemoryUsage:   memory,
		ResponseTime:  response,
		ErrorCount:    errorCount,
		ServiceStatus: status,
	}
	return &event, nil
}
func GetTelemetryByTime(conn *pgx.Conn, start string, end string) ([]models.TelemetryEvent, error) {

	rows, err := conn.Query(
		context.Background(),
		`
        SELECT
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
        FROM telemetry
        WHERE "Timestamp" BETWEEN $1 AND $2
        `,
		start,
		end,
	)

	if err != nil {
		return nil, err
	}
	defer rows.Close()

	var events []models.TelemetryEvent

	for rows.Next() {
		var event models.TelemetryEvent

		var failureType string
		var service string
		var cpu float64
		var memory float64
		var response float64
		var errorCount int
		var status string

		err := rows.Scan(
			&event.EventID,
			&event.EventType,
			&event.Source,
			&event.CorrelationID,
			&event.Timestamp,
			&failureType,
			&service,
			&cpu,
			&memory,
			&response,
			&errorCount,
			&status,
		)

		if err != nil {
			return nil, err
		}

		event.Payload = models.Payload{
			FailureType:   failureType,
			Service:       service,
			CPUUsage:      cpu,
			MemoryUsage:   memory,
			ResponseTime:  response,
			ErrorCount:    errorCount,
			ServiceStatus: status,
		}

		events = append(events, event)
	}

	return events, nil
}
