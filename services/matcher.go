package services

import "telemetry-collector/models"

func MatchEvent(health *models.Health, events []models.TelemetryEvent) *models.TelemetryEvent {

	for i := range events {

		event := &events[i]

		if event.Payload.Service == health.Service &&
			event.Payload.ServiceStatus == health.Status {

			return event
		}
	}

	return nil
}