package main

import (
	"github.com/nats-io/nats.go"
	"encoding/json"

	"telemetry-collector/models"
)

func NatsConnection()(*nats.Conn, error) {

	nc, err := nats.Connect("nats://localhost:4222")
	if err != nil {
		return nil, err

	}
	return nc, nil

}
func PublishTelemetry(nc *nats.Conn, event models.TelemetryEvent) error {

	data, err := json.Marshal(event)
	if err != nil {
		return err
	}

	err = nc.Publish("telemetry.events", data)
	if err != nil {
		return err
	}

	return nc.Flush()
}