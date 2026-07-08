package main

import (
	"github.com/nats-io/nats.go"
)

func NatsConnection(*nats.Conn, error) {

	nc, err := nats.Connect("nats://nats:4222")
	if err != nil {
		return nil, err

	}
	return nc, nil

}
