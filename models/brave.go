package models

import "time"

type EventFile struct {
	Events []TelemetryEvent `json:"events"`
}

type TelemetryEvent struct {
	EventID       string    `json:"EventID"`
	EventType     string    `json:"EventType"`
	Source        string    `json:"Source"`
	CorrelationID string    `json:"correlation_id"`
	Timestamp     time.Time `json:"Timestamp"`
	Payload       Payload   `json:"Payload"`
}

type Payload struct {
	FailureType   string  `json:"FailureType"`
	Service       string  `json:"Service"`
	CPUUsage      float64 `json:"CPUUsage"`
	MemoryUsage   float64 `json:"MemoryUsage"`
	ResponseTime  float64 `json:"ResponseTime"`
	ErrorCount    int     `json:"ErrorCount"`
	ServiceStatus string  `json:"ServiceStatus"`
}