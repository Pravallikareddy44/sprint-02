package models

import "time"

type Telemetry struct {
	ServiceName   string
	CPUUsage      float64
	MemoryUsage   float64
	DiskUsage     float64
	NetworkUsage  float64
	ErrorRate     float64
	ResponseTime  float64
	ServiceStatus bool
	Timestamp     time.Time
}