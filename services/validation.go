package services

import (
	"errors"
	"strings"
	"time"

	"telemetry-collector/models"
)

func ValidateTelemetry(t models.Telemetry) error {

	var errorsList []string

	// Service Name
	if strings.TrimSpace(t.ServiceName) == "" {
		log := models.Log{
			ServiceName: t.ServiceName,
			LogLevel:    "ERROR",
			Message:     "Service name is required",
			FailureType: "validation",
			EventTime:   time.Now(),
		}
		InsertLog(log)
		errorsList = append(errorsList, "Service name is required")
	}

	// CPU Usage
	if t.CPUUsage < 0 || t.CPUUsage > 100 {
		log := models.Log{
			ServiceName: t.ServiceName,
			LogLevel:    "ERROR",
			Message:     "CPU usage must be between 0 and 100",
			FailureType: "validation",
			EventTime:   time.Now(),
		}
		InsertLog(log)
		errorsList = append(errorsList, "CPU usage must be between 0 and 100")
	}

	// Memory Usage
	if t.MemoryUsage < 0 || t.MemoryUsage > 100 {
		log := models.Log{
			ServiceName: t.ServiceName,
			LogLevel:    "ERROR",
			Message:     "Memory usage must be between 0 and 100",
			FailureType: "validation",
			EventTime:   time.Now(),
		}
		InsertLog(log)
		errorsList = append(errorsList, "Memory usage must be between 0 and 100")
	}

	// Response Time
	if t.ResponseTime < 0 {
		log := models.Log{
			ServiceName: t.ServiceName,
			LogLevel:    "ERROR",
			Message:     "Response time cannot be negative",
			FailureType: "validation",
			EventTime:   time.Now(),
		}
		InsertLog(log)
		errorsList = append(errorsList, "Response time cannot be negative")
	}

	// Timestamp
	if t.Timestamp.IsZero() {
		log := models.Log{
			ServiceName: t.ServiceName,
			LogLevel:    "ERROR",
			Message:     "Timestamp is required",
			FailureType: "validation",
			EventTime:   time.Now(),
		}
		InsertLog(log)
		errorsList = append(errorsList, "Timestamp is required")
	}

	// Return all errors together
	if len(errorsList) > 0 {
		return errors.New(strings.Join(errorsList, "\n"))
	}

	return nil
}