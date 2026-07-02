package services

import (
	"math"
	"strings"
	"telemetry-collector/models"
)

func NormalizeTelemetry(t *models.Payload) {

	t.Service = strings.TrimSpace(strings.ToLower(t.Service))

	t.FailureType = strings.TrimSpace(strings.ToLower(t.FailureType))

	t.ServiceStatus = strings.TrimSpace(strings.ToLower(t.ServiceStatus))

	t.CPUUsage = math.Round(t.CPUUsage*100) / 100

	t.MemoryUsage = math.Round(t.MemoryUsage*100) / 100

	t.ResponseTime = math.Round(t.ResponseTime*100) / 100
	if t.ErrorCount < 0 {
    t.ErrorCount = 0
}
}