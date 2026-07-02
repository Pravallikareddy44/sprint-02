package main

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"os"
	"time"

	"telemetry-collector/db"
	"telemetry-collector/models"
)

func main() {

	// STEP 1: Get data from Health Endpoint

	resp, err := http.Get("http://localhost:5000/health")
	if err != nil {
		panic(err)
	}
	defer resp.Body.Close()

	var health models.Health

	err = json.NewDecoder(resp.Body).Decode(&health)
	if err != nil {
		panic(err)
	}
	fmt.PrintLn("jai ballaya")
	fmt.Println("Health Endpoint Data")
	fmt.Println("-----------------------------")
	fmt.Println("Status    :", health.Status)
	fmt.Println("Service   :", health.Service)
	fmt.Println("Version   :", health.Version)
	fmt.Println("Timestamp :", health.Timestamp)
	fmt.Println()

	// STEP 2: Read events.json
	

	data, err := os.ReadFile("events.json")
	if err != nil {
		panic(err)
	}

	var eventFile models.EventFile

	err = json.Unmarshal(data, &eventFile)
	if err != nil {
		panic(err)
	}

	fmt.Println("Loaded", len(eventFile.Events), "events")

	// STEP 3: Find matching event


	var matched *models.TelemetryEvent

	for i := range eventFile.Events {

		event := &eventFile.Events[i]

		if event.Payload.Service == health.Service &&
			event.Payload.ServiceStatus == health.Status {

			matched = event
			break
		}
	}

	if matched == nil {
		fmt.Println("No matching event found.")
		return
	}

	fmt.Println()
	fmt.Println("Matched Event")
	fmt.Println("-----------------------------")
	fmt.Println("Event ID      :", matched.EventID)
	fmt.Println("Failure Type  :", matched.Payload.FailureType)
	fmt.Println("Service       :", matched.Payload.Service)
	fmt.Println("CPU Usage     :", matched.Payload.CPUUsage)
	fmt.Println("Memory Usage  :", matched.Payload.MemoryUsage)
	fmt.Println("Response Time :", matched.Payload.ResponseTime)
	fmt.Println("Error Count   :", matched.Payload.ErrorCount)
	fmt.Println("Status        :", matched.Payload.ServiceStatus)

	// STEP 4: Convert Timestamp


	var eventTime time.Time

	if health.Timestamp == "" {

		// Endpoint didn't send timestamp
		eventTime = time.Now()

	} else {

		eventTime, err = time.Parse(time.RFC3339, health.Timestamp)
		if err != nil {
			fmt.Println("Invalid endpoint timestamp. Using current time.")
			eventTime = time.Now()
		}
	}


	// STEP 5: Connect PostgreSQL
	

	conn, err := db.Connect()
	if err != nil {
		panic(err)
	}
	defer conn.Close(context.Background())

	fmt.Println()
	fmt.Println("Connected to PostgreSQL")

	
	// STEP 6: Insert into Database
	

	_, err = conn.Exec(
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
		matched.EventID,
		matched.EventType,
		matched.Source,
		matched.CorrelationID,
		eventTime,
		matched.Payload.FailureType,
		matched.Payload.Service,
		matched.Payload.CPUUsage,
		matched.Payload.MemoryUsage,
		matched.Payload.ResponseTime,
		matched.Payload.ErrorCount,
		matched.Payload.ServiceStatus,
	)

	if err != nil {
		panic(err)
	}

	fmt.Println()
	fmt.Println("Telemetry inserted successfully!")
}