package main

import (
	"context"
	"encoding/json"
	"fmt"
	"os"
	"strings"
	"telemetry-collector/services"
	"time"

	"telemetry-collector/db"
	"telemetry-collector/models"

	"github.com/nats-io/nats.go"
)

func main() {

	// STEP 1: Get data from Health form nats

	nc,err:= NatsConnection()
	if err!=nil{
		return 
	}

	defer nc.Close()
	var health models.Health
	_, err = nc.Subscribe("infrastructure.health", func(msg *nats.Msg) {

		err := json.Unmarshal(msg.Data, &health)
		if err != nil {
			fmt.Println(err)
			return
		}
     process(nc,health) 
	
	}
)select {}
}
func process(nc *nats.Conn ,health models.Health) {
	fmt.Println("Health Endpoint Data")
	fmt.Println("-----------------------------")
	fmt.Println("Status    :", health.Status)
	fmt.Println("Service   :", health.Service)
	fmt.Println("Version   :", health.Version)
	fmt.Println("Timestamp :", health.Timestamp)
	fmt.Println()

	health.Service = strings.TrimSpace(strings.ToLower(health.Service))
	health.Status = strings.TrimSpace(strings.ToLower(health.Status))

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
	fmt.Println("Before Normalization")
	fmt.Println("Service      :", matched.Payload.Service)
	fmt.Println("Failure Type :", matched.Payload.FailureType)
	fmt.Println("Status       :", matched.Payload.ServiceStatus)
	fmt.Printf("CPU Usage    : %.6f\n", matched.Payload.CPUUsage)
	fmt.Printf("Memory Usage : %.6f\n", matched.Payload.MemoryUsage)
	fmt.Printf("ResponseTime : %.6f\n", matched.Payload.ResponseTime)
	fmt.Println("Error Count  :", matched.Payload.ErrorCount)
	fmt.Println()

	services.NormalizeTelemetry(&matched.Payload)

	fmt.Println("After Normalization")

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

	// Paste the validation code HERE

	telemetry := models.Telemetry{
		ServiceName:   matched.Payload.Service,
		CPUUsage:      matched.Payload.CPUUsage,
		MemoryUsage:   matched.Payload.MemoryUsage,
		ResponseTime:  float64(matched.Payload.ResponseTime),
		ServiceStatus: matched.Payload.ServiceStatus == "up",
		Timestamp:     eventTime,
	}

	err = services.ValidateTelemetry(telemetry)
	if err != nil {
		panic(err)
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

	log := models.Log{
		ServiceName: matched.Payload.Service,
		LogLevel:    "INFO",
		Message:     "Telemetry inserted successfully",
		FailureType: matched.Payload.FailureType,
		EventTime:   eventTime,
	}

	err = services.InsertLog(log)
	if err != nil {
		fmt.Println("Failed to insert log:", err)
	} else {
		fmt.Println("Log inserted successfully!")
	}
	fmt.Println("\nMetrics Table:")

	rows, err := conn.Query(context.Background(), "SELECT * FROM telemetry")
	if err != nil {
		panic(err)
	}
	defer rows.Close()
	fmt.Println("\n================ METRICS TABLE================")
	fmt.Printf("%-10s %-15s %-8s %-8s %-8s %-8s %-10s\n",
		"EventID", "Service", "CPU", "Memory", "Resp", "Errors", "Status")
	fmt.Println("--------------------------------------------------------------------------")
	for rows.Next() {
		var eventID, eventType, source, correlationID string
		var timestamp time.Time
		var failureType, service, status string
		var cpuUsage, memoryUsage, responseTime float64
		var errorCount int

		err = rows.Scan(
			&eventID,
			&eventType,
			&source,
			&correlationID,
			&timestamp,
			&failureType,
			&service,
			&cpuUsage,
			&memoryUsage,
			&responseTime,
			&errorCount,
			&status,
		)

		if err != nil {
			panic(err)
		}

		fmt.Printf("%-10s %-15s %-8.2f %-8.2f %-8.2f %-8d %-10s\n",
			eventID,
			service,
			cpuUsage,
			memoryUsage,
			responseTime,
			errorCount,
			status,
		)
	}

	fmt.Println("\nLogs Table:")

	rows2, err := conn.Query(context.Background(), "SELECT * FROM logs")
	if err != nil {
		panic(err)
	}
	defer rows2.Close()
	fmt.Println("\n================== LOGS TABLE ==================")
	fmt.Printf("%-4s %-18s %-8s %-35s %-18s %-20s\n",
		"ID", "Service", "Level", "Message", "Failure Type", "Event Time")
	fmt.Println("---------------------------------------------------------------------------------------------------------------")
	for rows2.Next() {
		var id int
		var serviceName, logLevel, message, failureType string
		var eventTime time.Time

		err = rows2.Scan(
			&id,
			&serviceName,
			&logLevel,
			&message,
			&failureType,
			&eventTime,
		)

		if err != nil {
			panic(err)
		}

		fmt.Printf("%-4d %-18s %-8s %-35s %-18s %-20s\n",
			id,
			serviceName,
			logLevel,
			message,
			failureType,
			eventTime.Format("2006-01-02 15:04:05"),
		)
	}
	data,err :=json.Marshal(matched)
	if err!=nil{
		fmt.Println("faild to convert telemetry event",err)
     return 
	}
	err=nc.Publish("telemetry.events",data)
	if err!=nil{
		fmt.Println("failed to publish telemetry")

	}
	fmt.Println("telemetry event published sucessfullly")


}
