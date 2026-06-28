package services

import (
	"encoding/json"
	"net/http"

	"telemetry-collector/models"
)

func GetHealth(url string) (*models.Health, error) {

	resp, err := http.Get(url) // endpoint connecting url
	if err != nil {
		return nil, err
	}
	defer resp.Body.Close()

	var health models.Health

	err = json.NewDecoder(resp.Body).Decode(&health)
	if err != nil {
		return nil, err
	}

	return &health, nil
}