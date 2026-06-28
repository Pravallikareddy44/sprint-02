const express = require('express');
const app = express();
const PORT = 5000;

app.get('/health', (req, res) => {
    // Creates a local timestamp string matching your system's timezone
    const localTimestamp = new Date().toLocaleString('sv-SE').replace(' ', 'T') + 'Z';

    // Returning an array containing multiple telemetry data objects
    res.status(200).json(
        {
            "status": "down",
            "service": "auth-service",
            "version": "1.0.0",
            "timestamp": localTimestamp
        });
});

app.listen(PORT, () => {
    console.log(`Server is running on http://localhost:${PORT}`);
});