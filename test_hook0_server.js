const express = require('express');
const bodyParser = require('body-parser');
const cors = require('cors');

const app = express();
const PORT = 8081;

app.use(cors());
app.use(bodyParser.json());

let eventCount = 0;
const events = [];

// Track consecutive errors to simulate retry scenarios
let consecutiveErrors = 0;

app.post('/events', (req, res) => {
  eventCount++;
  const event = {
    id: eventCount,
    timestamp: new Date().toISOString(),
    body: req.body
  };

  events.push(event);
  console.log(`Received event #${eventCount}:`, req.body);

  // Simulate different response scenarios based on event count
  // and consecutive errors
  if (eventCount === 3 || consecutiveErrors >= 2) {
    // Return 500 error to test retry logic
    consecutiveErrors++;
    console.log(`Simulating server error for event #${eventCount} (error count: ${consecutiveErrors})`);
    return res.status(500).json({ error: 'Internal server error' });
  } else if (eventCount === 5) {
    // Return 400 error to test different error handling
    console.log(`Simulating bad request error for event #${eventCount}`);
    return res.status(400).json({ error: 'Bad request' });
  } else {
    // Reset error count after successful event
    consecutiveErrors = 0;
    return res.status(200).json({ success: true, eventId: eventCount });
  }
});

app.get('/events', (req, res) => {
  res.json(events);
});

app.get('/reset', (req, res) => {
  eventCount = 0;
  events.length = 0;
  consecutiveErrors = 0;
  res.json({ success: true, message: 'Server reset' });
});

console.log(`Mock Hook0 API server listening on port ${PORT}`);
app.listen(PORT);