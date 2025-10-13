const express = require('express');
const bodyParser = require('body-parser');
const cors = require('cors');

const app = express();
const PORT = 8080;

app.use(cors());
app.use(bodyParser.json());

let eventCount = 0;
const events = [];

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
  if (eventCount === 3) {
    // Return 500 error on the 3rd event to test error handling
    console.log('Simulating server error for event #3');
    return res.status(500).json({ error: 'Internal server error' });
  } else if (eventCount === 5) {
    // Return 400 error on the 5th event to test error handling
    console.log('Simulating bad request error for event #5');
    return res.status(400).json({ error: 'Bad request' });
  }

  return res.status(200).json({ success: true, eventId: eventCount });
});

app.get('/events', (req, res) => {
  res.json(events);
});

console.log(`Mock HTTP server listening on port ${PORT}`);
app.listen(PORT);