const express = require('express');
const bodyParser = require('body-parser');
const cors = require('cors');

const app = express();
const PORT = 8082;

app.use(cors());
app.use(bodyParser.json());

const sentEmails = [];

app.post('/send', (req, res) => {
  const email = {
    from: req.body.from,
    to: req.body.to,
    subject: req.body.subject,
    body: req.body.body,
    timestamp: new Date().toISOString()
  };

  sentEmails.push(email);
  console.log('Email sent:', email);

  return res.status(200).json({ success: true });
});

app.get('/emails', (req, res) => {
  res.json(sentEmails);
});

app.get('/reset', (req, res) => {
  sentEmails.length = 0;
  res.json({ success: true, message: 'Email server reset' });
});

console.log(`Mock email server listening on port ${PORT}`);
app.listen(PORT);