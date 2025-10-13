use async_trait::async_trait;
use lettre::transport::smtp::authentication::Credentials;

use lettre::{SmtpTransport, Transport};

use lettre::address::Address;

use lettre::Message;
use reqwest::Client;
use tracing::{debug, error};

use super::EventSink;

use crate::email_config::EmailConfig;
use crate::errors::ReplicationResult;
use crate::event_sink::event_formatter;
use crate::types::ReplicationMessage;

use tokio::sync::Mutex;

use std::sync::Arc;

/// HTTP event sink configuration
#[derive(Debug, Clone)]
pub struct HttpEventSinkConfig {
    /// URL of the HTTP endpoint to send events to
    pub endpoint_url: String,
}

/// HTTP event sink for sending replication events to an external HTTP endpoint
#[derive(Clone)]
pub struct HttpEventSink {
    pub(crate) config: HttpEventSinkConfig,
    pub(crate) http_client: Arc<Mutex<Client>>,
    pub(crate) email_config: Option<EmailConfig>,
}

#[async_trait]
impl EventSink for HttpEventSink {
    /// Send a replication event to the HTTP endpoint
    async fn send_event(&self, event: &ReplicationMessage) -> ReplicationResult<()> {
        let json_event = event_formatter::EventFormatter::format(event);

        // Retry configuration
        let max_retries = 5;
        let base_delay_ms = 1000; // 1 second
        let max_delay_ms = 30000; // 30 seconds

        let mut attempt = 0;
        let mut delay_ms = base_delay_ms;

        loop {
            attempt += 1;

            let response = self
                .http_client
                .lock()
                .await
                .post(&self.config.endpoint_url)
                .json(&json_event)
                .send()
                .await;

            match response {
                Ok(resp) => {
                    if resp.status().is_success() {
                        debug!("Successfully sent event to HTTP endpoint");
                        return Ok(());
                    } else {
                        error!("Failed to send event to HTTP endpoint: {}", resp.status());
                        if attempt >= max_retries {
                            // Send email notification but log error instead of panicking
                            self.send_email_notification(&format!(
                                "Failed to send replication event after {} attempts. HTTP endpoint returned status: {}",
                                max_retries,
                                resp.status()
                            )).await;
                            error!(
                                "Failed to send replication event after {} attempts. HTTP endpoint returned status: {}",
                                max_retries,
                                resp.status()
                            );
                            return Err(crate::errors::ReplicationError::Sink {
                                message: format!(
                                    "HTTP endpoint failed after {} retries with status: {}",
                                    max_retries,
                                    resp.status()
                                ),
                                sink: "http".to_string(),
                            });
                        }
                        error!(
                            "HTTP request failed with status {}, retrying in {}ms (attempt {}/{})",
                            resp.status(),
                            delay_ms,
                            attempt,
                            max_retries
                        );
                    }
                }
                Err(e) => {
                    error!("HTTP request failed: {}", e);
                    if attempt >= max_retries {
                        // Send email notification but log error instead of panicking
                        self.send_email_notification(&format!(
                            "Failed to send replication event after {} attempts. HTTP request failed: {}",
                            max_retries,
                            e
                        )).await;
                        error!(
                            "Failed to send replication event after {} attempts. HTTP request failed: {}",
                            max_retries, e
                        );
                        return Err(crate::errors::ReplicationError::Sink {
                            message: format!(
                                "HTTP request failed after {} retries: {}",
                                max_retries, e
                            ),
                            sink: "http".to_string(),
                        });
                    }
                    error!(
                        "HTTP request failed, retrying in {}ms (attempt {}/{})",
                        delay_ms, attempt, max_retries
                    );
                }
            }

            // Exponential backoff with jitter
            tokio::time::sleep(tokio::time::Duration::from_millis(delay_ms)).await;

            // Increase delay for next attempt (exponential backoff)
            delay_ms = (delay_ms * 2).min(max_delay_ms);
        }
    }
}

impl HttpEventSink {
    /// Create a new HTTP event sink
    pub fn new(config: HttpEventSinkConfig) -> Result<Self, String> {
        // Validate email configuration at startup
        let email_config = match EmailConfig::from_env() {
            Ok(email_config) => Some(email_config),
            Err(e) => {
                tracing::warn!(
                    "Email configuration not found, email notifications will be disabled: {}",
                    e
                );
                None
            }
        };

        let http_client = Arc::new(Mutex::new(Client::new()));
        Ok(Self {
            config,
            http_client,
            email_config,
        })
    }

    /// Send an email notification about a failure
    pub(crate) async fn send_email_notification(&self, message: &str) {
        // If email config is not available, return early
        let email_config = match &self.email_config {
            Some(config) => config,
            None => {
                error!("Email configuration not available, cannot send notification");
                return;
            }
        };

        // Build the email message
        let email = Message::builder()
            .from(
                email_config
                    .from_email
                    .parse::<Address>()
                    .expect("Invalid from email address")
                    .into(),
            )
            .to(email_config
                .to_email
                .parse::<Address>()
                .expect("Invalid to email address")
                .into())
            .subject("Replication Event Failure")
            .body(message.to_string())
            .unwrap();

        // Create SMTP transport
        let mailer = SmtpTransport::builder_dangerous(email_config.smtp_host.as_str())
            .port(email_config.smtp_port)
            .credentials(Credentials::new(
                email_config.smtp_username.clone(),
                email_config.smtp_password.clone(),
            ))
            .build();

        // Send the email
        if let Err(e) = mailer.send(&email) {
            error!("Failed to send email notification: {}", e);
        } else {
            debug!("Email notification sent successfully");
        }
    }
}
