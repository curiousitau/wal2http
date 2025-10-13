use std::env;

/// Email configuration provider
#[derive(Debug, Clone)]
pub struct EmailConfig {
    pub smtp_host: String,
    pub smtp_port: u16,
    pub smtp_username: String,
    pub smtp_password: String,
    pub from_email: String,
    pub to_email: String,
}

impl EmailConfig {
    /// Validates email configuration from environment variables
    pub fn from_env() -> Result<Self, String> {
        let smtp_host = env::var("EMAIL_SMTP_HOST")
            .map_err(|_| "EMAIL_SMTP_HOST environment variable is missing".to_string())?;
        let smtp_port = env::var("EMAIL_SMTP_PORT")
            .map_err(|_| "EMAIL_SMTP_PORT environment variable is missing".to_string())?
            .parse::<u16>()
            .map_err(|_| "EMAIL_SMTP_PORT must be a valid port number".to_string())?;
        let smtp_username = env::var("EMAIL_SMTP_USERNAME")
            .map_err(|_| "EMAIL_SMTP_USERNAME environment variable is missing".to_string())?;
        let smtp_password = env::var("EMAIL_SMTP_PASSWORD")
            .map_err(|_| "EMAIL_SMTP_PASSWORD environment variable is missing".to_string())?;
        let from_email = env::var("EMAIL_FROM")
            .map_err(|_| "EMAIL_FROM environment variable is missing".to_string())?;
        let to_email = env::var("EMAIL_TO")
            .map_err(|_| "EMAIL_TO environment variable is missing".to_string())?;

        Ok(Self {
            smtp_host,
            smtp_port,
            smtp_username,
            smtp_password,
            from_email,
            to_email,
        })
    }
}
