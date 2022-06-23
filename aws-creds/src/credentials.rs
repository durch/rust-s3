#![allow(dead_code)]
use crate::error::CredentialsError;
use ini::Ini;
use serde::{Deserialize, Serialize};
use serde_xml_rs as serde_xml;
use std::collections::HashMap;
use std::env;
use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering;
use std::time::Duration;
use url::Url;

/// AWS access credentials: access key, secret key, and optional token.
///
/// # Example
///
/// Loads from the standard AWS credentials file with the given profile name,
/// defaults to "default".
///
/// ```no_run
/// # // Do not execute this as it would cause unit tests to attempt to access
/// # // real user credentials.
/// use awscreds::Credentials;
///
/// // Load credentials from `[default]` profile
/// #[cfg(feature="http-credentials")]
/// let credentials = Credentials::default();
///
/// // Also loads credentials from `[default]` profile
/// #[cfg(feature="http-credentials")]
/// let credentials = Credentials::new(None, None, None, None, None);
///
/// // Load credentials from `[my-profile]` profile
/// #[cfg(feature="http-credentials")]
/// let credentials = Credentials::new(None, None, None, None, Some("my-profile".into()));
/// ```
/// // Use anonymous credentials for public objects
/// let credentials = Credentials::anonymous();
///
/// Credentials may also be initialized directly or by the following environment variables:
///
///   - `AWS_ACCESS_KEY_ID`,
///   - `AWS_SECRET_ACCESS_KEY`
///   - `AWS_SESSION_TOKEN`
///
/// The order of preference is arguments, then environment, and finally AWS
/// credentials file.
///
/// ```
/// use awscreds::Credentials;
///
/// // Load credentials directly
/// let access_key = "AKIAIOSFODNN7EXAMPLE";
/// let secret_key = "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY";
/// #[cfg(feature="http-credentials")]
/// let credentials = Credentials::new(Some(access_key), Some(secret_key), None, None, None);
///
/// // Load credentials from the environment
/// use std::env;
/// env::set_var("AWS_ACCESS_KEY_ID", "AKIAIOSFODNN7EXAMPLE");
/// env::set_var("AWS_SECRET_ACCESS_KEY", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY");
/// #[cfg(feature="http-credentials")]
/// let credentials = Credentials::new(None, None, None, None, None);
/// ```
#[derive(Clone, Debug, Eq, PartialEq, Serialize, Deserialize)]
pub struct Credentials {
    /// AWS public access key.
    pub access_key: Option<String>,
    /// AWS secret key.
    pub secret_key: Option<String>,
    /// Temporary token issued by AWS service.
    pub security_token: Option<String>,
    pub session_token: Option<String>,
}

#[derive(Deserialize, Debug)]
pub struct AssumeRoleWithWebIdentityResponse {
    #[serde(rename = "AssumeRoleWithWebIdentityResult")]
    pub assume_role_with_web_identity_result: AssumeRoleWithWebIdentityResult,
    #[serde(rename = "ResponseMetadata")]
    pub response_metadata: ResponseMetadata,
}

#[derive(Deserialize, Debug)]
pub struct AssumeRoleWithWebIdentityResult {
    #[serde(rename = "SubjectFromWebIdentityToken")]
    pub subject_from_web_identity_token: String,
    #[serde(rename = "Audience")]
    pub audience: String,
    #[serde(rename = "AssumedRoleUser")]
    pub assumed_role_user: AssumedRoleUser,
    #[serde(rename = "Credentials")]
    pub credentials: StsResponseCredentials,
    #[serde(rename = "Provider")]
    pub provider: String,
}

#[derive(Deserialize, Debug)]
pub struct StsResponseCredentials {
    #[serde(rename = "SessionToken")]
    pub session_token: String,
    #[serde(rename = "SecretAccessKey")]
    pub secret_access_key: String,
    #[serde(rename = "Expiration")]
    pub expiration: String,
    #[serde(rename = "AccessKeyId")]
    pub access_key_id: String,
}

#[derive(Deserialize, Debug)]
pub struct AssumedRoleUser {
    #[serde(rename = "Arn")]
    pub arn: String,
    #[serde(rename = "AssumedRoleId")]
    pub assumed_role_id: String,
}

#[derive(Deserialize, Debug)]
pub struct ResponseMetadata {
    #[serde(rename = "RequestId")]
    pub request_id: String,
}

/// The global request timeout in milliseconds. 0 means no timeout.
///
/// Defaults to 30 seconds.
static REQUEST_TIMEOUT_MS: AtomicU32 = AtomicU32::new(30_000);

/// Sets the timeout for all credentials HTTP requests and returns the
/// old timeout value, if any; this timeout applies after a 30-second
/// connection timeout.
///
/// Short durations are bumped to one millisecond, and durations
/// greater than 4 billion milliseconds (49 days) are rounded up to
/// infinity (no timeout).
/// The global default value is 30 seconds.
#[cfg(feature = "http-credentials")]
pub fn set_request_timeout(timeout: Option<Duration>) -> Option<Duration> {
    use std::convert::TryInto;
    let duration_ms = timeout
        .as_ref()
        .map(Duration::as_millis)
        .unwrap_or(u128::MAX)
        .max(1); // A 0 duration means infinity.

    // Store that non-zero u128 value in an AtomicU32 by mapping large
    // values to 0: `http_get` maps that to no (infinite) timeout.
    let prev = REQUEST_TIMEOUT_MS.swap(duration_ms.try_into().unwrap_or(0), Ordering::Relaxed);

    if prev == 0 {
        None
    } else {
        Some(Duration::from_millis(prev as u64))
    }
}

/// Sends a GET request to `url` with a request timeout if one was set.
#[cfg(feature = "http-credentials")]
fn http_get(url: &str) -> attohttpc::Result<attohttpc::Response> {
    let mut builder = attohttpc::get(url);

    let timeout_ms = REQUEST_TIMEOUT_MS.load(Ordering::Relaxed);
    if timeout_ms > 0 {
        builder = builder.timeout(Duration::from_millis(timeout_ms as u64));
    }

    builder.send()
}

impl Credentials {
    #[cfg(feature = "http-credentials")]
    pub fn from_sts_env(session_name: &str) -> Result<Credentials, CredentialsError> {
        let role_arn = env::var("AWS_ROLE_ARN")?;
        let web_identity_token_file = env::var("AWS_WEB_IDENTITY_TOKEN_FILE")?;
        let web_identity_token = std::fs::read_to_string(web_identity_token_file)?;
        Credentials::from_sts(&role_arn, session_name, &web_identity_token)
    }

    #[cfg(feature = "http-credentials")]
    pub fn from_sts(
        role_arn: &str,
        session_name: &str,
        web_identity_token: &str,
    ) -> Result<Credentials, CredentialsError> {
        let url = Url::parse_with_params(
            "https://sts.amazonaws.com/",
            &[
                ("Action", "AssumeRoleWithWebIdentity"),
                ("RoleSessionName", session_name),
                ("RoleArn", role_arn),
                ("WebIdentityToken", web_identity_token),
                ("Version", "2011-06-15"),
            ],
        )?;
        let response = http_get(url.as_str())?;
        let serde_response =
            serde_xml::from_str::<AssumeRoleWithWebIdentityResponse>(&response.text()?)?;
        // assert!(serde_xml::from_str::<AssumeRoleWithWebIdentityResponse>(&response.text()?).unwrap());

        Ok(Credentials {
            access_key: Some(
                serde_response
                    .assume_role_with_web_identity_result
                    .credentials
                    .access_key_id,
            ),
            secret_key: Some(
                serde_response
                    .assume_role_with_web_identity_result
                    .credentials
                    .secret_access_key,
            ),
            security_token: None,
            session_token: Some(
                serde_response
                    .assume_role_with_web_identity_result
                    .credentials
                    .session_token,
            ),
        })
    }

    #[cfg(feature = "http-credentials")]
    pub fn default() -> Result<Credentials, CredentialsError> {
        Credentials::new(None, None, None, None, None)
    }

    pub fn anonymous() -> Result<Credentials, CredentialsError> {
        Ok(Credentials {
            access_key: None,
            secret_key: None,
            security_token: None,
            session_token: None,
        })
    }

    /// Initialize Credentials directly with key ID, secret key, and optional
    /// token.
    #[cfg(feature = "http-credentials")]
    pub fn new(
        access_key: Option<&str>,
        secret_key: Option<&str>,
        security_token: Option<&str>,
        session_token: Option<&str>,
        profile: Option<&str>,
    ) -> Result<Credentials, CredentialsError> {
        if access_key.is_some() {
            return Ok(Credentials {
                access_key: access_key.map(|s| s.to_string()),
                secret_key: secret_key.map(|s| s.to_string()),
                security_token: security_token.map(|s| s.to_string()),
                session_token: session_token.map(|s| s.to_string()),
            });
        }

        Credentials::from_sts_env("aws-creds")
            .or_else(|_| Credentials::from_env())
            .or_else(|_| Credentials::from_profile(profile))
            .or_else(|_| Credentials::from_instance_metadata())
    }

    pub fn from_env_specific(
        access_key_var: Option<&str>,
        secret_key_var: Option<&str>,
        security_token_var: Option<&str>,
        session_token_var: Option<&str>,
    ) -> Result<Credentials, CredentialsError> {
        let access_key = from_env_with_default(access_key_var, "AWS_ACCESS_KEY_ID")?;
        let secret_key = from_env_with_default(secret_key_var, "AWS_SECRET_ACCESS_KEY")?;

        let security_token = from_env_with_default(security_token_var, "AWS_SECURITY_TOKEN").ok();
        let session_token = from_env_with_default(session_token_var, "AWS_SESSION_TOKEN").ok();
        Ok(Credentials {
            access_key: Some(access_key),
            secret_key: Some(secret_key),
            security_token,
            session_token,
        })
    }

    pub fn from_env() -> Result<Credentials, CredentialsError> {
        Credentials::from_env_specific(None, None, None, None)
    }

    #[cfg(feature = "http-credentials")]
    pub fn from_instance_metadata() -> Result<Credentials, CredentialsError> {
        #[derive(Deserialize)]
        #[serde(rename_all = "PascalCase")]
        struct Response {
            access_key_id: String,
            secret_access_key: String,
            token: String,
            //expiration: time::OffsetDateTime, // TODO fix #163
        }

        let resp: Response = match env::var("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI") {
            Ok(credentials_path) => {
                // We are on ECS
                attohttpc::get(&format!("http://169.254.170.2{}", credentials_path))
                    .send()?
                    .json()?
            }
            Err(_) => {
                if !is_ec2() {
                    return Err(CredentialsError::NotEc2);
                }

                let role = attohttpc::get(
                    "http://169.254.169.254/latest/meta-data/iam/security-credentials",
                )
                .send()?
                .text()?;

                attohttpc::get(&format!(
                    "http://169.254.169.254/latest/meta-data/iam/security-credentials/{}",
                    role
                ))
                .send()?
                .json()?
            }
        };

        Ok(Credentials {
            access_key: Some(resp.access_key_id),
            secret_key: Some(resp.secret_access_key),
            security_token: Some(resp.token),
            session_token: None,
        })
    }

    pub fn from_profile(section: Option<&str>) -> Result<Credentials, CredentialsError> {
        let home_dir = dirs::home_dir().ok_or(CredentialsError::HomeDir)?;
        let profile = format!("{}/.aws/credentials", home_dir.display());
        let conf = Ini::load_from_file(&profile)?;
        let section = section.unwrap_or("default");
        let data = conf
            .section(Some(section))
            .ok_or(CredentialsError::ConfigNotFound)?;
        let access_key = data
            .get("aws_access_key_id")
            .map(|s| s.to_string())
            .ok_or(CredentialsError::ConfigMissingAccessKeyId)?;
        let secret_key = data
            .get("aws_secret_access_key")
            .map(|s| s.to_string())
            .ok_or(CredentialsError::ConfigMissingSecretKey)?;
        let credentials = Credentials {
            access_key: Some(access_key),
            secret_key: Some(secret_key),
            security_token: data.get("aws_security_token").map(|s| s.to_string()),
            session_token: data.get("aws_session_token").map(|s| s.to_string()),
        };
        Ok(credentials)
    }
}

fn from_env_with_default(var: Option<&str>, default: &str) -> Result<String, CredentialsError> {
    let val = var.unwrap_or(default);
    env::var(val)
        .or_else(|_e| env::var(val))
        .map_err(|_| CredentialsError::MissingEnvVar(val.to_string(), default.to_string()))
}

fn is_ec2() -> bool {
    if let Ok(uuid) = std::fs::read_to_string("/sys/hypervisor/uuid") {
        if uuid.starts_with("ec2") {
            return true;
        }
    }
    if let Ok(vendor) = std::fs::read_to_string("/sys/class/dmi/id/board_vendor") {
        if vendor.starts_with("Amazon EC2") {
            return true;
        }
    }
    false
}
