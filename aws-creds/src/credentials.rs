#![allow(dead_code)]

use anyhow::anyhow;
use anyhow::Result;
use ini::Ini;
use serde_xml_rs as serde_xml;
use std::collections::HashMap;
use std::env;
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
/// let credentials = Credentials::default();
///
/// // Also loads credentials from `[default]` profile
/// let credentials = Credentials::new(None, None, None, None, None);
///
/// // Load credentials from `[my-profile]` profile
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
/// let credentials = Credentials::new(Some(access_key), Some(secret_key), None, None, None);
///
/// // Load credentials from the environment
/// use std::env;
/// env::set_var("AWS_ACCESS_KEY_ID", "AKIAIOSFODNN7EXAMPLE");
/// env::set_var("AWS_SECRET_ACCESS_KEY", "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY");
/// let credentials = Credentials::new(None, None, None, None, None);
/// ```
#[derive(Clone, Debug, Eq, PartialEq)]
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

impl Credentials {
    pub fn from_sts_env(session_name: &str) -> Result<Credentials> {
        let role_arn = env::var("AWS_ROLE_ARN")?;
        let web_identity_token_file = env::var("AWS_WEB_IDENTITY_TOKEN_FILE")?;
        let web_identity_token = std::fs::read_to_string(web_identity_token_file)?;
        Credentials::from_sts(&role_arn, session_name, &web_identity_token)
    }

    pub fn from_sts(
        role_arn: &str,
        session_name: &str,
        web_identity_token: &str,
    ) -> Result<Credentials> {
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
        let response = attohttpc::get(url.as_str()).send()?;
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

    pub fn default() -> Result<Credentials> {
        Credentials::new(None, None, None, None, None)
    }

    pub fn anonymous() -> Result<Credentials> {
        Ok(Credentials {
            access_key: None,
            secret_key: None,
            security_token: None,
            session_token: None,
        })
    }

    /// Initialize Credentials directly with key ID, secret key, and optional
    /// token.
    pub fn new(
        access_key: Option<&str>,
        secret_key: Option<&str>,
        security_token: Option<&str>,
        session_token: Option<&str>,
        profile: Option<&str>,
    ) -> Result<Credentials> {
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
    ) -> Result<Credentials> {
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

    pub fn from_env() -> Result<Credentials> {
        Credentials::from_env_specific(None, None, None, None)
    }

    pub fn from_instance_metadata() -> Result<Credentials> {
        if !Credentials::is_ec2() {
            return Err(anyhow!("Not an EC2 instance"));
        }
        let mut resp: HashMap<String, String> =
            match env::var("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI") {
                Ok(credentials_path) => Some(
                    attohttpc::get(&format!("http://169.254.170.2{}", credentials_path))
                        .send()?
                        .json()?,
                ),
                Err(_) => {
                    let role = attohttpc::get(
                        "http://169.254.169.254/latest/meta-data/iam/security-credentials",
                    )
                    .send()?
                    .text()?;

                    let creds = attohttpc::get(&format!(
                        "http://169.254.169.254/latest/meta-data/iam/security-credentials/{}",
                        role
                    ))
                    .send()?
                    .json()?;

                    Some(creds)
                }
            }
            .unwrap();

        Ok(Credentials {
            access_key: resp.remove("AccessKeyId"),
            secret_key: resp.remove("SecretAccessKey"),
            security_token: resp.remove("Token"),
            session_token: None,
        })
    }

    fn is_ec2() -> bool {
        if let Ok(uuid) = std::fs::read_to_string("/sys/hypervisor/uuid") {
            if uuid.len() >= 3 && &uuid[..3] == "ec2" {
                return true;
            }
        }
        if let Ok(uuid) = std::fs::read_to_string("/sys/class/dmi/id/board_vendor") {
            if uuid.len() >= 10 && &uuid[..10] == "Amazon EC2" {
                return true;
            }
        }
        false
    }

    pub fn from_profile(section: Option<&str>) -> Result<Credentials> {
        let home_dir = dirs::home_dir().ok_or_else(|| anyhow!("Invalid home dir"))?;
        let profile = format!("{}/.aws/credentials", home_dir.display());
        let conf = Ini::load_from_file(&profile)?;
        let section = section.unwrap_or("default");
        let data = conf
            .section(Some(section))
            .ok_or_else(|| anyhow!("Config missing"))?;
        let access_key = data
            .get("aws_access_key_id")
            .map(|s| s.to_string())
            .ok_or_else(|| anyhow!("Missing aws_access_key_id section"))?;
        let secret_key = data
            .get("aws_secret_access_key")
            .map(|s| s.to_string())
            .ok_or_else(|| anyhow!("Missing aws_secret_access_key section"))?;
        let credentials = Credentials {
            access_key: Some(access_key),
            secret_key: Some(secret_key),
            security_token: data.get("aws_security_token").map(|s| s.to_string()),
            session_token: data.get("aws_session_token").map(|s| s.to_string()),
        };
        Ok(credentials)
    }
}

fn from_env_with_default(var: Option<&str>, default: &str) -> Result<String> {
    let val = var.unwrap_or(default);
    env::var(val).or_else(|_e| env::var(val)).map_err(|_| {
        anyhow!(
            "Neither {:?}, nor {} does not exist in the environment",
            var,
            default
        )
    })
}
