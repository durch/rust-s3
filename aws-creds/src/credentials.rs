#![allow(dead_code)]

use crate::error::CredentialsError;
use ini::Ini;
use log::debug;
use serde::{Deserialize, Serialize};
use std::collections::HashMap;
use std::env;
use std::ops::Deref;
use std::path::{Path, PathBuf};
use std::sync::atomic::AtomicU32;
use std::sync::atomic::Ordering;
use std::time::Duration;
use time::OffsetDateTime;
#[cfg(feature = "http-credentials")]
use url::Host;
use url::Url;

#[cfg(feature = "http-credentials")]
fn absolute_lexical_path(path: &Path) -> Result<PathBuf, CredentialsError> {
    if path.is_absolute() {
        Ok(path.to_path_buf())
    } else {
        Ok(env::current_dir()?.join(path))
    }
}

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
///
/// // Use anonymous credentials for public objects
/// let credentials = Credentials::anonymous();
/// ```
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
#[derive(Clone, Serialize, Deserialize)]
pub struct Credentials {
    /// AWS public access key.
    pub access_key: Option<String>,
    /// AWS secret key.
    pub secret_key: Option<String>,
    /// Temporary token issued by AWS service.
    pub security_token: Option<String>,
    pub session_token: Option<String>,
    pub expiration: Option<Rfc3339OffsetDateTime>,
    #[serde(skip)]
    refresh_source: Option<RefreshSource>,
}

#[derive(Clone, Eq, PartialEq)]
enum RefreshSource {
    StsWebIdentity {
        role_arn: String,
        session_name: String,
        token_file: PathBuf,
    },
    #[cfg(feature = "http-credentials")]
    Container {
        endpoint: ContainerEndpoint,
        token: Option<ContainerTokenSource>,
    },
    InstanceMetadata {
        v2: bool,
        not_ec2: bool,
    },
}

impl std::fmt::Debug for RefreshSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("RefreshSource([REDACTED])")
    }
}

#[cfg(feature = "http-credentials")]
#[derive(Clone, Eq, PartialEq)]
enum ContainerEndpoint {
    Relative(Url),
    Full(Url),
}

#[cfg(feature = "http-credentials")]
#[derive(Clone, Eq, PartialEq)]
enum ContainerTokenSource {
    File(PathBuf),
    Environment(String),
}

#[cfg(feature = "http-credentials")]
impl std::fmt::Debug for ContainerTokenSource {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        f.write_str("ContainerTokenSource([REDACTED])")
    }
}

impl PartialEq for Credentials {
    fn eq(&self, other: &Self) -> bool {
        self.access_key == other.access_key
            && self.secret_key == other.secret_key
            && self.security_token == other.security_token
            && self.session_token == other.session_token
            && self.expiration == other.expiration
    }
}

impl Eq for Credentials {}

impl std::fmt::Debug for Credentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        const REDACTED: &str = "[REDACTED]";

        f.debug_struct("Credentials")
            .field("access_key", &self.access_key.as_ref().map(|_| REDACTED))
            .field("secret_key", &self.secret_key.as_ref().map(|_| REDACTED))
            .field(
                "security_token",
                &self.security_token.as_ref().map(|_| REDACTED),
            )
            .field(
                "session_token",
                &self.session_token.as_ref().map(|_| REDACTED),
            )
            .field("expiration", &self.expiration)
            .finish()
    }
}

#[derive(Debug, Copy, Clone, Eq, PartialEq, Serialize, Deserialize)]
#[repr(transparent)]
pub struct Rfc3339OffsetDateTime(#[serde(with = "time::serde::rfc3339")] pub time::OffsetDateTime);

impl From<time::OffsetDateTime> for Rfc3339OffsetDateTime {
    fn from(v: time::OffsetDateTime) -> Self {
        Self(v)
    }
}

impl From<Rfc3339OffsetDateTime> for time::OffsetDateTime {
    fn from(v: Rfc3339OffsetDateTime) -> Self {
        v.0
    }
}

impl Deref for Rfc3339OffsetDateTime {
    type Target = time::OffsetDateTime;

    fn deref(&self) -> &Self::Target {
        &self.0
    }
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "PascalCase")]
pub struct AssumeRoleWithWebIdentityResponse {
    pub assume_role_with_web_identity_result: AssumeRoleWithWebIdentityResult,
    pub response_metadata: ResponseMetadata,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "PascalCase")]
pub struct AssumeRoleWithWebIdentityResult {
    pub subject_from_web_identity_token: String,
    pub audience: String,
    pub assumed_role_user: AssumedRoleUser,
    pub credentials: StsResponseCredentials,
    pub provider: String,
}

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
pub struct StsResponseCredentials {
    pub session_token: String,
    pub secret_access_key: String,
    pub expiration: Rfc3339OffsetDateTime,
    pub access_key_id: String,
}

impl std::fmt::Debug for StsResponseCredentials {
    fn fmt(&self, f: &mut std::fmt::Formatter<'_>) -> std::fmt::Result {
        const REDACTED: &str = "[REDACTED]";

        f.debug_struct("StsResponseCredentials")
            .field("session_token", &REDACTED)
            .field("secret_access_key", &REDACTED)
            .field("expiration", &self.expiration)
            .field("access_key_id", &REDACTED)
            .finish()
    }
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "PascalCase")]
pub struct AssumedRoleUser {
    pub arn: String,
    pub assumed_role_id: String,
}

#[derive(Deserialize, Debug)]
#[serde(rename_all = "PascalCase")]
pub struct ResponseMetadata {
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

#[cfg(feature = "http-credentials")]
fn apply_timeout(builder: attohttpc::RequestBuilder) -> attohttpc::RequestBuilder {
    let timeout_ms = REQUEST_TIMEOUT_MS.load(Ordering::Relaxed);
    if timeout_ms > 0 {
        return builder.timeout(Duration::from_millis(timeout_ms as u64));
    }
    builder
}

/// Sends a GET request to `url` with a request timeout if one was set.
#[cfg(feature = "http-credentials")]
fn http_get(url: &str) -> attohttpc::Result<attohttpc::Response> {
    let builder = apply_timeout(attohttpc::get(url));

    builder.send()
}

#[cfg(feature = "http-credentials")]
fn refresh_sts_from_token_file<F>(
    role_arn: &str,
    session_name: &str,
    token_file: &Path,
    exchange: F,
) -> Result<Credentials, CredentialsError>
where
    F: FnOnce(&str, &str, &str) -> Result<Credentials, CredentialsError>,
{
    let token = std::fs::read_to_string(token_file)?;
    exchange(role_arn, session_name, &token)
}

#[cfg(feature = "http-credentials")]
const ECS_CREDENTIALS_HOST: &str = "169.254.170.2";
#[cfg(feature = "http-credentials")]
const ECS_CREDENTIALS_IPV4: [u8; 4] = [169, 254, 170, 2];
#[cfg(feature = "http-credentials")]
const EKS_POD_IDENTITY_AGENT_IPV4: [u8; 4] = [169, 254, 170, 23];
#[cfg(feature = "http-credentials")]
const EKS_POD_IDENTITY_AGENT_IPV6: [u16; 8] = [0xfd00, 0x0ec2, 0, 0, 0, 0, 0, 0x23];

#[cfg(feature = "http-credentials")]
fn invalid_container_uri() -> CredentialsError {
    CredentialsError::InvalidContainerCredentialsUri
}

#[cfg(feature = "http-credentials")]
fn parse_container_endpoint(
) -> Result<(ContainerEndpoint, Option<ContainerTokenSource>), CredentialsError> {
    let endpoint = if let Some(relative_uri) = env::var_os("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI")
    {
        let relative_uri = relative_uri.to_str().ok_or_else(invalid_container_uri)?;
        let base = Url::parse(&format!("http://{ECS_CREDENTIALS_HOST}"))
            .map_err(|_| invalid_container_uri())?;
        if !relative_uri.starts_with('/') || relative_uri.starts_with("//") {
            return Err(invalid_container_uri());
        }
        let url = base
            .join(relative_uri)
            .map_err(|_| invalid_container_uri())?;
        if url.scheme() != "http"
            || url.host_str() != Some(ECS_CREDENTIALS_HOST)
            || url.port().is_some()
            || !url.username().is_empty()
            || url.password().is_some()
            || url.fragment().is_some()
        {
            return Err(invalid_container_uri());
        }
        ContainerEndpoint::Relative(url)
    } else if let Some(full_uri) = env::var_os("AWS_CONTAINER_CREDENTIALS_FULL_URI") {
        let full_uri = full_uri.to_str().ok_or_else(invalid_container_uri)?;
        ContainerEndpoint::Full(validate_full_container_uri(full_uri)?)
    } else {
        return Err(CredentialsError::NotContainer);
    };

    let token = if matches!(endpoint, ContainerEndpoint::Relative(_)) {
        None
    } else if let Some(path) = env::var_os("AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE") {
        let path = path
            .to_str()
            .ok_or(CredentialsError::InvalidContainerAuthorization)?;
        let path = absolute_lexical_path(Path::new(path))?;
        Some(ContainerTokenSource::File(path))
    } else if let Some(token) = env::var_os("AWS_CONTAINER_AUTHORIZATION_TOKEN") {
        let token = token
            .into_string()
            .map_err(|_| CredentialsError::InvalidContainerAuthorization)?;
        Some(ContainerTokenSource::Environment(token))
    } else {
        None
    };
    Ok((endpoint, token))
}

#[cfg(feature = "http-credentials")]
fn validate_full_container_uri_host(url: &Url) -> Result<(), CredentialsError> {
    if url.username() != "" || url.password().is_some() || url.fragment().is_some() {
        return Err(invalid_container_uri());
    }
    match (url.scheme(), url.host()) {
        ("https", Some(_)) => Ok(()),
        ("http", Some(Host::Domain(host))) if host.eq_ignore_ascii_case("localhost") => Ok(()),
        ("http", Some(Host::Ipv4(ip)))
            if ip.is_loopback()
                || ip.octets() == ECS_CREDENTIALS_IPV4
                || ip.octets() == EKS_POD_IDENTITY_AGENT_IPV4 =>
        {
            Ok(())
        }
        ("http", Some(Host::Ipv6(ip)))
            if ip.is_loopback() || ip.segments() == EKS_POD_IDENTITY_AGENT_IPV6 =>
        {
            Ok(())
        }
        _ => Err(invalid_container_uri()),
    }
}

#[cfg(feature = "http-credentials")]
fn validate_full_container_uri(uri: &str) -> Result<Url, CredentialsError> {
    let mut url = Url::parse(uri).map_err(|_| invalid_container_uri())?;
    let authority = uri
        .split_once("://")
        .map(|(_, rest)| rest.split(['/', '?', '#']).next().unwrap_or_default())
        .ok_or_else(invalid_container_uri)?;
    if authority.contains('@') {
        return Err(invalid_container_uri());
    }
    validate_full_container_uri_host(&url)?;
    if url.scheme() == "http" && url.host_str() == Some("localhost") {
        url.set_host(Some("127.0.0.1"))
            .map_err(|_| invalid_container_uri())?;
    }
    Ok(url)
}

#[cfg(feature = "http-credentials")]
fn read_container_token(source: &ContainerTokenSource) -> Result<String, CredentialsError> {
    let token = match source {
        ContainerTokenSource::File(path) => std::fs::read_to_string(path)?,
        ContainerTokenSource::Environment(token) => token.clone(),
    };
    let token = token.trim_end_matches(['\r', '\n']).to_owned();
    if token.is_empty() || token.contains(['\r', '\n']) {
        return Err(CredentialsError::InvalidContainerAuthorization);
    }
    Ok(token)
}

#[cfg(feature = "http-credentials")]
fn fetch_container_credentials(
    endpoint: &ContainerEndpoint,
    token_source: Option<&ContainerTokenSource>,
) -> Result<Credentials, CredentialsError> {
    let url = match endpoint {
        ContainerEndpoint::Relative(url) | ContainerEndpoint::Full(url) => url,
    };
    let mut request = apply_timeout(attohttpc::get(url.as_str())).follow_redirects(false);
    if url.scheme() == "http" {
        request = request.proxy_settings(attohttpc::ProxySettings::builder().build());
    }
    if let Some(token_source) = token_source {
        let token = read_container_token(token_source)?;
        let mut header = attohttpc::header::HeaderValue::try_from(token.as_str())
            .map_err(|_| CredentialsError::InvalidContainerAuthorization)?;
        header.set_sensitive(true);
        request = request
            .try_header("Authorization", header)
            .map_err(|_| CredentialsError::InvalidContainerAuthorization)?;
    }
    let response = request
        .send()
        .map_err(|_| CredentialsError::ContainerCredentialsRequest)?;
    if !response.is_success() {
        return Err(CredentialsError::UnexpectedStatusCode(
            response.status().as_u16(),
        ));
    }
    let response: CredentialsFromInstanceMetadata = response
        .json()
        .map_err(|_| CredentialsError::ContainerCredentialsRequest)?;
    Ok(Credentials {
        access_key: Some(response.access_key_id),
        secret_key: Some(response.secret_access_key),
        security_token: Some(response.token),
        expiration: Some(response.expiration),
        session_token: None,
        refresh_source: None,
    })
}

impl Credentials {
    pub fn refresh(&mut self) -> Result<(), CredentialsError> {
        self.refresh_with(Self::refresh_from_source)
    }

    fn refresh_with<F>(&mut self, refresh: F) -> Result<(), CredentialsError>
    where
        F: FnOnce(&RefreshSource) -> Result<Self, CredentialsError>,
    {
        if let Some(expiration) = self.expiration {
            if expiration.0 <= OffsetDateTime::now_utc() {
                debug!("Refreshing credentials!");
                let source = self
                    .refresh_source
                    .clone()
                    .ok_or(CredentialsError::NoRefreshSource)?;
                let refreshed = refresh(&source)?;
                *self = refreshed
            }
        }
        Ok(())
    }

    #[cfg(feature = "http-credentials")]
    fn refresh_from_source(source: &RefreshSource) -> Result<Self, CredentialsError> {
        Self::refresh_from_source_with(source, |source| match source {
            RefreshSource::StsWebIdentity {
                role_arn,
                session_name,
                token_file,
            } => refresh_sts_from_token_file(role_arn, session_name, token_file, Self::from_sts),
            #[cfg(feature = "http-credentials")]
            RefreshSource::Container { endpoint, token } => {
                fetch_container_credentials(endpoint, token.as_ref())
            }
            RefreshSource::InstanceMetadata { v2, not_ec2 } => {
                if *v2 {
                    Self::from_instance_metadata_v2(*not_ec2)
                } else {
                    Self::from_instance_metadata(*not_ec2)
                }
            }
        })
    }

    #[cfg(not(feature = "http-credentials"))]
    fn refresh_from_source(_: &RefreshSource) -> Result<Self, CredentialsError> {
        Err(CredentialsError::NoRefreshSource)
    }

    fn refresh_from_source_with<F>(
        source: &RefreshSource,
        refresh: F,
    ) -> Result<Self, CredentialsError>
    where
        F: FnOnce(&RefreshSource) -> Result<Self, CredentialsError>,
    {
        let mut credentials = refresh(source)?;
        credentials.refresh_source = Some(source.clone());
        Ok(credentials)
    }

    #[cfg(feature = "http-credentials")]
    pub fn from_sts_env(session_name: &str) -> Result<Credentials, CredentialsError> {
        let role_arn = env::var("AWS_ROLE_ARN")?;
        let web_identity_token_file = env::var("AWS_WEB_IDENTITY_TOKEN_FILE")?;
        let token_file = absolute_lexical_path(Path::new(&web_identity_token_file))?;
        let web_identity_token = std::fs::read_to_string(&token_file)?;
        let mut credentials = Credentials::from_sts(&role_arn, session_name, &web_identity_token)?;
        credentials.refresh_source = Some(RefreshSource::StsWebIdentity {
            role_arn,
            session_name: session_name.to_owned(),
            token_file,
        });
        Ok(credentials)
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
            quick_xml::de::from_str::<AssumeRoleWithWebIdentityResponse>(&response.text()?)?;
        // assert!(quick_xml::de::from_str::<AssumeRoleWithWebIdentityResponse>(&response.text()?).unwrap());

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
            expiration: Some(
                serde_response
                    .assume_role_with_web_identity_result
                    .credentials
                    .expiration,
            ),
            refresh_source: None,
        })
    }

    #[allow(clippy::should_implement_trait)]
    pub fn default() -> Result<Credentials, CredentialsError> {
        Credentials::new(None, None, None, None, None)
    }

    pub fn anonymous() -> Result<Credentials, CredentialsError> {
        Ok(Credentials {
            access_key: None,
            secret_key: None,
            security_token: None,
            session_token: None,
            expiration: None,
            refresh_source: None,
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
    ) -> Result<Credentials, CredentialsError> {
        if access_key.is_some() {
            return Ok(Credentials {
                access_key: access_key.map(|s| s.to_string()),
                secret_key: secret_key.map(|s| s.to_string()),
                security_token: security_token.map(|s| s.to_string()),
                session_token: session_token.map(|s| s.to_string()),
                expiration: None,
                refresh_source: None,
            });
        }

        let credentials = Credentials::from_env().or_else(|_| Credentials::from_profile(profile));

        #[cfg(feature = "http-credentials")]
        let credentials = credentials
            .or_else(|_| Credentials::from_sts_env("aws-creds"))
            .or_else(|_| Credentials::from_container_credentials_provider())
            .or_else(|_| Credentials::from_instance_metadata_v2(false))
            .or_else(|_| Credentials::from_instance_metadata(false));

        credentials.map_err(|_| CredentialsError::NoCredentials)
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
            expiration: None,
            refresh_source: None,
        })
    }

    pub fn from_env() -> Result<Credentials, CredentialsError> {
        Credentials::from_env_specific(None, None, None, None)
    }

    /// Load credentials from ECS container or EKS Pod Identity metadata.
    ///
    /// `AWS_CONTAINER_CREDENTIALS_RELATIVE_URI` takes precedence over
    /// `AWS_CONTAINER_CREDENTIALS_FULL_URI`. Full URIs may use HTTPS with a
    /// normal host. HTTP is limited to literal loopback, `localhost`, and the
    /// documented ECS/EKS link-local addresses. `localhost` is normalized to
    /// `127.0.0.1`. This intentionally narrower HTTP-host policy avoids sending
    /// authorization tokens to arbitrary hosts; redirects are not followed.
    ///
    /// Full URI authorization uses `AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE`
    /// before `AWS_CONTAINER_AUTHORIZATION_TOKEN`; the selected source is
    /// retained for refresh, and token files are reread on each refresh. When
    /// the relative URI is set, full-URI token settings are ignored.
    ///
    /// # Example
    ///
    /// ```no_run
    /// use awscreds::Credentials;
    /// let credentials = Credentials::from_container_credentials_provider()?;
    /// # Ok::<(), awscreds::error::CredentialsError>(())
    /// ```
    #[cfg(feature = "http-credentials")]
    pub fn from_container_credentials_provider() -> Result<Credentials, CredentialsError> {
        let (endpoint, token) = parse_container_endpoint()?;
        let mut credentials = fetch_container_credentials(&endpoint, token.as_ref())?;
        credentials.refresh_source = Some(RefreshSource::Container { endpoint, token });
        Ok(credentials)
    }

    #[cfg(feature = "http-credentials")]
    pub fn from_instance_metadata(not_ec2: bool) -> Result<Credentials, CredentialsError> {
        if !not_ec2 && !is_ec2() {
            return Err(CredentialsError::NotEc2);
        }

        let role = apply_timeout(attohttpc::get(
            "http://169.254.169.254/latest/meta-data/iam/security-credentials",
        ))
        .send()?
        .text()?;

        let resp: CredentialsFromInstanceMetadata = apply_timeout(attohttpc::get(format!(
            "http://169.254.169.254/latest/meta-data/iam/security-credentials/{}",
            role
        )))
        .send()?
        .json()?;

        Ok(Credentials {
            access_key: Some(resp.access_key_id),
            secret_key: Some(resp.secret_access_key),
            security_token: Some(resp.token),
            expiration: Some(resp.expiration),
            session_token: None,
            refresh_source: Some(RefreshSource::InstanceMetadata { v2: false, not_ec2 }),
        })
    }

    #[cfg(feature = "http-credentials")]
    pub fn from_instance_metadata_v2(not_ec2: bool) -> Result<Credentials, CredentialsError> {
        if !not_ec2 && !is_ec2() {
            return Err(CredentialsError::NotEc2);
        }

        let token = apply_timeout(attohttpc::put("http://169.254.169.254/latest/api/token"))
            .header("X-aws-ec2-metadata-token-ttl-seconds", "21600")
            .send()?;
        if !token.is_success() {
            return Err(CredentialsError::UnexpectedStatusCode(
                token.status().as_u16(),
            ));
        }
        let token = token.text()?;

        let role = apply_timeout(attohttpc::get(
            "http://169.254.169.254/latest/meta-data/iam/security-credentials",
        ))
        .header("X-aws-ec2-metadata-token", &token)
        .send()?
        .text()?;

        let resp: CredentialsFromInstanceMetadata = apply_timeout(attohttpc::get(format!(
            "http://169.254.169.254/latest/meta-data/iam/security-credentials/{}",
            role
        )))
        .header("X-aws-ec2-metadata-token", &token)
        .send()?
        .json()?;

        Ok(Credentials {
            access_key: Some(resp.access_key_id),
            secret_key: Some(resp.secret_access_key),
            security_token: Some(resp.token),
            expiration: Some(resp.expiration),
            session_token: None,
            refresh_source: Some(RefreshSource::InstanceMetadata { v2: true, not_ec2 }),
        })
    }

    /// Load credentials from a specific credentials file.
    ///
    /// This method allows loading AWS credentials from a custom file location,
    /// which is useful when credentials are stored in a non-standard location.
    ///
    /// # Arguments
    ///
    /// * `file` - Path to the credentials file
    /// * `section` - Optional profile name to load (defaults to "default")
    ///
    /// # Example
    ///
    /// ```no_run
    /// use awscreds::Credentials;
    ///
    /// let credentials = Credentials::from_credentials_file(
    ///     "/custom/path/credentials",
    ///     Some("production")
    /// ).unwrap();
    /// ```
    pub fn from_credentials_file<P: AsRef<Path>>(
        file: P,
        section: Option<&str>,
    ) -> Result<Credentials, CredentialsError> {
        let conf = Ini::load_from_file(file.as_ref())?;
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
            expiration: None,
            refresh_source: None,
        };
        Ok(credentials)
    }

    pub fn from_profile(section: Option<&str>) -> Result<Credentials, CredentialsError> {
        // Check for AWS_SHARED_CREDENTIALS_FILE environment variable first
        let profile = if let Ok(path) = env::var("AWS_SHARED_CREDENTIALS_FILE") {
            path
        } else {
            let home_dir = home::home_dir().ok_or(CredentialsError::HomeDir)?;
            format!("{}/.aws/credentials", home_dir.display())
        };
        Credentials::from_credentials_file(&profile, section)
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

#[derive(Deserialize)]
#[serde(rename_all = "PascalCase")]
struct CredentialsFromInstanceMetadata {
    access_key_id: String,
    secret_access_key: String,
    token: String,
    expiration: Rfc3339OffsetDateTime, // TODO fix #163
}

#[cfg(test)]
mod tests {
    use super::*;
    use std::io::{Read, Write};
    use std::net::TcpListener;
    use std::process::{Command, Stdio};
    use std::thread;
    use std::time::{Duration, Instant};
    use tempfile::NamedTempFile;

    #[cfg(feature = "http-credentials")]
    const CONTAINER_TEST_MARKER: &str = "AWS_CREDS_CONTAINER_TEST_CHILD";

    #[cfg(feature = "http-credentials")]
    fn enter_container_test_child(test_name: &str) -> bool {
        if env::var_os(CONTAINER_TEST_MARKER).as_deref() == Some(std::ffi::OsStr::new(test_name)) {
            set_request_timeout(Some(Duration::from_secs(2)));
            return true;
        }
        let mut child = Command::new(env::current_exe().unwrap());
        child
            .args(["--exact", &format!("credentials::tests::{test_name}")])
            .env(CONTAINER_TEST_MARKER, test_name)
            .env_remove("AWS_CONTAINER_CREDENTIALS_RELATIVE_URI")
            .env_remove("AWS_CONTAINER_CREDENTIALS_FULL_URI")
            .env_remove("AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE")
            .env_remove("AWS_CONTAINER_AUTHORIZATION_TOKEN")
            .env_remove("HTTP_PROXY")
            .env_remove("http_proxy")
            .env_remove("HTTPS_PROXY")
            .env_remove("https_proxy")
            .env_remove("ALL_PROXY")
            .env_remove("all_proxy")
            .env_remove("NO_PROXY")
            .env_remove("no_proxy")
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit());
        let mut child = child
            .spawn()
            .expect("spawn isolated container credentials test");
        let deadline = Instant::now() + Duration::from_secs(10);
        let status = loop {
            match child.try_wait() {
                Ok(Some(status)) => break status,
                Ok(None) if Instant::now() < deadline => thread::sleep(Duration::from_millis(10)),
                Ok(None) => {
                    let _ = child.kill();
                    let _ = child.wait();
                    panic!("isolated container credentials test timed out");
                }
                Err(error) => {
                    let _ = child.kill();
                    let _ = child.wait();
                    panic!("could not wait for isolated container credentials test: {error}");
                }
            }
        };
        assert!(
            status.success(),
            "isolated container credentials test failed: {status}"
        );
        false
    }

    #[cfg(feature = "http-credentials")]
    fn local_metadata_server(
        responses: Vec<(u16, &'static str, Option<String>)>,
    ) -> (String, thread::JoinHandle<Vec<String>>) {
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        let address = listener.local_addr().unwrap();
        listener.set_nonblocking(true).unwrap();
        let worker = thread::spawn(move || {
            let mut requests = Vec::new();
            for (status, body, location) in responses {
                let deadline = Instant::now() + Duration::from_secs(5);
                let (mut stream, _) = loop {
                    match listener.accept() {
                        Ok(connection) => break connection,
                        Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                            if Instant::now() >= deadline {
                                panic!("timed out waiting for local metadata request");
                            }
                            thread::sleep(Duration::from_millis(5));
                        }
                        Err(error) => panic!("local metadata accept failed: {error}"),
                    }
                };
                stream
                    .set_read_timeout(Some(Duration::from_secs(2)))
                    .unwrap();
                stream
                    .set_write_timeout(Some(Duration::from_secs(2)))
                    .unwrap();
                let mut request = Vec::new();
                let mut chunk = [0_u8; 512];
                while !request.windows(4).any(|window| window == b"\r\n\r\n") {
                    let count = stream.read(&mut chunk).unwrap();
                    if count == 0 {
                        break;
                    }
                    request.extend_from_slice(&chunk[..count]);
                }
                requests.push(String::from_utf8_lossy(&request).into_owned());
                let reason = match status {
                    200 => "OK",
                    302 => "Found",
                    403 => "Forbidden",
                    _ => "Response",
                };
                let location_header = location
                    .map(|url| format!("Location: {url}\r\n"))
                    .unwrap_or_default();
                write!(
                    stream,
                    "HTTP/1.1 {status} {reason}\r\n{location_header}Content-Type: application/json\r\nContent-Length: {}\r\nConnection: close\r\n\r\n{body}",
                    body.len()
                )
                .unwrap();
            }
            requests
        });
        (format!("http://{address}/credentials"), worker)
    }

    #[cfg(feature = "http-credentials")]
    const METADATA_JSON: &str = r#"{"AccessKeyId":"test-access","SecretAccessKey":"test-secret","Token":"test-session-token","Expiration":"2099-01-01T00:00:00Z"}"#;

    #[cfg(feature = "http-credentials")]
    fn assert_listener_idle(listener: &TcpListener) {
        let deadline = Instant::now() + Duration::from_millis(100);
        loop {
            match listener.accept() {
                Ok(_) => panic!("unexpected connection to isolated test listener"),
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    if Instant::now() >= deadline {
                        return;
                    }
                    thread::sleep(Duration::from_millis(5));
                }
                Err(error) => panic!("isolated listener check failed: {error}"),
            }
        }
    }

    #[cfg(feature = "http-credentials")]
    fn clear_container_env() {
        for key in [
            "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI",
            "AWS_CONTAINER_CREDENTIALS_FULL_URI",
            "AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE",
            "AWS_CONTAINER_AUTHORIZATION_TOKEN",
        ] {
            env::remove_var(key);
        }
    }

    #[cfg(feature = "http-credentials")]
    #[test]
    fn container_credentials_success_refresh_and_proxy_isolation() {
        if !enter_container_test_child("container_credentials_success_refresh_and_proxy_isolation")
        {
            return;
        }
        clear_container_env();
        let (uri, worker) = local_metadata_server(vec![
            (200, METADATA_JSON, None),
            (200, METADATA_JSON, None),
            (200, METADATA_JSON, None),
            (200, METADATA_JSON, None),
        ]);
        let proxy = TcpListener::bind("127.0.0.1:0").unwrap();
        proxy.set_nonblocking(true).unwrap();
        env::set_var(
            "HTTP_PROXY",
            format!("http://{}", proxy.local_addr().unwrap()),
        );
        env::set_var("AWS_CONTAINER_CREDENTIALS_FULL_URI", &uri);
        let mut token_file = NamedTempFile::new().unwrap();
        token_file.write_all(b"first-token\n").unwrap();
        token_file.flush().unwrap();
        let token_path = absolute_lexical_path(token_file.path()).unwrap();
        env::set_var("AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE", token_file.path());
        env::set_var("AWS_CONTAINER_AUTHORIZATION_TOKEN", "ignored-token");

        let mut credentials = Credentials::from_container_credentials_provider().unwrap();
        assert_eq!(credentials.access_key.as_deref(), Some("test-access"));
        assert_eq!(credentials.secret_key.as_deref(), Some("test-secret"));
        assert_eq!(
            credentials.security_token.as_deref(),
            Some("test-session-token")
        );
        assert_eq!(credentials.session_token, None);
        assert_eq!(credentials.expiration.unwrap().year(), 2099);
        assert!(matches!(
            credentials.refresh_source,
            Some(RefreshSource::Container {
                endpoint: ContainerEndpoint::Full(_),
                token: Some(ContainerTokenSource::File(_))
            })
        ));
        let serialized = serde_json::to_string(&credentials).unwrap();
        let debug = format!("{credentials:?}");
        assert_eq!(
            serde_json::from_str::<serde_json::Value>(&serialized)
                .unwrap()
                .as_object()
                .unwrap()
                .len(),
            5
        );
        assert!(!serialized.contains("first-token"));
        assert!(!serialized.contains(&uri));
        assert!(!debug.contains("first-token"));
        assert!(!debug.contains(&uri));

        std::fs::write(&token_path, "rotated-token").unwrap();
        env::set_var(
            "AWS_CONTAINER_CREDENTIALS_FULL_URI",
            "http://untrusted.invalid/changed",
        );
        env::set_var(
            "AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE",
            "/missing/rotated/token",
        );
        env::set_var("AWS_CONTAINER_AUTHORIZATION_TOKEN", "changed-env-token");
        credentials.expiration = Some(OffsetDateTime::from_unix_timestamp(0).unwrap().into());
        credentials.refresh().unwrap();
        assert_eq!(credentials.access_key.as_deref(), Some("test-access"));
        assert_eq!(credentials.secret_key.as_deref(), Some("test-secret"));
        assert_eq!(
            credentials.security_token.as_deref(),
            Some("test-session-token")
        );
        assert_eq!(credentials.expiration.unwrap().year(), 2099);

        env::set_var("AWS_CONTAINER_CREDENTIALS_FULL_URI", &uri);
        env::remove_var("AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE");
        env::set_var("AWS_CONTAINER_AUTHORIZATION_TOKEN", "captured-env-token");
        let mut env_credentials = Credentials::from_container_credentials_provider().unwrap();
        assert!(matches!(
            env_credentials.refresh_source,
            Some(RefreshSource::Container {
                endpoint: ContainerEndpoint::Full(_),
                token: Some(ContainerTokenSource::Environment(_))
            })
        ));
        env::set_var(
            "AWS_CONTAINER_CREDENTIALS_FULL_URI",
            "http://changed.invalid/endpoint",
        );
        env::set_var("AWS_CONTAINER_AUTHORIZATION_TOKEN", "changed-env-token");
        env_credentials.expiration = Some(OffsetDateTime::from_unix_timestamp(0).unwrap().into());
        env_credentials.refresh().unwrap();

        let requests = worker.join().unwrap();
        assert_eq!(requests.len(), 4);
        assert!(requests[0].starts_with("GET /credentials HTTP/1.1"));
        assert!(requests[1].starts_with("GET /credentials HTTP/1.1"));
        assert!(requests[2].starts_with("GET /credentials HTTP/1.1"));
        assert!(requests[3].starts_with("GET /credentials HTTP/1.1"));
        assert!(requests[0]
            .to_ascii_lowercase()
            .contains("authorization: first-token"));
        assert!(requests[1]
            .to_ascii_lowercase()
            .contains("authorization: rotated-token"));
        assert!(requests[2]
            .to_ascii_lowercase()
            .contains("authorization: captured-env-token"));
        assert!(requests[3]
            .to_ascii_lowercase()
            .contains("authorization: captured-env-token"));
        assert!(!requests[0].contains("ignored-token"));
        assert!(!requests[3].contains("changed-env-token"));
        assert_listener_idle(&proxy);
    }

    #[cfg(feature = "http-credentials")]
    #[test]
    fn container_relative_uri_precedes_full_uri_and_ignores_full_token_sources() {
        if !enter_container_test_child(
            "container_relative_uri_precedes_full_uri_and_ignores_full_token_sources",
        ) {
            return;
        }
        clear_container_env();
        env::set_var(
            "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI",
            "/v2/credentials/task",
        );
        env::set_var(
            "AWS_CONTAINER_CREDENTIALS_FULL_URI",
            "http://user@bad.invalid/path#fragment",
        );
        env::set_var(
            "AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE",
            "/missing/token-file",
        );
        env::set_var("AWS_CONTAINER_AUTHORIZATION_TOKEN", "stale-token");

        let (endpoint, token) = parse_container_endpoint().unwrap();
        let ContainerEndpoint::Relative(uri) = endpoint else {
            panic!("relative endpoint did not have precedence");
        };
        assert_eq!(uri.as_str(), "http://169.254.170.2/v2/credentials/task");
        assert!(token.is_none());

        env::set_var(
            "AWS_CONTAINER_CREDENTIALS_RELATIVE_URI",
            "//attacker.invalid/path",
        );
        assert!(matches!(
            parse_container_endpoint(),
            Err(CredentialsError::InvalidContainerCredentialsUri)
        ));
    }

    #[cfg(feature = "http-credentials")]
    #[test]
    fn container_bad_token_file_does_not_fallback_to_environment() {
        if !enter_container_test_child("container_bad_token_file_does_not_fallback_to_environment")
        {
            return;
        }
        clear_container_env();
        let listener = TcpListener::bind("127.0.0.1:0").unwrap();
        listener.set_nonblocking(true).unwrap();
        env::set_var(
            "AWS_CONTAINER_CREDENTIALS_FULL_URI",
            format!("http://{}/credentials", listener.local_addr().unwrap()),
        );
        let mut token_file = NamedTempFile::new().unwrap();
        env::set_var(
            "AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE",
            "/missing/container-token",
        );
        env::set_var("AWS_CONTAINER_AUTHORIZATION_TOKEN", "valid-fallback-token");

        let error = Credentials::from_container_credentials_provider().unwrap_err();
        assert!(matches!(error, CredentialsError::Io(_)));
        env::set_var("AWS_CONTAINER_AUTHORIZATION_TOKEN_FILE", token_file.path());
        token_file.write_all(b"\n").unwrap();
        token_file.flush().unwrap();
        let error = Credentials::from_container_credentials_provider().unwrap_err();
        assert!(matches!(
            error,
            CredentialsError::InvalidContainerAuthorization
        ));
        token_file.as_file_mut().set_len(0).unwrap();
        token_file.write_all(b"first\r\nsecond").unwrap();
        token_file.flush().unwrap();
        let error = Credentials::from_container_credentials_provider().unwrap_err();
        assert!(matches!(
            error,
            CredentialsError::InvalidContainerAuthorization
        ));
        assert_listener_idle(&listener);
    }

    #[cfg(feature = "http-credentials")]
    #[test]
    fn container_status_redirect_and_invalid_header_are_checked() {
        if !enter_container_test_child("container_status_redirect_and_invalid_header_are_checked") {
            return;
        }
        clear_container_env();
        let target = TcpListener::bind("127.0.0.1:0").unwrap();
        target.set_nonblocking(true).unwrap();
        let target_uri = format!("http://{}/redirect-target", target.local_addr().unwrap());
        let (uri, worker) = local_metadata_server(vec![
            (403, METADATA_JSON, None),
            (302, METADATA_JSON, Some(target_uri)),
            (200, r#"{"AccessKeyId":"response-secret-sentinel"}"#, None),
        ]);
        env::set_var("AWS_CONTAINER_CREDENTIALS_FULL_URI", &uri);
        env::set_var("AWS_CONTAINER_AUTHORIZATION_TOKEN", "test-auth-token");
        let error = Credentials::from_container_credentials_provider().unwrap_err();
        assert!(matches!(error, CredentialsError::UnexpectedStatusCode(403)));

        // The second response is a redirect status with valid credential JSON;
        // it must not be followed or accepted as credentials.
        let error = Credentials::from_container_credentials_provider().unwrap_err();
        assert!(matches!(error, CredentialsError::UnexpectedStatusCode(302)));
        env::set_var("AWS_CONTAINER_CREDENTIALS_FULL_URI", &uri);
        let error = Credentials::from_container_credentials_provider().unwrap_err();
        assert!(matches!(
            error,
            CredentialsError::ContainerCredentialsRequest
        ));
        assert!(!error.to_string().contains("response-secret-sentinel"));
        let requests = worker.join().unwrap();
        assert_eq!(requests.len(), 3);
        assert!(requests[0]
            .to_ascii_lowercase()
            .contains("authorization: test-auth-token"));
        assert_listener_idle(&target);

        env::set_var("AWS_CONTAINER_CREDENTIALS_FULL_URI", &uri);
        env::set_var("AWS_CONTAINER_AUTHORIZATION_TOKEN", "bad\nheader-sentinel");
        let error = Credentials::from_container_credentials_provider().unwrap_err();
        assert!(matches!(
            error,
            CredentialsError::InvalidContainerAuthorization
        ));
        assert!(!error.to_string().contains("header-sentinel"));
    }

    #[cfg(feature = "http-credentials")]
    #[test]
    fn container_endpoint_validation_rejects_unsafe_uri_without_echoing_it() {
        for uri in [
            "http://example.com/path?query-secret",
            "http://169.254.170.24/path",
            "http://user@localhost/path",
            "http://localhost@/path",
            "http://localhost/path#fragment-secret",
            "ftp://localhost/path",
        ] {
            let error = validate_full_container_uri(uri).unwrap_err();
            assert!(matches!(
                error,
                CredentialsError::InvalidContainerCredentialsUri
            ));
            assert!(!error.to_string().contains("query-secret"));
            assert!(!error.to_string().contains("fragment-secret"));
        }
        for uri in [
            "https://example.com/credentials",
            "http://localhost:1234/credentials",
            "http://127.0.0.1/credentials",
            "http://[::1]/credentials",
            "http://169.254.170.2/v2/credentials/task",
            "http://169.254.170.23/credentials",
            "http://[fd00:ec2::23]/credentials",
        ] {
            assert!(validate_full_container_uri(uri).is_ok(), "{uri}");
        }
        let normalized = validate_full_container_uri("http://localhost:1234/credentials").unwrap();
        assert_eq!(normalized.host_str(), Some("127.0.0.1"));
        assert_eq!(normalized.port(), Some(1234));
    }

    fn expired_credentials(refresh_source: Option<RefreshSource>) -> Credentials {
        Credentials {
            access_key: Some("old-access".into()),
            secret_key: Some("old-secret".into()),
            security_token: Some("old-security-token".into()),
            session_token: Some("old-session-token".into()),
            expiration: Some(OffsetDateTime::from_unix_timestamp(0).unwrap().into()),
            refresh_source,
        }
    }

    #[test]
    fn expired_credentials_without_source_fail_without_mutation() {
        let mut credentials = expired_credentials(None);
        let original = credentials.clone();

        assert!(matches!(
            credentials.refresh(),
            Err(CredentialsError::NoRefreshSource)
        ));
        assert_eq!(credentials, original);
    }

    #[test]
    fn refresh_preserves_source_and_only_replaces_credentials_on_success() {
        let source = RefreshSource::StsWebIdentity {
            role_arn: "arn:aws:iam::123456789012:role/test".into(),
            session_name: "session".into(),
            token_file: PathBuf::from("/tmp/oidc-token"),
        };
        let mut credentials = expired_credentials(Some(source.clone()));
        let original = credentials.clone();

        let failed = credentials.refresh_with(|_| Err(CredentialsError::NoCredentials));
        assert!(matches!(failed, Err(CredentialsError::NoCredentials)));
        assert_eq!(credentials, original);
        assert_eq!(credentials.refresh_source, Some(source.clone()));

        let refreshed = Credentials::refresh_from_source_with(&source, |_| {
            Ok(Credentials {
                access_key: Some("new-access".into()),
                secret_key: Some("new-secret".into()),
                security_token: None,
                session_token: Some("new-session-token".into()),
                expiration: Some((OffsetDateTime::now_utc() + time::Duration::minutes(5)).into()),
                refresh_source: None,
            })
        })
        .unwrap();

        assert_eq!(refreshed.access_key.as_deref(), Some("new-access"));
        assert_eq!(refreshed.refresh_source, Some(source));
    }

    #[cfg(feature = "http-credentials")]
    #[test]
    fn sts_refresh_rereads_the_captured_token_file() {
        let mut token_file = NamedTempFile::new().unwrap();
        token_file.write_all(b"first-token").unwrap();
        token_file.flush().unwrap();
        let path = absolute_lexical_path(token_file.path()).unwrap();

        for expected_token in ["first-token", "rotated-token"] {
            if expected_token == "rotated-token" {
                std::fs::write(token_file.path(), expected_token).unwrap();
            }
            let refreshed = refresh_sts_from_token_file(
                "arn:aws:iam::123456789012:role/test",
                "test-session",
                &path,
                |role_arn, session_name, token| {
                    assert_eq!(role_arn, "arn:aws:iam::123456789012:role/test");
                    assert_eq!(session_name, "test-session");
                    assert_eq!(token, expected_token);
                    Credentials::anonymous()
                },
            )
            .unwrap();
            assert_eq!(refreshed, Credentials::anonymous().unwrap());
        }
    }

    #[cfg(feature = "http-credentials")]
    #[test]
    fn sts_token_file_paths_are_resolved_lexically() {
        let relative = Path::new("rotating-token-file");
        assert_eq!(
            absolute_lexical_path(relative).unwrap(),
            env::current_dir().unwrap().join(relative)
        );

        let absolute = env::current_dir().unwrap().join("rotating-token-file");
        assert_eq!(absolute_lexical_path(&absolute).unwrap(), absolute);
    }

    #[test]
    fn serialized_credentials_keep_the_five_field_wire_shape() {
        let credentials = expired_credentials(Some(RefreshSource::StsWebIdentity {
            role_arn: "arn:aws:iam::123456789012:role/test".into(),
            session_name: "test-session".into(),
            token_file: PathBuf::from("/tmp/test-token"),
        }));
        let value = serde_json::to_value(&credentials).unwrap();
        let fields = value.as_object().unwrap();
        assert_eq!(fields.len(), 5);
        assert!(fields.contains_key("access_key"));
        assert!(fields.contains_key("secret_key"));
        assert!(fields.contains_key("security_token"));
        assert!(fields.contains_key("session_token"));
        assert!(fields.contains_key("expiration"));

        let decoded: Credentials = serde_json::from_value(value).unwrap();
        assert_eq!(decoded, credentials);
        assert!(decoded.refresh_source.is_none());
    }

    fn create_test_credentials_file(content: &str) -> NamedTempFile {
        let mut file = NamedTempFile::new().unwrap();
        file.write_all(content.as_bytes()).unwrap();
        file.flush().unwrap();
        file
    }

    #[test]
    fn test_from_credentials_file_custom_location() {
        let content = r#"[default]
aws_access_key_id = AKIAIOSFODNN7EXAMPLE
aws_secret_access_key = wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY

[production]
aws_access_key_id = PROD_KEY
aws_secret_access_key = PROD_SECRET
aws_session_token = PROD_SESSION_TOKEN
"#;
        let file = create_test_credentials_file(content);

        // Test default section
        let creds = Credentials::from_credentials_file(file.path(), None).unwrap();
        assert_eq!(creds.access_key.unwrap(), "AKIAIOSFODNN7EXAMPLE");

        // Test custom section
        let creds = Credentials::from_credentials_file(file.path(), Some("production")).unwrap();
        assert_eq!(creds.access_key.unwrap(), "PROD_KEY");
        assert_eq!(creds.session_token.unwrap(), "PROD_SESSION_TOKEN");
    }

    #[test]
    fn test_from_profile_respects_env_var() {
        const CHILD_MARKER: &str = "AWS_CREDS_TEST_PROFILE_CHILD";
        const MARKER_PREFIX: &str = "aws-creds-profile-test-child:";

        let profile_path = env::var_os("AWS_SHARED_CREDENTIALS_FILE");
        let mut expected_marker = std::ffi::OsString::from(MARKER_PREFIX);
        if let Some(path) = profile_path.as_ref() {
            expected_marker.push(path);
        }
        if profile_path.is_some()
            && env::var_os(CHILD_MARKER).as_deref() == Some(expected_marker.as_os_str())
        {
            let creds = Credentials::from_profile(None).unwrap();
            assert_eq!(creds.access_key.unwrap(), "ENV_KEY");
            return;
        }

        let content = r#"[default]
aws_access_key_id = ENV_KEY
aws_secret_access_key = ENV_SECRET
"#;
        let file = create_test_credentials_file(content);

        let mut child_marker = std::ffi::OsString::from(MARKER_PREFIX);
        child_marker.push(file.path().as_os_str());
        let mut child = Command::new(env::current_exe().unwrap())
            .args([
                "--exact",
                "credentials::tests::test_from_profile_respects_env_var",
            ])
            .env("AWS_SHARED_CREDENTIALS_FILE", file.path())
            .env(CHILD_MARKER, child_marker)
            .stdout(Stdio::inherit())
            .stderr(Stdio::inherit())
            .spawn()
            .unwrap();

        let deadline = Instant::now() + Duration::from_secs(10);
        let status = loop {
            match child.try_wait() {
                Ok(Some(status)) => break status,
                Ok(None) if Instant::now() < deadline => {
                    thread::sleep(Duration::from_millis(10));
                }
                Ok(None) => {
                    let _ = child.kill();
                    let _ = child.wait();
                    panic!("credential profile child test timed out");
                }
                Err(error) => {
                    let _ = child.kill();
                    let _ = child.wait();
                    panic!("could not wait for credential profile child test: {error}");
                }
            }
        };
        assert!(
            status.success(),
            "credential profile child test failed: {status}"
        );
    }

    #[test]
    fn test_from_credentials_file_errors() {
        // Test missing file
        let result = Credentials::from_credentials_file("/nonexistent/path", None);
        assert!(result.is_err());

        // Test missing section
        let content = r#"[default]
aws_access_key_id = KEY
aws_secret_access_key = SECRET
"#;
        let file = create_test_credentials_file(content);
        let result = Credentials::from_credentials_file(file.path(), Some("nonexistent"));
        assert!(matches!(
            result.unwrap_err(),
            CredentialsError::ConfigNotFound
        ));
    }

    #[test]
    fn credentials_debug_redacts_keys_and_tokens() {
        let credentials = Credentials {
            access_key: Some("ACCESS_KEY_SENTINEL".into()),
            secret_key: Some("SECRET_KEY_SENTINEL".into()),
            security_token: Some("SECURITY_TOKEN_SENTINEL".into()),
            session_token: Some("SESSION_TOKEN_SENTINEL".into()),
            expiration: Some(OffsetDateTime::from_unix_timestamp(0).unwrap().into()),
            refresh_source: None,
        };

        for rendered in [format!("{credentials:?}"), format!("{credentials:#?}")] {
            for sentinel in [
                "ACCESS_KEY_SENTINEL",
                "SECRET_KEY_SENTINEL",
                "SECURITY_TOKEN_SENTINEL",
                "SESSION_TOKEN_SENTINEL",
            ] {
                assert!(
                    !rendered.contains(sentinel),
                    "debug output leaked a credential"
                );
            }
            for field in [
                "access_key",
                "secret_key",
                "security_token",
                "session_token",
            ] {
                assert!(rendered.contains(field), "debug output omitted {field}");
            }
            assert!(rendered.contains("expiration"));
            assert!(rendered.contains("1970"));
        }
    }

    #[test]
    fn credentials_debug_preserves_absent_option_state() {
        let credentials = Credentials {
            access_key: None,
            secret_key: None,
            security_token: None,
            session_token: None,
            expiration: None,
            refresh_source: None,
        };

        let rendered = format!("{credentials:?}");
        for field in [
            "access_key",
            "secret_key",
            "security_token",
            "session_token",
            "expiration",
        ] {
            assert!(rendered.contains(&format!("{field}: None")));
        }
    }

    #[test]
    fn sts_debug_redacts_nested_credentials() {
        let response = AssumeRoleWithWebIdentityResponse {
            assume_role_with_web_identity_result: AssumeRoleWithWebIdentityResult {
                subject_from_web_identity_token: "SUBJECT_IDENTIFIER_SENTINEL".into(),
                audience: "AUDIENCE_SENTINEL".into(),
                assumed_role_user: AssumedRoleUser {
                    arn: "arn:aws:iam::123456789012:role/example".into(),
                    assumed_role_id: "ROLE_ID_SENTINEL".into(),
                },
                credentials: StsResponseCredentials {
                    session_token: "STS_SESSION_SENTINEL".into(),
                    secret_access_key: "STS_SECRET_SENTINEL".into(),
                    expiration: OffsetDateTime::from_unix_timestamp(0).unwrap().into(),
                    access_key_id: "STS_ACCESS_KEY_SENTINEL".into(),
                },
                provider: "PROVIDER_SENTINEL".into(),
            },
            response_metadata: ResponseMetadata {
                request_id: "REQUEST_ID_SENTINEL".into(),
            },
        };

        for rendered in [format!("{response:?}"), format!("{response:#?}")] {
            for sentinel in [
                "STS_SESSION_SENTINEL",
                "STS_SECRET_SENTINEL",
                "STS_ACCESS_KEY_SENTINEL",
            ] {
                assert!(
                    !rendered.contains(sentinel),
                    "nested debug output leaked {sentinel}"
                );
            }
            for detail in [
                "SUBJECT_IDENTIFIER_SENTINEL",
                "AUDIENCE_SENTINEL",
                "ROLE_ID_SENTINEL",
                "PROVIDER_SENTINEL",
                "REQUEST_ID_SENTINEL",
            ] {
                assert!(
                    rendered.contains(detail),
                    "nested debug output omitted {detail}"
                );
            }
            assert!(rendered.contains("StsResponseCredentials"));
            assert!(rendered.contains("expiration"));
            assert!(rendered.contains("1970"));
        }
    }

    #[test]
    fn credentials_serde_representation_is_unchanged() {
        let credentials = Credentials {
            access_key: Some("ACCESS_KEY_SENTINEL".into()),
            secret_key: Some("SECRET_KEY_SENTINEL".into()),
            security_token: Some("SECURITY_TOKEN_SENTINEL".into()),
            session_token: Some("SESSION_TOKEN_SENTINEL".into()),
            expiration: Some(OffsetDateTime::from_unix_timestamp(0).unwrap().into()),
            refresh_source: None,
        };

        let serialized = serde_json::to_value(&credentials).unwrap();
        assert_eq!(serialized["access_key"], "ACCESS_KEY_SENTINEL");
        assert_eq!(serialized["secret_key"], "SECRET_KEY_SENTINEL");
        assert_eq!(serialized["security_token"], "SECURITY_TOKEN_SENTINEL");
        assert_eq!(serialized["session_token"], "SESSION_TOKEN_SENTINEL");
        assert_eq!(
            serde_json::from_value::<Credentials>(serialized).unwrap(),
            credentials
        );
    }
}

#[cfg(test)]
#[test]
fn test_instance_metadata_creds_deserialization() {
    // As documented here:
    // https://docs.aws.amazon.com/AWSEC2/latest/UserGuide/iam-roles-for-amazon-ec2.html#instance-metadata-security-credentials
    serde_json::from_str::<CredentialsFromInstanceMetadata>(
        r#"
        {
            "Code" : "Success",
            "LastUpdated" : "2012-04-26T16:39:16Z",
            "Type" : "AWS-HMAC",
            "AccessKeyId" : "ASIAIOSFODNN7EXAMPLE",
            "SecretAccessKey" : "wJalrXUtnFEMI/K7MDENG/bPxRfiCYEXAMPLEKEY",
            "Token" : "token",
            "Expiration" : "2017-05-17T15:09:54Z"
        }
    "#,
    )
    .unwrap();
}
