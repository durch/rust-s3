/// AWS access credentials: access key, secret key, and optional token.
///
/// # Example
/// ```
/// use s3::credentials::Credentials;
///
/// // Load from environment AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY, and
/// // AWS_SESSION_TOKEN variables
/// // TODO let credentials = Credentials::from_env().unwrap();
///
/// // Load credentials from the standard AWS credentials file with the given
/// // profile name.
/// // TODO let credentials = Credentials::from_profile("default").unwrap();
///
/// // Initialize directly with key ID, secret key, and optional token
/// let credentials = Credentials::new("access_key", "secret_key", Some("token"));
/// ```
#[derive(Clone, Debug, Eq, PartialEq)]
pub struct Credentials {
    /// AWS public access key.
    pub access_key: String,
    /// AWS secret key.
    pub secret_key: String,
    /// Temporary token issued by AWS service.
    pub token: Option<String>,
    _private: (),
}

impl Credentials {
    /// Initialize Credentials directly with key ID, secret key, and optional
    /// token.
    pub fn new(access_key: &str, secret_key: &str, token: Option<&str>) -> Credentials {
        Credentials {
            access_key: access_key.into(),
            secret_key: secret_key.into(),
            token: token.map(|s| s.into()),
            _private: (),
        }
    }
}