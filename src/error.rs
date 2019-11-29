use simpl::err;

err!(S3Error);
from!(serde_xml::Error);
from!(reqwest::Error);
from!(reqwest::header::InvalidHeaderName);
from!(reqwest::header::InvalidHeaderValue);
from!(std::env::VarError);
from!(ini::ini::Error);
from!(hmac::crypto_mac::InvalidKeyLength);