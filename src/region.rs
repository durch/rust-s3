use std::fmt;
use std::str::{self, FromStr};

use error::{S3Result, S3Error};

/// AWS S3 [region identifier](https://docs.aws.amazon.com/general/latest/gr/rande.html#s3_region)
///
/// # Example
/// ```
/// use std::str::FromStr;
/// use s3::region::Region;
///
/// // Parse from a string
/// let region: Region = "us-east-1".parse().unwrap();
///
/// // Choose region directly
/// let region = Region::EuWest2;
/// ```
#[derive(Clone, Copy, Debug, Eq, PartialEq)]
pub enum Region {
    /// us-east-1
    UsEast1,
    /// us-east-2
    UsEast2,
    /// us-west-1
    UsWest1,
    /// us-west-2
    UsWest2,
    /// ca-central-1
    CaCentral1,
    /// ap-south-1
    ApSouth1,
    /// ap-northeast-1
    ApNortheast1,
    /// ap-northeast-2
    ApNortheast2,
    /// ap-southeast-1
    ApSoutheast1,
    /// ap-southeast-2
    ApSoutheast2,
    /// eu-central-1
    EuCentral1,
    /// eu-west-1
    EuWest1,
    /// eu-west-2
    EuWest2,
    /// sa-east-1
    SaEast1,
    /// Digital Ocean nyc3
    DoNyc3,
}

impl fmt::Display for Region {
    fn fmt(&self, f: &mut fmt::Formatter) -> fmt::Result {
        use self::Region::*;
        match *self {
            UsEast1 => write!(f, "us-east-1"),
            UsEast2 => write!(f, "us-east-2"),
            UsWest1 => write!(f, "us-west-1"),
            UsWest2 => write!(f, "us-west-2"),
            CaCentral1 => write!(f, "ca-central-1"),
            ApSouth1 => write!(f, "ap-south-1"),
            ApNortheast1 => write!(f, "ap-northeast-1"),
            ApNortheast2 => write!(f, "ap-northeast-2"),
            ApSoutheast1 => write!(f, "ap-southeast-1"),
            ApSoutheast2 => write!(f, "ap-southeast-2"),
            EuCentral1 => write!(f, "eu-central-1"),
            EuWest1 => write!(f, "eu-west-1"),
            EuWest2 => write!(f, "eu-west-2"),
            SaEast1 => write!(f, "sa-east-1"),
            DoNyc3 => write!(f, "nyc3"),
        }
    }
}

impl FromStr for Region {
    type Err = S3Error;

    fn from_str(s: &str) -> S3Result<Self> {
        use self::Region::*;
        match s {
            "us-east-1" => Ok(UsEast1),
            "us-east-2" => Ok(UsEast2),
            "us-west-1" => Ok(UsWest1),
            "us-west-2" => Ok(UsWest2),
            "ca-central-1" => Ok(CaCentral1),
            "ap-south-1" => Ok(ApSouth1),
            "ap-northeast-1" => Ok(ApNortheast1),
            "ap-northeast-2" => Ok(ApNortheast2),
            "ap-southeast-1" => Ok(ApSoutheast1),
            "ap-southeast-2" => Ok(ApSoutheast2),
            "eu-central-1" => Ok(EuCentral1),
            "eu-west-1" => Ok(EuWest1),
            "eu-west-2" => Ok(EuWest2),
            "sa-east-1" => Ok(SaEast1),
            "nyc3" => Ok(DoNyc3),
            _ => Err(s.to_string().into()),
        }
    }
}

impl Region {
    pub fn endpoint(&self) -> &str {
        use self::Region::*;
        match *self {
            // Surprisingly, us-east-1 does not have a
            // s3-us-east-1.amazonaws.com DNS record
            UsEast1 => "s3.amazonaws.com",
            UsEast2 => "s3-us-east-2.amazonaws.com",
            UsWest1 => "s3-us-west-1.amazonaws.com",
            UsWest2 => "s3-us-west-2.amazonaws.com",
            CaCentral1 => "s3-ca-central-1.amazonaws.com",
            ApSouth1 => "s3-ap-south-1.amazonaws.com",
            ApNortheast1 => "s3-ap-northeast-1.amazonaws.com",
            ApNortheast2 => "s3-ap-northeast-2.amazonaws.com",
            ApSoutheast1 => "s3-ap-southeast-1.amazonaws.com",
            ApSoutheast2 => "s3-ap-southeast-2.amazonaws.com",
            EuCentral1 => "s3-eu-central-1.amazonaws.com",
            EuWest1 => "s3-eu-west-1.amazonaws.com",
            EuWest2 => "s3-eu-west-2.amazonaws.com",
            SaEast1 => "s3-sa-east-1.amazonaws.com",
            DoNyc3 => "nyc3.digitaloceanspaces.com",
        }
    }
}
