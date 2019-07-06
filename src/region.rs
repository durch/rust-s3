use std::fmt;
use std::str::{self, FromStr};

use error::{S3Result, S3Error};

/// AWS S3 [region identifier](https://docs.aws.amazon.com/general/latest/gr/rande.html#s3_region),
/// passing in custom values is also possible, in that case it is up to you to pass a valid endpoint,
/// otherwise boom will happen :)
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
#[derive(Clone, Debug, Eq, PartialEq)]
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
    /// ap-northeast-3
    ApNortheast3,
    /// ap-southeast-1
    ApSoutheast1,
    /// ap-southeast-2
    ApSoutheast2,
    /// cn-north-1
    CnNorth1,
    /// cn-northwest-1
    CnNorthwest1,
    /// eu-central-1
    EuCentral1,
    /// eu-west-1
    EuWest1,
    /// eu-west-2
    EuWest2,
    /// eu-west-3
    EuWest3,
    /// sa-east-1
    SaEast1,
    /// Digital Ocean nyc3
    DoNyc3,
    /// Digital Ocean ams3
    DoAms3,
    /// Digital Ocean sgp1
    DoSgp1,
    /// Custom region
    Custom { region: String, endpoint: String },
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
            ApNortheast3 => write!(f, "ap-northeast-3"),
            ApSoutheast1 => write!(f, "ap-southeast-1"),
            ApSoutheast2 => write!(f, "ap-southeast-2"),
            CnNorth1 => write!(f, "cn-north-1"),
            CnNorthwest1 => write!(f, "cn-northwest-1"),
            EuCentral1 => write!(f, "eu-central-1"),
            EuWest1 => write!(f, "eu-west-1"),
            EuWest2 => write!(f, "eu-west-2"),
            EuWest3 => write!(f, "eu-west-3"),
            SaEast1 => write!(f, "sa-east-1"),
            DoNyc3 => write!(f, "nyc3"),
            DoAms3 => write!(f, "ams3"),
            DoSgp1 => write!(f, "sgp1"),
            Custom { ref region, endpoint: _ } => write!(f, "{}", region.to_string())
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
            "ap-northeast-3" => Ok(ApNortheast3),
            "ap-southeast-1" => Ok(ApSoutheast1),
            "ap-southeast-2" => Ok(ApSoutheast2),
            "cn-north-1" => Ok(CnNorth1),
            "cn-northwest-1" => Ok(CnNorthwest1),
            "eu-central-1" => Ok(EuCentral1),
            "eu-west-1" => Ok(EuWest1),
            "eu-west-2" => Ok(EuWest2),
            "eu-west-3" => Ok(EuWest3),
            "sa-east-1" => Ok(SaEast1),
            "nyc3" => Ok(DoNyc3),
            "ams3" => Ok(DoAms3),
            "sgp1" => Ok(DoSgp1),
            x => Ok(Custom{ region: x.to_string(), endpoint: x.to_string() })
        }
    }
}

impl Region {
    pub fn endpoint(&self) -> String {
        use self::Region::*;
        match *self {
            // Surprisingly, us-east-1 does not have a
            // s3-us-east-1.amazonaws.com DNS record
            UsEast1 => String::from("s3.amazonaws.com"),
            UsEast2 => String::from("s3-us-east-2.amazonaws.com"),
            UsWest1 => String::from("s3-us-west-1.amazonaws.com"),
            UsWest2 => String::from("s3-us-west-2.amazonaws.com"),
            CaCentral1 => String::from("s3-ca-central-1.amazonaws.com"),
            ApSouth1 => String::from("s3-ap-south-1.amazonaws.com"),
            ApNortheast1 => String::from("s3-ap-northeast-1.amazonaws.com"),
            ApNortheast2 => String::from("s3-ap-northeast-2.amazonaws.com"),
            ApNortheast3 => String::from("s3-ap-northeast-3.amazonaws.com"),
            ApSoutheast1 => String::from("s3-ap-southeast-1.amazonaws.com"),
            ApSoutheast2 => String::from("s3-ap-southeast-2.amazonaws.com"),
            CnNorth1 => String::from("s3.cn-north-1.amazonaws.com.cn"),
            CnNorthwest1 => String::from("s3.cn-northwest-1.amazonaws.com.cn"),
            EuCentral1 => String::from("s3-eu-central-1.amazonaws.com"),
            EuWest1 => String::from("s3-eu-west-1.amazonaws.com"),
            EuWest2 => String::from("s3-eu-west-2.amazonaws.com"),
            EuWest3 => String::from("s3-eu-west-3.amazonaws.com"),
            SaEast1 => String::from("s3-sa-east-1.amazonaws.com"),
            DoNyc3 => String::from("nyc3.digitaloceanspaces.com"),
            DoAms3 => String::from("ams3.digitaloceanspaces.com"),
            DoSgp1 => String::from("sgp1.digitaloceanspaces.com"),
            Custom { region: _, ref endpoint } => endpoint.to_string()
        }
    }

    pub fn scheme(&self) -> String {
        match *self {
            Region::Custom{ region: _, ref endpoint } => {
                match endpoint.find("://") {
                    Some(pos) => endpoint[..pos].to_string(),
                    None => "https".to_string()
                }
            },
            _ => "https".to_string()
        }
    }

    pub fn host(&self) -> String {
        match *self {
            Region::Custom{ region: _, ref endpoint } => {
                match endpoint.find("://") {
                    Some(pos) => endpoint[pos + 3..].to_string(),
                    None => endpoint.to_string()
                }
            },
            _ => self.endpoint()
        }
    }
}
