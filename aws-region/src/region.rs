#![allow(dead_code)]

use std::fmt;
use std::str::{self, FromStr};

/// AWS S3 [region identifier](https://docs.aws.amazon.com/general/latest/gr/rande.html#s3_region),
/// passing in custom values is also possible, in that case it is up to you to pass a valid endpoint,
/// otherwise boom will happen :)
///
/// # Example
/// ```
/// use std::str::FromStr;
/// use awsregion::Region;
///
/// // Parse from a string
/// let region: Region = Region::from("us-east-1");
///
/// // Choose region directly
/// let region = Region::EuWest2;
///
/// // Custom region requires valid region name and endpoint
/// let region_name = "nl-ams".to_string();
/// let endpoint = "https://s3.nl-ams.scw.cloud".to_string();
/// let region = Region::Custom { region: region_name, endpoint };
///
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
    /// eu-north-1
    EuNorth1,
    /// eu-central-1
    EuCentral1,
    /// eu-west-1
    EuWest1,
    /// eu-west-2
    EuWest2,
    /// eu-west-3
    EuWest3,
    /// me-south-1
    MeSouth1,
    /// sa-east-1
    SaEast1,
    /// Digital Ocean nyc3
    DoNyc3,
    /// Digital Ocean ams3
    DoAms3,
    /// Digital Ocean sgp1
    DoSgp1,
    /// Digiral Ocean fra1
    DoFra1,
    /// Yandex Object Storage
    Yandex,
    /// Wasabi us-east-1
    WaUsEast1,
    /// Wasabi us-east-2
    WaUsEast2,
    /// Wasabi us-west-1
    WaUsWest1,
    /// Wasabi eu-central-1
    WaEuCentral1,
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
            EuNorth1 => write!(f, "eu-north-1"),
            EuCentral1 => write!(f, "eu-central-1"),
            EuWest1 => write!(f, "eu-west-1"),
            EuWest2 => write!(f, "eu-west-2"),
            EuWest3 => write!(f, "eu-west-3"),
            SaEast1 => write!(f, "sa-east-1"),
            MeSouth1 => write!(f, "me-south-1"),
            DoNyc3 => write!(f, "nyc3"),
            DoAms3 => write!(f, "ams3"),
            DoSgp1 => write!(f, "sgp1"),
            DoFra1 => write!(f, "fra1"),
            Yandex => write!(f, "ru-central1"),
            WaUsEast1 => write!(f, "us-east-1"),
            WaUsEast2 => write!(f, "us-east-2"),
            WaUsWest1 => write!(f, "us-west-1"),
            WaEuCentral1 => write!(f, "eu-central-1"),
            Custom { ref region, .. } => write!(f, "{}", region.to_string()),
        }
    }
}

impl From<&str> for Region {
    fn from(s: &str) -> Region {
        use self::Region::*;
        match s {
            "us-east-1" => UsEast1,
            "us-east-2" => UsEast2,
            "us-west-1" => UsWest1,
            "us-west-2" => UsWest2,
            "ca-central-1" => CaCentral1,
            "ap-south-1" => ApSouth1,
            "ap-northeast-1" => ApNortheast1,
            "ap-northeast-2" => ApNortheast2,
            "ap-northeast-3" => ApNortheast3,
            "ap-southeast-1" => ApSoutheast1,
            "ap-southeast-2" => ApSoutheast2,
            "cn-north-1" => CnNorth1,
            "cn-northwest-1" => CnNorthwest1,
            "eu-north-1" => EuNorth1,
            "eu-central-1" => EuCentral1,
            "eu-west-1" => EuWest1,
            "eu-west-2" => EuWest2,
            "eu-west-3" => EuWest3,
            "sa-east-1" => SaEast1,
            "me-south-1" => MeSouth1,
            "nyc3" => DoNyc3,
            "ams3" => DoAms3,
            "sgp1" => DoSgp1,
            "fra1" => DoFra1,
            "yandex" => Yandex,
            "ru-central1" => Yandex,
            "wa-us-east-1" => WaUsEast1,
            "wa-us-east-2" => WaUsEast2,
            "wa-us-west-1" => WaUsWest1,
            "wa-eu-central-1" => WaEuCentral1,
            x => Custom {
                region: x.to_string(),
                endpoint: x.to_string(),
            },
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
            EuNorth1 => String::from("s3-eu-north-1.amazonaws.com"),
            EuCentral1 => String::from("s3-eu-central-1.amazonaws.com"),
            EuWest1 => String::from("s3-eu-west-1.amazonaws.com"),
            EuWest2 => String::from("s3-eu-west-2.amazonaws.com"),
            EuWest3 => String::from("s3-eu-west-3.amazonaws.com"),
            SaEast1 => String::from("s3-sa-east-1.amazonaws.com"),
            MeSouth1 => String::from("s3-me-south-1.amazonaws.com"),
            DoNyc3 => String::from("nyc3.digitaloceanspaces.com"),
            DoAms3 => String::from("ams3.digitaloceanspaces.com"),
            DoSgp1 => String::from("sgp1.digitaloceanspaces.com"),
            DoFra1 => String::from("fra1.digitaloceanspaces.com"),
            Yandex => String::from("storage.yandexcloud.net"),
            WaUsEast1 => String::from("s3.us-east-1.wasabisys.com"),
            WaUsEast2 => String::from("s3.us-east-2.wasabisys.com"),
            WaUsWest1 => String::from("s3.us-west-1.wasabisys.com"),
            WaEuCentral1 => String::from("s3.eu-central-1.wasabisys.com"),
            Custom { ref endpoint, .. } => endpoint.to_string(),
        }
    }

    pub fn scheme(&self) -> String {
        match *self {
            Region::Custom { ref endpoint, .. } => match endpoint.find("://") {
                Some(pos) => endpoint[..pos].to_string(),
                None => "https".to_string(),
            },
            _ => "https".to_string(),
        }
    }

    pub fn host(&self) -> String {
        match *self {
            Region::Custom { ref endpoint, .. } => match endpoint.find("://") {
                Some(pos) => endpoint[pos + 3..].to_string(),
                None => endpoint.to_string(),
            },
            _ => self.endpoint(),
        }
    }
}

#[test]
fn yandex_object_storage() {
    let yandex = Region::Custom {
        endpoint: "storage.yandexcloud.net".to_string(),
        region: "ru-central1".to_string(),
    };

    let yandex_region = Region::from("ru-central1");

    assert_eq!(yandex.endpoint(), yandex_region.endpoint());

    assert_eq!(yandex.to_string(), yandex_region.to_string());
}
