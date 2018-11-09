error_chain! {
        types {
            S3Error, ErrorKind, ResultExt, S3Result;
        }
        foreign_links {
            FromUtf8(::std::string::FromUtf8Error);
            IoError(::std::io::Error);
            SerdeXML(::serde_xml::Error);
            Env(::std::env::VarError);
            Ini(::ini::ini::Error);
            Reqwest(::reqwest::Error);
            ReqwestInvalidHeaderName(::reqwest::header::InvalidHeaderName);
            ReqwestInvalidHeaderValue(::reqwest::header::InvalidHeaderValue);
        }
        errors {
            AwsError { info: ::serde_types::AwsError, status: u32, body: String } {
            }
        }
    }
