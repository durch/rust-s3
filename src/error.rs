error_chain! {
        types {
            S3Error, ErrorKind, ResultExt, S3Result;
        }
        foreign_links {
            FromUtf8(::std::string::FromUtf8Error);
            SerdeXML(::serde_xml::Error);
            Curl(::curl::Error);
        }

    }
