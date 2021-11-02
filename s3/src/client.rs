use crate::{bucket::Bucket, command::Command, request_trait::Request};

pub trait Client<'a> {
    type Request: Request + 'a;

    fn request(&'a self, bucket: &'a Bucket, path: &'a str, command: Command<'a>) -> Self::Request;
}

#[cfg(feature = "with-attohttpc")]
mod attohttpc {
    use super::Client;
    use crate::blocking::AttoRequest;
    use crate::{bucket::Bucket, command::Command};
    use time::OffsetDateTime;

    impl<'a> Client<'a> for attohttpc::Session {
        type Request = AttoRequest<'a>;

        fn request(
            &'a self,
            bucket: &'a Bucket,
            path: &'a str,
            command: Command<'a>,
        ) -> Self::Request {
            AttoRequest {
                session: self,
                bucket,
                path,
                command,
                datetime: OffsetDateTime::now_utc(),
                sync: true,
            }
        }
    }
}

#[cfg(feature = "with-surf")]
mod surf {
    use super::Client;
    use crate::surf_request::SurfRequest;
    use crate::{bucket::Bucket, command::Command};
    use time::OffsetDateTime;

    impl<'a> Client<'a> for surf::Client {
        type Request = SurfRequest<'a>;

        fn request(
            &'a self,
            bucket: &'a Bucket,
            path: &'a str,
            command: Command<'a>,
        ) -> Self::Request {
            SurfRequest {
                client: self,
                bucket,
                path,
                command,
                datetime: OffsetDateTime::now_utc(),
                sync: false,
            }
        }
    }
}

#[cfg(feature = "with-reqwest")]
mod reqwest {
    use super::Client;
    use crate::request::Reqwest;
    use crate::{bucket::Bucket, command::Command};
    use time::OffsetDateTime;

    impl<'a> Client<'a> for reqwest::Client {
        type Request = Reqwest<'a>;

        fn request(
            &'a self,
            bucket: &'a Bucket,
            path: &'a str,
            command: Command<'a>,
        ) -> Self::Request {
            Reqwest {
                client: self,
                bucket,
                path,
                command,
                datetime: OffsetDateTime::now_utc(),
                sync: false,
            }
        }
    }
}
