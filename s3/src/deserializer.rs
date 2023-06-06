use serde::de::*;

pub fn bool_deserializer<'de, D>(d: D) -> Result<bool, D::Error>
where
    D: Deserializer<'de>,
{
    let s = String::deserialize(d)?;
    match &s[..] {
        "true" => Ok(true),
        "false" => Ok(false),
        other => Err(D::Error::custom(format!(
            "got {}, but expected `true` or `false`",
            other
        ))),
    }
}

pub fn maybe_datetime_deserializer<'de, D>(
    d: D,
) -> Result<Option<crate::serde_types::DateTime>, D::Error>
where
    D: Deserializer<'de>,
{
    match Option::<String>::deserialize(d)? {
        Some(s) => chrono::DateTime::parse_from_rfc2822(&s)
            .map(|parsed| Some(parsed.with_timezone(&chrono::Utc)))
            .map_err(|err| D::Error::custom(format!("Datetime parse from rfc2822 error: {}", err))),
        None => Ok(None),
    }
}

pub fn datetime_deserializer<'de, D>(d: D) -> Result<crate::serde_types::DateTime, D::Error>
where
    D: Deserializer<'de>,
{
    let s = String::deserialize(d)?;

    chrono::DateTime::parse_from_rfc2822(&s)
        .map(|parsed| parsed.with_timezone(&chrono::Utc))
        .map_err(|err| D::Error::custom(format!("Datetime parse from rfc2822 error: {}", err)))
}
