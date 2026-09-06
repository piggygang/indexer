//! Query-string parsing for the browse endpoints (ALG-625).
//!
//! `web::Query` cannot express the contract's filter syntax: it deserializes
//! into a map and collapses repeated keys, but
//! `?trait[Background]=Pink&trait[Background]=Blue` *depends* on repetition —
//! that is how values OR within a type. So the raw query string is parsed here
//! instead.
//!
//! Brackets arrive both ways and both are valid: the Explorer's documented
//! `querySerializer` percent-encodes the type but leaves the brackets literal,
//! while CI sends `trait%5BHead%5D=Crown`. `form_urlencoded` decodes the
//! percent-escapes first, so by the time a key is inspected the two are
//! identical.

use std::collections::BTreeMap;

use actix_web::http::StatusCode;
use indexer_data_model::browse::Sort;
use indexer_data_model::types::EventKind;
use serde_json::json;

use crate::error::{ApiError, Code};

/// Contract limits: "At most 16 distinct trait types and 64 values per
/// request."
const MAX_TRAIT_TYPES: usize = 16;
const MAX_TRAIT_VALUES: usize = 64;
const MAX_VALUE_LEN: usize = 128;
const MAX_Q_LEN: usize = 64;
const MAX_SLUG_LEN: usize = 64;
pub const DEFAULT_LIMIT: i64 = 24;
pub const MAX_LIMIT: i64 = 100;

/// Everything the browse and facet endpoints share.
#[derive(Debug, Clone, Default, PartialEq, Eq)]
pub struct Filters {
    pub traits: BTreeMap<String, Vec<String>>,
    pub q: Option<String>,
}

impl Filters {
    /// A canonical, stable rendering of the filter set, for the cursor
    /// fingerprint. `BTreeMap` plus sorted values makes it independent of the
    /// order the client happened to send.
    pub fn canonical(&self) -> String {
        let mut out = String::new();
        for (name, values) in &self.traits {
            let mut values = values.clone();
            values.sort();
            out.push_str(name);
            out.push('=');
            out.push_str(&values.join(","));
            out.push(';');
        }
        out
    }
}

/// Parses `trait[...]` and `q` out of a raw query string.
pub fn filters(query: &str) -> Result<Filters, ApiError> {
    let mut traits: BTreeMap<String, Vec<String>> = BTreeMap::new();
    let mut q = None;
    let mut values = 0usize;

    for (key, value) in form_urlencoded::parse(query.as_bytes()) {
        if key == "q" {
            let trimmed = value.trim().to_string();
            // "Surrounding whitespace is trimmed, an empty result is treated
            // as absent" — an empty `q` is not an error.
            if trimmed.is_empty() {
                continue;
            }
            if trimmed.chars().count() > MAX_Q_LEN {
                return Err(ApiError::invalid("q", "`q` is longer than 64 characters"));
            }
            q = Some(trimmed);
            continue;
        }
        let Some(name) = key
            .strip_prefix("trait[")
            .and_then(|rest| rest.strip_suffix(']'))
        else {
            continue;
        };
        if name.is_empty() || value.is_empty() {
            return Err(ApiError::invalid(
                "trait",
                "a trait type and value must both be non-empty",
            ));
        }
        if value.chars().count() > MAX_VALUE_LEN {
            return Err(ApiError::invalid(
                "trait",
                "a trait value is longer than 128 characters",
            ));
        }
        values += 1;
        if values > MAX_TRAIT_VALUES {
            return Err(ApiError::invalid(
                "trait",
                "at most 64 trait values per request",
            ));
        }
        let entry = traits.entry(name.to_string()).or_default();
        // Repeating a value is the client's business, not an error; deduping
        // keeps it from inflating the value budget in the SQL.
        if !entry.iter().any(|v| v == &value) {
            entry.push(value.to_string());
        }
        if traits.len() > MAX_TRAIT_TYPES {
            return Err(ApiError::invalid(
                "trait",
                "at most 16 distinct trait types per request",
            ));
        }
    }
    Ok(Filters { traits, q })
}

/// A single scalar query parameter, if present.
fn scalar(query: &str, name: &str) -> Option<String> {
    form_urlencoded::parse(query.as_bytes())
        .find(|(k, _)| k == name)
        .map(|(_, v)| v.into_owned())
}

/// `sort`, defaulting to `number`.
///
/// `rarity`/`-rarity` are reserved by the contract so the client's union stays
/// stable when ALG-627 ships; until then they are `422 unsupported_sort` with
/// the supported list in `details`, exactly as the contract's example shows.
pub fn sort(query: &str) -> Result<Sort, ApiError> {
    let Some(raw) = scalar(query, "sort") else {
        return Ok(Sort::Number);
    };
    if let Some(sort) = Sort::parse(&raw) {
        return Ok(sort);
    }
    if raw == "rarity" || raw == "-rarity" {
        return Err(ApiError::new(
            Code::UnsupportedSort,
            format!("sort={raw} is not available until rarity scoring ships"),
            json!({
                "parameter": "sort",
                "supported": ["number", "-number", "name", "-name", "activity", "-activity"],
            }),
        ));
    }
    Err(ApiError::invalid("sort", format!("unknown sort `{raw}`")))
}

/// `limit`, defaulting to 24 with a maximum of 100 — the shared `Limit`
/// parameter.
///
/// `over_limit` is the *status* for a value above the maximum: browse is the
/// only endpoint that declares `422`, so everywhere else an out-of-range limit
/// is the `400` those endpoints do declare. The code stays
/// `invalid_parameter` either way — it is the parameter that is wrong, not the
/// sort. Never silently clamped.
pub fn limit(query: &str, over_limit: StatusCode) -> Result<i64, ApiError> {
    limit_with(query, DEFAULT_LIMIT, MAX_LIMIT, over_limit)
}

/// The same rules for the two endpoints that declare their `limit` inline
/// rather than reusing the shared parameter: `/holders` (default 25, max 100)
/// and `/search` (default 10, max 25).
pub fn limit_with(
    query: &str,
    default: i64,
    maximum: i64,
    over_limit: StatusCode,
) -> Result<i64, ApiError> {
    let Some(raw) = scalar(query, "limit") else {
        return Ok(default);
    };
    let value: i64 = raw
        .parse()
        .map_err(|_| ApiError::invalid("limit", "`limit` is not an integer"))?;
    if value < 1 {
        return Err(ApiError::invalid("limit", "`limit` must be at least 1"));
    }
    if value > maximum {
        return Err(ApiError::new(
            Code::InvalidParameter,
            format!("`limit` must be at most {maximum}"),
            json!({"parameter": "limit", "maximum": maximum}),
        )
        .with_status(over_limit));
    }
    Ok(value)
}

/// The repeated `?kind=` filter, defaulting to every public kind.
///
/// Repetition is the point (`?kind=sale&kind=transfer` ORs), so this walks the
/// raw query string for the same reason `filters` does. Duplicates are deduped
/// rather than refused — a repeated member expresses the same intent — but an
/// unknown one is `400`: the request enum is closed, and unlike a trait value
/// there is no data-driven reading of `?kind=listing`.
pub fn kinds(query: &str) -> Result<Vec<String>, ApiError> {
    let mut selected: Vec<String> = Vec::new();
    for (key, value) in form_urlencoded::parse(query.as_bytes()) {
        if key != "kind" {
            continue;
        }
        let kind = EventKind::PUBLIC
            .iter()
            .find(|k| k.as_str() == value)
            .ok_or_else(|| {
                ApiError::new(
                    Code::InvalidParameter,
                    format!("unknown activity kind `{value}`"),
                    json!({
                        "parameter": "kind",
                        "supported": EventKind::PUBLIC.map(|k| k.as_str()),
                    }),
                )
            })?;
        let kind = kind.as_str().to_string();
        if !selected.contains(&kind) {
            selected.push(kind);
        }
    }
    Ok(if selected.is_empty() {
        EventKind::public_strings()
    } else {
        selected
    })
}

/// A slug-shaped query parameter (`?collection=`), validated against the
/// contract's pattern so a malformed one is a named `400` rather than a
/// silently empty result.
pub fn slug(query: &str, name: &str) -> Result<Option<String>, ApiError> {
    let Some(value) = scalar(query, name).filter(|v| !v.is_empty()) else {
        return Ok(None);
    };
    let shaped = value.len() <= MAX_SLUG_LEN
        && !value.starts_with('-')
        && !value.ends_with('-')
        && !value.contains("--")
        && value
            .bytes()
            .all(|b| b.is_ascii_lowercase() || b.is_ascii_digit() || b == b'-');
    if !shaped {
        return Err(ApiError::invalid(name, format!("`{name}` is not a slug")));
    }
    Ok(Some(value))
}

/// `/search`'s required `q`.
pub fn required_q(query: &str) -> Result<String, ApiError> {
    let value = scalar(query, "q").unwrap_or_default().trim().to_string();
    if value.is_empty() {
        return Err(ApiError::invalid("q", "`q` is required"));
    }
    if value.chars().count() > MAX_Q_LEN {
        return Err(ApiError::invalid("q", "`q` is longer than 64 characters"));
    }
    Ok(value)
}

/// A path parameter that must be a base58 Solana address.
///
/// The contract types `{id}` and `{address}` with a pattern, so a malformed one
/// is `400 invalid_parameter` naming the parameter — never a `404`, which would
/// claim we looked and found nothing.
pub fn address<'a>(value: &'a str, parameter: &str) -> Result<&'a str, ApiError> {
    let shaped = (32..=44).contains(&value.len())
        && value
            .bytes()
            .all(|b| b.is_ascii_alphanumeric() && !b"0OIl".contains(&b));
    if shaped {
        Ok(value)
    } else {
        Err(ApiError::invalid(
            parameter,
            format!("`{parameter}` is not a base58 Solana address"),
        ))
    }
}

pub fn cursor(query: &str) -> Option<String> {
    scalar(query, "cursor").filter(|c| !c.is_empty())
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn brackets_parse_literally_and_percent_encoded() {
        // The Explorer sends the first, CI sends the second; the contract
        // requires both to work.
        let literal =
            filters("trait[Background]=Pink&trait[Background]=Blue&trait[Head]=Crown").unwrap();
        let encoded =
            filters("trait%5BBackground%5D=Pink&trait%5BBackground%5D=Blue&trait%5BHead%5D=Crown")
                .unwrap();
        assert_eq!(literal, encoded);
        assert_eq!(literal.traits["Background"], vec!["Pink", "Blue"]);
        assert_eq!(literal.traits["Head"], vec!["Crown"]);
    }

    #[test]
    fn the_canonical_form_is_order_independent() {
        let a = filters("trait[Head]=Crown&trait[Background]=Pink&trait[Background]=Blue").unwrap();
        let b = filters("trait[Background]=Blue&trait[Background]=Pink&trait[Head]=Crown").unwrap();
        assert_eq!(a.canonical(), b.canonical());
        // …but a different filter set must not collide, or a stale cursor
        // would be accepted.
        let c = filters("trait[Background]=Blue&trait[Head]=Crown").unwrap();
        assert_ne!(a.canonical(), c.canonical());
    }

    #[test]
    fn q_is_trimmed_and_empty_means_absent() {
        assert_eq!(filters("q=%20%20").unwrap().q, None);
        assert_eq!(
            filters("q=%20%231234%20").unwrap().q.as_deref(),
            Some("#1234")
        );
    }

    #[test]
    fn the_contract_limits_are_enforced() {
        let many: String = (0..17)
            .map(|i| format!("trait[T{i}]=v"))
            .collect::<Vec<_>>()
            .join("&");
        assert_eq!(filters(&many).unwrap_err().code, Code::InvalidParameter);

        let wide: String = (0..65)
            .map(|i| format!("trait[T]=v{i}"))
            .collect::<Vec<_>>()
            .join("&");
        assert_eq!(filters(&wide).unwrap_err().code, Code::InvalidParameter);
    }

    #[test]
    fn rarity_is_reserved_not_unknown() {
        let err = sort("sort=rarity").unwrap_err();
        assert_eq!(err.code, Code::UnsupportedSort);
        assert_eq!(err.details["parameter"], "sort");
        assert_eq!(sort("sort=-activity").unwrap(), Sort::ActivityDesc);
        assert_eq!(sort("").unwrap(), Sort::Number);
        assert_eq!(sort("sort=nope").unwrap_err().code, Code::InvalidParameter);
    }

    #[test]
    fn an_over_limit_is_never_clamped() {
        let browse = StatusCode::UNPROCESSABLE_ENTITY;
        assert_eq!(limit("", browse).unwrap(), DEFAULT_LIMIT);
        assert_eq!(limit("limit=50", browse).unwrap(), 50);

        // Browse declares 422; the other endpoints only declare 400. The code
        // is `invalid_parameter` in both cases — an over-large limit is a bad
        // parameter, not an unsupported sort.
        let over = limit("limit=101", browse).unwrap_err();
        assert_eq!(over.code, Code::InvalidParameter);
        assert_eq!(over.status, Some(StatusCode::UNPROCESSABLE_ENTITY));

        let elsewhere = limit("limit=101", StatusCode::BAD_REQUEST).unwrap_err();
        assert_eq!(elsewhere.code, Code::InvalidParameter);
        assert_eq!(elsewhere.status, Some(StatusCode::BAD_REQUEST));

        assert_eq!(
            limit("limit=0", browse).unwrap_err().code,
            Code::InvalidParameter
        );
    }
}
