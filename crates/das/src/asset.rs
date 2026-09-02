//! The subset of a DAS asset this indexer stores, plus the normalization
//! rules that turn arbitrary 2021-era metadata into the attribute shape the
//! frozen API contract promises.
//!
//! Every field is `#[serde(default)]` and nothing uses `deny_unknown_fields`:
//! DAS grows fields, and a run must never fail because it did.

use serde::Deserialize;
use serde_json::Value;

use indexer_data_model::assets::TraitInput;

/// A digital asset as DAS reports it.
#[derive(Debug, Clone, Default, Deserialize)]
pub struct Asset {
    /// Mint (Token Metadata) or asset id (Core).
    #[serde(default)]
    pub id: String,
    #[serde(default)]
    pub interface: String,
    #[serde(default)]
    pub content: Option<Content>,
    #[serde(default)]
    pub ownership: Option<Ownership>,
    /// DAS still reports burned assets; they stay in the browse population
    /// and the UI greys them out.
    #[serde(default)]
    pub burnt: bool,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct Content {
    /// The off-chain URI recorded on chain. May point at a dead host.
    #[serde(default)]
    pub json_uri: Option<String>,
    #[serde(default)]
    pub files: Vec<AssetFile>,
    #[serde(default)]
    pub metadata: Option<Metadata>,
    #[serde(default)]
    pub links: Option<Links>,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct AssetFile {
    #[serde(default)]
    pub uri: Option<String>,
    #[serde(default)]
    pub mime: Option<String>,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct Metadata {
    #[serde(default)]
    pub name: Option<String>,
    #[serde(default)]
    pub symbol: Option<String>,
    /// `None` and `Some(vec![])` are different: absent means DAS never
    /// resolved the off-chain JSON, empty means it did and there were none.
    #[serde(default)]
    pub attributes: Option<Vec<RawAttribute>>,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct Links {
    #[serde(default)]
    pub image: Option<String>,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct RawAttribute {
    #[serde(default)]
    pub trait_type: Option<String>,
    #[serde(default)]
    pub value: Option<Value>,
}

#[derive(Debug, Clone, Default, Deserialize)]
pub struct Ownership {
    #[serde(default)]
    pub owner: Option<String>,
}

impl Asset {
    pub fn name(&self) -> Option<&str> {
        self.content.as_ref()?.metadata.as_ref()?.name.as_deref()
    }

    pub fn symbol(&self) -> Option<&str> {
        self.content.as_ref()?.metadata.as_ref()?.symbol.as_deref()
    }

    pub fn json_uri(&self) -> Option<&str> {
        self.content.as_ref()?.json_uri.as_deref()
    }

    pub fn owner(&self) -> Option<&str> {
        self.ownership.as_ref()?.owner.as_deref()
    }

    /// `links.image` first, then the first image-ish file. Never a CDN URL —
    /// `assets.image_uri` records where the image actually lives, and a proxy
    /// is derived at serve time.
    pub fn image(&self) -> Option<&str> {
        let content = self.content.as_ref()?;
        if let Some(image) = content.links.as_ref().and_then(|l| l.image.as_deref()) {
            if !image.is_empty() {
                return Some(image);
            }
        }
        content
            .files
            .iter()
            .find(|f| {
                f.uri.as_deref().is_some_and(|u| !u.is_empty())
                    && f.mime.as_deref().is_none_or(|m| m.starts_with("image/"))
            })
            .and_then(|f| f.uri.as_deref())
    }

    /// Attributes as DAS cached them — the fallback when we cannot fetch the
    /// off-chain document ourselves. `None` when DAS has none, so the caller
    /// leaves whatever is stored alone.
    pub fn attributes(&self) -> Option<Vec<TraitInput>> {
        let raw = self
            .content
            .as_ref()?
            .metadata
            .as_ref()?
            .attributes
            .as_ref()?;
        let normalized = normalize_attributes(raw);
        // An empty DAS attribute list is far more often "never resolved the
        // JSON" than "genuinely trait-less", so it is treated as unobserved
        // rather than as an instruction to delete.
        (!normalized.is_empty()).then_some(normalized)
    }
}

/// Largest `position` the `smallint` column holds; longer attribute lists
/// clamp rather than overflow.
const MAX_POSITION: usize = i16::MAX as usize;

/// Turns raw metadata attributes into storable trait pairs.
///
/// The contract types `value` as a string and calls it "exact and
/// case-sensitive" — it is the key clients echo back as a filter — so values
/// are **not** trimmed and **not** case-folded. Non-string JSON is
/// canonicalized rather than dropped, because `{"trait_type":"Level",
/// "value":5}` is real metadata.
pub fn normalize_attributes(raw: &[RawAttribute]) -> Vec<TraitInput> {
    let mut out = Vec::with_capacity(raw.len());
    let mut seen = std::collections::HashSet::new();
    for (index, attribute) in raw.iter().enumerate() {
        let Some(trait_type) = attribute.trait_type.as_deref() else {
            continue;
        };
        if trait_type.is_empty() {
            continue;
        }
        let Some(value) = attribute.value.as_ref().and_then(scalar_to_string) else {
            continue;
        };
        if value.is_empty() {
            continue;
        }
        if !seen.insert((trait_type.to_string(), value.clone())) {
            continue;
        }
        out.push(TraitInput {
            trait_type: trait_type.to_string(),
            value,
            position: index.min(MAX_POSITION) as i16,
        });
    }
    out
}

/// `null` is "no value" and is skipped; everything else has a faithful text
/// form. Strings pass through untouched so a value round-trips as a filter.
fn scalar_to_string(value: &Value) -> Option<String> {
    match value {
        Value::Null => None,
        Value::String(s) => Some(s.clone()),
        Value::Bool(b) => Some(b.to_string()),
        Value::Number(n) => Some(n.to_string()),
        other => Some(other.to_string()),
    }
}

/// Attributes from a fetched off-chain document. `None` when the JSON has no
/// `attributes` array at all (unobserved); `Some(vec![])` when it has an
/// empty one (observed and empty — the asset's stored attributes are then
/// deleted).
pub fn document_attributes(document: &Value) -> Option<Vec<TraitInput>> {
    let raw = document.get("attributes")?.as_array()?;
    let parsed: Vec<RawAttribute> = raw
        .iter()
        .map(|value| serde_json::from_value(value.clone()).unwrap_or_default())
        .collect();
    Some(normalize_attributes(&parsed))
}

/// `image`, falling back to the first entry of `properties.files`.
pub fn document_image(document: &Value) -> Option<String> {
    if let Some(image) = document.get("image").and_then(Value::as_str) {
        if !image.is_empty() {
            return Some(image.to_string());
        }
    }
    document
        .get("properties")?
        .get("files")?
        .as_array()?
        .iter()
        .find_map(|file| {
            file.get("uri")
                .and_then(Value::as_str)
                .filter(|uri| !uri.is_empty())
                .map(str::to_string)
        })
}

pub fn document_string(document: &Value, key: &str) -> Option<String> {
    document
        .get(key)
        .and_then(Value::as_str)
        .filter(|s| !s.is_empty())
        .map(str::to_string)
}

#[cfg(test)]
mod tests {
    use super::*;
    use serde_json::json;

    fn attrs(value: Value) -> Vec<TraitInput> {
        let raw: Vec<RawAttribute> = serde_json::from_value(value).unwrap();
        normalize_attributes(&raw)
    }

    #[test]
    fn values_are_stringified_faithfully() {
        let out = attrs(json!([
            {"trait_type": "Background", "value": "Pink"},
            {"trait_type": "Level", "value": 5},
            {"trait_type": "Ratio", "value": 1.5},
            {"trait_type": "Shiny", "value": true},
        ]));
        let pairs: Vec<(&str, &str)> = out
            .iter()
            .map(|t| (t.trait_type.as_str(), t.value.as_str()))
            .collect();
        assert_eq!(
            pairs,
            vec![
                ("Background", "Pink"),
                ("Level", "5"),
                ("Ratio", "1.5"),
                ("Shiny", "true"),
            ]
        );
    }

    #[test]
    fn unusable_entries_are_skipped_not_fatal() {
        let out = attrs(json!([
            {"trait_type": "Background", "value": null},
            {"trait_type": "", "value": "x"},
            {"value": "no type"},
            {"trait_type": "Empty", "value": ""},
            {"trait_type": "Good", "value": "Yes"},
        ]));
        assert_eq!(out.len(), 1);
        assert_eq!(out[0].trait_type, "Good");
    }

    /// Values are the filter key clients send back, so casing and whitespace
    /// are preserved exactly rather than tidied.
    #[test]
    fn values_are_not_trimmed_or_case_folded() {
        let out = attrs(json!([
            {"trait_type": "Background", "value": " Pink"},
            {"trait_type": "Body", "value": "pink"},
        ]));
        assert_eq!(out[0].value, " Pink");
        assert_eq!(out[1].value, "pink");
    }

    #[test]
    fn duplicate_pairs_collapse_and_position_follows_the_source() {
        let out = attrs(json!([
            {"trait_type": "Background", "value": "Pink"},
            {"trait_type": "Head", "value": "Crown"},
            {"trait_type": "Background", "value": "Pink"},
        ]));
        assert_eq!(out.len(), 2);
        assert_eq!(out[1].position, 1);
    }

    #[test]
    fn unknown_fields_and_missing_sections_are_tolerated() {
        let asset: Asset = serde_json::from_value(json!({
            "id": "SYN",
            "interface": "V1_NFT",
            "future_field": {"nested": true},
            "content": {"json_uri": "https://example.invalid/1.json", "surprise": 1},
        }))
        .unwrap();
        assert_eq!(asset.json_uri(), Some("https://example.invalid/1.json"));
        assert_eq!(asset.name(), None);
        assert_eq!(asset.attributes(), None);
        assert!(!asset.burnt);
    }

    /// A burned asset can still carry an owner in the response; the writer
    /// drops it, because `assets_burned_has_no_owner` forbids the pair.
    #[test]
    fn burnt_asset_parses_with_an_owner_present() {
        let asset: Asset = serde_json::from_value(json!({
            "id": "SYN",
            "burnt": true,
            "ownership": {"owner": "SYNOWNER", "ownership_model": "single"},
        }))
        .unwrap();
        assert!(asset.burnt);
        assert_eq!(asset.owner(), Some("SYNOWNER"));
    }

    #[test]
    fn image_falls_back_to_files_when_links_is_absent() {
        let asset: Asset = serde_json::from_value(json!({
            "id": "SYN",
            "content": {"files": [{"uri": "https://example.invalid/1.png", "mime": "image/png"}]},
        }))
        .unwrap();
        assert_eq!(asset.image(), Some("https://example.invalid/1.png"));
    }

    #[test]
    fn empty_das_attributes_read_as_unobserved() {
        let asset: Asset = serde_json::from_value(json!({
            "id": "SYN",
            "content": {"metadata": {"attributes": []}},
        }))
        .unwrap();
        assert_eq!(
            asset.attributes(),
            None,
            "an empty DAS list must not delete stored attributes"
        );
    }

    #[test]
    fn document_helpers_read_the_offchain_shape() {
        let document = json!({
            "name": "#1934",
            "symbol": "SYN",
            "image": "https://example.invalid/1934.png",
            "attributes": [{"trait_type": "Hair", "value": "Blue Hair"}],
        });
        assert_eq!(document_string(&document, "name").as_deref(), Some("#1934"));
        assert_eq!(
            document_image(&document).as_deref(),
            Some("https://example.invalid/1934.png")
        );
        let parsed = document_attributes(&document).unwrap();
        assert_eq!(parsed.len(), 1);
        assert_eq!(parsed[0].value, "Blue Hair");
    }

    /// A document with an explicitly empty list IS observed — unlike DAS's
    /// empty list, this one we fetched ourselves, so it is authoritative.
    #[test]
    fn document_with_empty_attribute_list_is_observed() {
        assert_eq!(
            document_attributes(&json!({"attributes": []})),
            Some(vec![])
        );
        assert_eq!(document_attributes(&json!({"name": "#1"})), None);
    }

    #[test]
    fn document_image_falls_back_to_properties_files() {
        let document = json!({
            "properties": {"files": [{"uri": "https://example.invalid/a.png", "type": "image/png"}]},
        });
        assert_eq!(
            document_image(&document).as_deref(),
            Some("https://example.invalid/a.png")
        );
    }
}
