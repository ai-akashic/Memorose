use crate::arbitrator::ExtractedMemoryFact;
use memorose_common::{MemoryDomain, MemoryUnit, StoredMemoryFact};
use serde::Deserialize;
use std::collections::{HashMap, HashSet};
use std::sync::OnceLock;

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MemoryFactAttribute {
    Residence,
    Preference,
    Employment,
    Relationship,
    Status,
    Contact,
    Ownership,
    Skill,
    Schedule,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MemoryFactSubject {
    User,
    Organization,
    Agent,
    External,
}

#[derive(Debug, Clone, Copy, PartialEq, Eq)]
pub(crate) enum MemoryFactChangeType {
    Update,
    Contradiction,
    Negation,
    Historical,
    Reaffirm,
    Addition,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum MemoryFactValueKind {
    Freeform,
    Email,
    Phone,
    City,
    OrganizationName,
    PersonName,
    Title,
    SkillName,
    DateTimeLike,
    AssetName,
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) enum MemoryFactValuePayload {
    Freeform { text: String },
    Email { address: String },
    Phone { digits: String },
    City { name: String },
    OrganizationName { name: String },
    PersonName { name: String },
    Title { name: String },
    SkillName { name: String },
    DateTimeLike { text: String },
    AssetName { name: String },
}

impl MemoryFactValuePayload {
    pub(crate) fn comparison_key(&self) -> &str {
        match self {
            Self::Freeform { text }
            | Self::Email { address: text }
            | Self::Phone { digits: text }
            | Self::City { name: text }
            | Self::OrganizationName { name: text }
            | Self::PersonName { name: text }
            | Self::Title { name: text }
            | Self::SkillName { name: text }
            | Self::DateTimeLike { text }
            | Self::AssetName { name: text } => text,
        }
    }
}

#[derive(Debug, Clone, PartialEq, Eq)]
pub(crate) struct MemoryFactDescriptor {
    pub(crate) subject: MemoryFactSubject,
    pub(crate) subject_key: String,
    pub(crate) attribute: MemoryFactAttribute,
    pub(crate) value: String,
    pub(crate) canonical_value: String,
    pub(crate) value_kind: MemoryFactValueKind,
    pub(crate) value_payload: MemoryFactValuePayload,
    pub(crate) change_type: MemoryFactChangeType,
    pub(crate) confidence: u8,
}

struct AttributeRule {
    attribute: MemoryFactAttribute,
    default_confidence: u8,
}

#[derive(Debug, Deserialize, Default)]
struct MultilingualAttributeExtension {
    attribute: String,
    #[serde(default)]
    search_phrases: Vec<String>,
    #[serde(default)]
    patterns: Vec<String>,
    #[serde(default)]
    keyword_hints: Vec<String>,
}

#[derive(Debug, Deserialize, Default)]
struct MultilingualChangeMarkerExtension {
    #[serde(default)]
    negation: Vec<String>,
    #[serde(default)]
    addition: Vec<String>,
    #[serde(default)]
    update: Vec<String>,
    #[serde(default)]
    historical: Vec<String>,
}

#[derive(Debug, Deserialize, Default)]
struct MultilingualSubjectExtension {
    #[serde(default)]
    non_entity_subjects: Vec<String>,
    #[serde(default)]
    pronoun_subjects: Vec<String>,
    #[serde(default)]
    subject_markers: Vec<String>,
    #[serde(default)]
    leading_subject_markers: Vec<String>,
    #[serde(default)]
    user_self_markers_lowered: Vec<String>,
    #[serde(default)]
    user_self_markers_raw: Vec<String>,
    #[serde(default)]
    schedule_context_lowered: Vec<String>,
    #[serde(default)]
    schedule_context_raw: Vec<String>,
}

#[derive(Debug, Deserialize, Default)]
struct MultilingualSegmentationExtension {
    #[serde(default)]
    split_delimiters: Vec<String>,
    #[serde(default)]
    trim_delimiters: Vec<String>,
    #[serde(default)]
    trim_suffixes: Vec<String>,
}

#[derive(Debug, Deserialize, Default)]
struct MultilingualBeforeMarkerExtractor {
    #[serde(default)]
    end_markers: Vec<String>,
    #[serde(default)]
    subject_boundaries: Vec<String>,
}

#[derive(Debug, Deserialize, Default)]
struct MultilingualValueExtractorExtension {
    #[serde(default)]
    residence_before: MultilingualBeforeMarkerExtractor,
    #[serde(default)]
    employment_before: MultilingualBeforeMarkerExtractor,
    #[serde(default)]
    preference_before: MultilingualBeforeMarkerExtractor,
}

#[derive(Debug, Deserialize, Default)]
struct MultilingualTransitionExtension {
    #[serde(default)]
    possessives: Vec<String>,
    #[serde(default)]
    generic_to_markers: Vec<String>,
    #[serde(default)]
    contact_verbs: Vec<String>,
    #[serde(default)]
    contact_fields: Vec<String>,
    #[serde(default)]
    contact_start_markers: Vec<String>,
    #[serde(default)]
    residence_verbs: Vec<String>,
    #[serde(default)]
    residence_fields: Vec<String>,
    #[serde(default)]
    residence_presence_markers: Vec<String>,
    #[serde(default)]
    residence_source_markers: Vec<String>,
    #[serde(default)]
    residence_movement_start_markers: Vec<String>,
    #[serde(default)]
    residence_movement_separator_markers: Vec<String>,
    #[serde(default)]
    residence_field_start_markers: Vec<String>,
    #[serde(default)]
    employment_verbs: Vec<String>,
    #[serde(default)]
    employment_fields: Vec<String>,
    #[serde(default)]
    employment_presence_markers: Vec<String>,
    #[serde(default)]
    employment_source_markers: Vec<String>,
    #[serde(default)]
    employment_start_markers: Vec<String>,
    #[serde(default)]
    employment_separator_markers: Vec<String>,
}

#[derive(Debug, Deserialize, Default)]
struct MultilingualFactExtractionConfig {
    #[serde(default)]
    attribute_extensions: Vec<MultilingualAttributeExtension>,
    #[serde(default)]
    change_markers: MultilingualChangeMarkerExtension,
    #[serde(default)]
    subject: MultilingualSubjectExtension,
    #[serde(default)]
    segmentation: MultilingualSegmentationExtension,
    #[serde(default)]
    value_extractors: MultilingualValueExtractorExtension,
    #[serde(default)]
    transitions: MultilingualTransitionExtension,
}

fn multilingual_fact_extraction_config() -> &'static MultilingualFactExtractionConfig {
    static CONFIG: OnceLock<MultilingualFactExtractionConfig> = OnceLock::new();
    CONFIG.get_or_init(|| {
        serde_json::from_str(include_str!("fact_extraction_multilingual.json"))
            .expect("fact extraction multilingual config should parse")
    })
}

fn multilingual_attribute_extension(
    attribute: MemoryFactAttribute,
) -> &'static MultilingualAttributeExtension {
    let attribute_key = memory_fact_attribute_label(attribute);
    multilingual_fact_extraction_config()
        .attribute_extensions
        .iter()
        .find(|extension| extension.attribute == attribute_key)
        .unwrap_or_else(|| panic!("missing fact extraction config for attribute: {attribute_key}"))
}

const ATTRIBUTE_RULES: &[AttributeRule] = &[
    AttributeRule {
        attribute: MemoryFactAttribute::Schedule,
        default_confidence: 70,
    },
    AttributeRule {
        attribute: MemoryFactAttribute::Residence,
        default_confidence: 90,
    },
    AttributeRule {
        attribute: MemoryFactAttribute::Preference,
        default_confidence: 85,
    },
    AttributeRule {
        attribute: MemoryFactAttribute::Employment,
        default_confidence: 85,
    },
    AttributeRule {
        attribute: MemoryFactAttribute::Relationship,
        default_confidence: 80,
    },
    AttributeRule {
        attribute: MemoryFactAttribute::Status,
        default_confidence: 75,
    },
    AttributeRule {
        attribute: MemoryFactAttribute::Contact,
        default_confidence: 80,
    },
    AttributeRule {
        attribute: MemoryFactAttribute::Ownership,
        default_confidence: 75,
    },
    AttributeRule {
        attribute: MemoryFactAttribute::Skill,
        default_confidence: 75,
    },
];

fn attribute_search_phrases(attribute: MemoryFactAttribute) -> Vec<&'static str> {
    let config = multilingual_attribute_extension(attribute);
    config_string_slice(&config.search_phrases)
}

fn attribute_patterns(attribute: MemoryFactAttribute) -> Vec<&'static str> {
    let config = multilingual_attribute_extension(attribute);
    config_string_slice(&config.patterns)
}

fn attribute_keyword_hints(attribute: MemoryFactAttribute) -> Vec<&'static str> {
    let config = multilingual_attribute_extension(attribute);
    config_string_slice(&config.keyword_hints)
}

fn config_string_slice(values: &'static [String]) -> Vec<&'static str> {
    values.iter().map(|value| value.as_str()).collect()
}

fn negation_change_markers() -> Vec<&'static str> {
    config_string_slice(&multilingual_fact_extraction_config().change_markers.negation)
}

fn addition_change_markers() -> Vec<&'static str> {
    config_string_slice(&multilingual_fact_extraction_config().change_markers.addition)
}

fn update_change_markers() -> Vec<&'static str> {
    config_string_slice(&multilingual_fact_extraction_config().change_markers.update)
}

fn historical_change_markers() -> Vec<&'static str> {
    config_string_slice(&multilingual_fact_extraction_config().change_markers.historical)
}

fn non_entity_subjects() -> Vec<&'static str> {
    config_string_slice(&multilingual_fact_extraction_config().subject.non_entity_subjects)
}

fn pronoun_subjects() -> Vec<&'static str> {
    config_string_slice(&multilingual_fact_extraction_config().subject.pronoun_subjects)
}

fn subject_markers() -> Vec<&'static str> {
    config_string_slice(&multilingual_fact_extraction_config().subject.subject_markers)
}

fn leading_subject_markers() -> Vec<&'static str> {
    config_string_slice(&multilingual_fact_extraction_config().subject.leading_subject_markers)
}

fn user_self_markers_lowered() -> Vec<&'static str> {
    multilingual_fact_extraction_config()
        .subject
        .user_self_markers_lowered
        .iter()
        .map(|value| value.as_str())
        .collect()
}

fn user_self_markers_raw() -> Vec<&'static str> {
    multilingual_fact_extraction_config()
        .subject
        .user_self_markers_raw
        .iter()
        .map(|value| value.as_str())
        .collect()
}

fn schedule_context_markers_lowered() -> Vec<&'static str> {
    multilingual_fact_extraction_config()
        .subject
        .schedule_context_lowered
        .iter()
        .map(|value| value.as_str())
        .collect()
}

fn schedule_context_markers_raw() -> Vec<&'static str> {
    multilingual_fact_extraction_config()
        .subject
        .schedule_context_raw
        .iter()
        .map(|value| value.as_str())
        .collect()
}

fn split_delimiters() -> Vec<&'static str> {
    config_string_slice(&multilingual_fact_extraction_config().segmentation.split_delimiters)
}

fn trim_delimiters() -> Vec<&'static str> {
    config_string_slice(&multilingual_fact_extraction_config().segmentation.trim_delimiters)
}

fn trim_suffixes() -> Vec<&'static str> {
    multilingual_fact_extraction_config()
        .segmentation
        .trim_suffixes
        .iter()
        .map(|value| value.as_str())
        .collect()
}

fn residence_before_markers() -> (&'static [String], &'static [String]) {
    let config = multilingual_fact_extraction_config();
    (
        &config.value_extractors.residence_before.end_markers,
        &config.value_extractors.residence_before.subject_boundaries,
    )
}

fn employment_before_markers() -> (&'static [String], &'static [String]) {
    let config = multilingual_fact_extraction_config();
    (
        &config.value_extractors.employment_before.end_markers,
        &config.value_extractors.employment_before.subject_boundaries,
    )
}

fn preference_before_markers() -> (&'static [String], &'static [String]) {
    let config = multilingual_fact_extraction_config();
    (
        &config.value_extractors.preference_before.end_markers,
        &config.value_extractors.preference_before.subject_boundaries,
    )
}

fn transition_possessives() -> Vec<&'static str> {
    config_string_slice(&multilingual_fact_extraction_config().transitions.possessives)
}

fn generic_transition_to_markers() -> Vec<&'static str> {
    config_string_slice(&multilingual_fact_extraction_config().transitions.generic_to_markers)
}

fn contact_transition_verbs() -> Vec<&'static str> {
    config_string_slice(&multilingual_fact_extraction_config().transitions.contact_verbs)
}

fn contact_transition_fields() -> Vec<&'static str> {
    config_string_slice(&multilingual_fact_extraction_config().transitions.contact_fields)
}

fn contact_transition_start_marker_literals() -> Vec<&'static str> {
    config_string_slice(&multilingual_fact_extraction_config().transitions.contact_start_markers)
}

fn residence_transition_verbs() -> Vec<&'static str> {
    config_string_slice(&multilingual_fact_extraction_config().transitions.residence_verbs)
}

fn residence_transition_fields() -> Vec<&'static str> {
    config_string_slice(&multilingual_fact_extraction_config().transitions.residence_fields)
}

fn residence_transition_presence_markers() -> Vec<&'static str> {
    config_string_slice(
        &multilingual_fact_extraction_config()
            .transitions
            .residence_presence_markers,
    )
}

fn residence_transition_source_markers() -> Vec<&'static str> {
    config_string_slice(
        &multilingual_fact_extraction_config()
            .transitions
            .residence_source_markers,
    )
}

fn residence_transition_movement_start_markers() -> Vec<&'static str> {
    config_string_slice(
        &multilingual_fact_extraction_config()
            .transitions
            .residence_movement_start_markers,
    )
}

fn residence_transition_separator_markers() -> Vec<&'static str> {
    config_string_slice(
        &multilingual_fact_extraction_config()
            .transitions
            .residence_movement_separator_markers,
    )
}

fn residence_field_transition_start_marker_literals() -> Vec<&'static str> {
    config_string_slice(
        &multilingual_fact_extraction_config()
            .transitions
            .residence_field_start_markers,
    )
}

fn employment_transition_verbs() -> Vec<&'static str> {
    config_string_slice(&multilingual_fact_extraction_config().transitions.employment_verbs)
}

fn employment_transition_fields() -> Vec<&'static str> {
    config_string_slice(&multilingual_fact_extraction_config().transitions.employment_fields)
}

fn employment_transition_presence_markers() -> Vec<&'static str> {
    config_string_slice(
        &multilingual_fact_extraction_config()
            .transitions
            .employment_presence_markers,
    )
}

fn employment_transition_source_markers() -> Vec<&'static str> {
    config_string_slice(
        &multilingual_fact_extraction_config()
            .transitions
            .employment_source_markers,
    )
}

fn employment_transition_start_marker_literals() -> Vec<&'static str> {
    config_string_slice(&multilingual_fact_extraction_config().transitions.employment_start_markers)
}

fn employment_transition_separator_markers() -> Vec<&'static str> {
    config_string_slice(
        &multilingual_fact_extraction_config()
            .transitions
            .employment_separator_markers,
    )
}

fn normalize_whitespace(text: &str) -> String {
    text.split_whitespace().collect::<Vec<_>>().join(" ")
}

fn contains_any_marker<I, S>(text: &str, markers: I) -> bool
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    markers.into_iter().any(|marker| text.contains(marker.as_ref()))
}

fn is_cjk_char(ch: char) -> bool {
    matches!(
        ch as u32,
        0x3400..=0x4DBF
            | 0x4E00..=0x9FFF
            | 0xF900..=0xFAFF
            | 0x3040..=0x30FF
            | 0xAC00..=0xD7AF
    )
}

fn contains_cjk(text: &str) -> bool {
    text.chars().any(is_cjk_char)
}

pub(crate) fn normalize_memory_keywords(keywords: &[String], limit: usize) -> Vec<String> {
    let mut normalized = Vec::new();
    for keyword in keywords {
        let keyword = normalize_whitespace(keyword).trim().to_string();
        if keyword.is_empty() {
            continue;
        }
        if normalized.iter().any(|existing| existing == &keyword) {
            continue;
        }
        normalized.push(keyword);
        if normalized.len() >= limit {
            break;
        }
    }
    normalized
}

pub(crate) fn tokenize_search_text(text: &str) -> Vec<String> {
    fn push_token(tokens: &mut Vec<String>, seen: &mut HashSet<String>, token: String) {
        let trimmed = token.trim();
        if trimmed.is_empty() {
            return;
        }
        let normalized = if contains_cjk(trimmed) {
            trimmed.to_string()
        } else {
            trimmed.to_ascii_lowercase()
        };
        if seen.insert(normalized.clone()) {
            tokens.push(normalized);
        }
    }

    fn flush_ascii_buffer(
        tokens: &mut Vec<String>,
        seen: &mut HashSet<String>,
        buffer: &mut String,
    ) {
        if !buffer.is_empty() {
            push_token(tokens, seen, std::mem::take(buffer));
        }
    }

    fn flush_cjk_buffer(tokens: &mut Vec<String>, seen: &mut HashSet<String>, buffer: &mut String) {
        if buffer.is_empty() {
            return;
        }
        let segment = std::mem::take(buffer);
        let chars = segment.chars().collect::<Vec<_>>();
        if chars.len() <= 2 {
            push_token(tokens, seen, segment);
            return;
        }

        for window in chars.windows(2) {
            push_token(tokens, seen, window.iter().collect());
        }
        if chars.len() <= 6 {
            push_token(tokens, seen, chars.iter().collect());
        }
    }

    let mut tokens = Vec::new();
    let mut seen = HashSet::new();
    let mut ascii_buffer = String::new();
    let mut cjk_buffer = String::new();

    for ch in text.chars() {
        if is_cjk_char(ch) {
            flush_ascii_buffer(&mut tokens, &mut seen, &mut ascii_buffer);
            cjk_buffer.push(ch);
        } else if ch.is_alphanumeric() {
            flush_cjk_buffer(&mut tokens, &mut seen, &mut cjk_buffer);
            ascii_buffer.push(ch.to_ascii_lowercase());
        } else if ch.is_alphabetic() || ch.is_numeric() {
            flush_cjk_buffer(&mut tokens, &mut seen, &mut cjk_buffer);
            ascii_buffer.extend(ch.to_lowercase());
        } else {
            flush_ascii_buffer(&mut tokens, &mut seen, &mut ascii_buffer);
            flush_cjk_buffer(&mut tokens, &mut seen, &mut cjk_buffer);
        }
    }

    flush_ascii_buffer(&mut tokens, &mut seen, &mut ascii_buffer);
    flush_cjk_buffer(&mut tokens, &mut seen, &mut cjk_buffer);
    tokens
}

pub(crate) fn keyword_overlap_score(query_text: &str, content: &str, keywords: &[String]) -> f32 {
    let query_terms = tokenize_search_text(query_text);
    if query_terms.is_empty() {
        return 0.0;
    }

    let mut haystack = content.to_ascii_lowercase();
    for keyword in keywords {
        haystack.push(' ');
        haystack.push_str(&keyword.to_ascii_lowercase());
    }

    let matched = query_terms
        .iter()
        .filter(|term| haystack.contains(term.as_str()))
        .count();
    matched as f32 / query_terms.len() as f32
}

fn split_fact_candidate_segments(content: &str) -> Vec<String> {
    let normalized = normalize_whitespace(content);
    if normalized.is_empty() {
        return Vec::new();
    }

    let mut segments = vec![normalized.clone()];
    for delimiter in split_delimiters() {
        let mut next = Vec::new();
        for segment in segments {
            let pieces = segment
                .split(delimiter)
                .map(str::trim)
                .filter(|piece| !piece.is_empty())
                .map(str::to_string)
                .collect::<Vec<_>>();
            if pieces.len() <= 1 {
                next.push(segment);
            } else {
                next.extend(pieces);
            }
        }
        segments = next;
    }

    let mut deduped = Vec::new();
    let mut seen = HashSet::new();
    for segment in segments {
        let key = segment.to_ascii_lowercase();
        if seen.insert(key) {
            deduped.push(segment);
        }
    }
    deduped
}

fn strip_leading_articles(value: &str) -> String {
    value
        .trim()
        .trim_start_matches("the ")
        .trim_start_matches("a ")
        .trim_start_matches("an ")
        .trim()
        .to_string()
}

fn normalize_subject_key_fragment(value: &str) -> Option<String> {
    let normalized = normalize_whitespace(value)
        .trim_matches(|ch: char| !ch.is_alphanumeric() && ch != '\'' && ch != '-')
        .trim_end_matches("'s")
        .trim()
        .to_ascii_lowercase();
    if normalized.is_empty() {
        return None;
    }

    let key = normalized
        .chars()
        .map(|ch| if ch.is_alphanumeric() { ch } else { '_' })
        .collect::<String>();
    let collapsed = key
        .split('_')
        .filter(|part| !part.is_empty())
        .collect::<Vec<_>>()
        .join("_");
    if collapsed.is_empty() {
        None
    } else {
        Some(collapsed)
    }
}

fn looks_like_organization_subject(name: &str) -> bool {
    let lowered = name.to_ascii_lowercase();
    [
        "inc",
        "corp",
        "corporation",
        "llc",
        "ltd",
        "company",
        "team",
        "studio",
        "labs",
        "lab",
        "group",
        "org",
        "公司",
        "集团",
        "团队",
        "实验室",
        "研究院",
        "大学",
        "学院",
    ]
    .iter()
    .any(|marker| {
        lowered
            .split_whitespace()
            .any(|part| part.trim_matches('.').eq(*marker))
            || name.contains(marker)
    })
}

fn is_agent_self_reference_content(content: &str, lowered_content: &str) -> bool {
    lowered_content.contains(" as the assistant ")
        || lowered_content.starts_with("as the assistant ")
        || lowered_content.contains(" as an assistant ")
        || lowered_content.starts_with("as an assistant ")
        || lowered_content.contains(" as your assistant ")
        || lowered_content.starts_with("assistant: ")
        || lowered_content.starts_with("i am the assistant ")
        || lowered_content.starts_with("i'm the assistant ")
        || content.contains("作为助手")
        || content.contains("作为AI助手")
        || content.contains("作为 ai 助手")
        || content.contains("我是助手")
        || content.contains("我是 AI 助手")
}

fn looks_like_non_entity_subject(name: &str) -> bool {
    let normalized = normalize_whitespace(name).trim().to_ascii_lowercase();
    non_entity_subjects().contains(&normalized.as_str())
}

fn is_pronoun_subject(name: &str) -> bool {
    pronoun_subjects().contains(&name.trim())
}

fn extract_subject_before_marker(content: &str) -> Option<String> {
    let normalized = normalize_whitespace(content);
    let first_segment = normalized
        .split(['.', ',', ';', ':', '，', '。', '；', '：'])
        .next()
        .unwrap_or("")
        .trim();
    if first_segment.is_empty() {
        return None;
    }

    for marker in subject_markers() {
        if let Some(index) = first_segment.find(marker) {
            let candidate = first_segment[..index]
                .trim()
                .trim_matches(|ch: char| {
                    !ch.is_alphanumeric() && !is_cjk_char(ch) && ch != '\'' && ch != '-'
                })
                .trim();
            if candidate.is_empty()
                || is_pronoun_subject(candidate)
                || looks_like_non_entity_subject(candidate)
            {
                continue;
            }
            return Some(candidate.to_string());
        }
    }

    None
}

fn extract_english_subject_before_marker(content: &str) -> Option<String> {
    let normalized = normalize_whitespace(content);
    let first_segment = normalized
        .split(['.', ',', ';', ':', '，', '。', '；', '：'])
        .next()
        .unwrap_or("")
        .trim();
    if first_segment.is_empty() {
        return None;
    }

    for marker in leading_subject_markers() {
        if let Some(index) = first_segment.to_ascii_lowercase().find(marker) {
            let candidate = first_segment[..index]
                .trim()
                .trim_matches(|ch: char| !ch.is_alphanumeric() && ch != '\'' && ch != '-')
                .trim();
            let lowered_candidate = candidate.to_ascii_lowercase();
            if candidate.is_empty()
                || is_pronoun_subject(candidate)
                || looks_like_non_entity_subject(candidate)
                || lowered_candidate.contains(" lives in ")
                || lowered_candidate.contains(" live in ")
                || lowered_candidate.contains(" works at ")
                || lowered_candidate.contains(" work at ")
                || lowered_candidate.contains(" works for ")
                || lowered_candidate.contains(" work for ")
                || lowered_candidate.contains(" based in ")
            {
                continue;
            }
            return Some(candidate.to_string());
        }
    }

    None
}

fn extract_leading_named_subject(content: &str) -> Option<String> {
    if let Some(subject) = extract_subject_before_marker(content) {
        return Some(subject);
    }
    if let Some(subject) = extract_english_subject_before_marker(content) {
        return Some(subject);
    }

    let normalized = normalize_whitespace(content);
    let first_segment = normalized
        .split(['.', ',', ';', ':'])
        .next()
        .unwrap_or("")
        .trim();
    if first_segment.is_empty() {
        return None;
    }

    let mut tokens = Vec::new();
    let organization_suffix_tokens = [
        "team", "company", "group", "labs", "lab", "studio", "org", "inc", "corp", "llc", "ltd",
    ];
    for token in first_segment.split_whitespace().take(4) {
        let cleaned =
            token.trim_matches(|ch: char| !ch.is_alphanumeric() && ch != '\'' && ch != '-');
        if cleaned.is_empty() {
            break;
        }
        let starts_upper = cleaned
            .chars()
            .next()
            .map(|ch| ch.is_uppercase())
            .unwrap_or(false);
        let lower_cleaned = cleaned.to_ascii_lowercase();
        if !starts_upper
            && !(!tokens.is_empty() && organization_suffix_tokens.contains(&lower_cleaned.as_str()))
        {
            break;
        }
        if matches!(
            cleaned,
            "Current"
                | "Favorite"
                | "Home"
                | "Status"
                | "The"
                | "Meeting"
                | "Appointment"
                | "Scheduled"
        ) {
            return None;
        }
        if is_pronoun_subject(cleaned) || looks_like_non_entity_subject(cleaned) {
            return None;
        }
        tokens.push(cleaned);
    }

    if tokens.is_empty() {
        None
    } else {
        Some(tokens.join(" "))
    }
}

fn canonicalize_contact_value(value: &str) -> String {
    let normalized = normalize_whitespace(value);
    if normalized.contains('@') {
        return normalized.to_ascii_lowercase();
    }

    let digits = normalized
        .chars()
        .filter(|ch| ch.is_ascii_digit())
        .collect::<String>();
    if digits.len() >= 7 {
        digits
    } else {
        normalized.to_ascii_lowercase()
    }
}

fn canonicalize_memory_fact_value(attribute: MemoryFactAttribute, value: &str) -> String {
    let normalized = normalize_whitespace(value);
    let lowered = normalized.to_ascii_lowercase();

    match attribute {
        MemoryFactAttribute::Contact => canonicalize_contact_value(&normalized),
        MemoryFactAttribute::Residence
        | MemoryFactAttribute::Employment
        | MemoryFactAttribute::Relationship
        | MemoryFactAttribute::Status
        | MemoryFactAttribute::Ownership
        | MemoryFactAttribute::Skill
        | MemoryFactAttribute::Schedule => strip_leading_articles(&lowered)
            .trim_matches(|ch: char| {
                !ch.is_alphanumeric() && !matches!(ch, '@' | '+' | ':' | '/' | '-')
            })
            .trim()
            .to_string(),
        MemoryFactAttribute::Preference => strip_leading_articles(&lowered)
            .trim_start_matches("to ")
            .trim()
            .to_string(),
    }
}

fn infer_memory_fact_value_kind(
    attribute: MemoryFactAttribute,
    canonical_value: &str,
) -> MemoryFactValueKind {
    match attribute {
        MemoryFactAttribute::Contact => {
            if canonical_value.contains('@') {
                MemoryFactValueKind::Email
            } else if canonical_value
                .chars()
                .filter(|ch| ch.is_ascii_digit())
                .count()
                >= 7
            {
                MemoryFactValueKind::Phone
            } else {
                MemoryFactValueKind::Freeform
            }
        }
        MemoryFactAttribute::Residence => MemoryFactValueKind::City,
        MemoryFactAttribute::Employment => MemoryFactValueKind::OrganizationName,
        MemoryFactAttribute::Relationship => MemoryFactValueKind::PersonName,
        MemoryFactAttribute::Status => MemoryFactValueKind::Title,
        MemoryFactAttribute::Skill => MemoryFactValueKind::SkillName,
        MemoryFactAttribute::Schedule => MemoryFactValueKind::DateTimeLike,
        MemoryFactAttribute::Ownership => MemoryFactValueKind::AssetName,
        MemoryFactAttribute::Preference => MemoryFactValueKind::Freeform,
    }
}

fn infer_memory_fact_value_payload(
    value_kind: &MemoryFactValueKind,
    canonical_value: &str,
) -> MemoryFactValuePayload {
    match value_kind {
        MemoryFactValueKind::Freeform => MemoryFactValuePayload::Freeform {
            text: canonical_value.to_string(),
        },
        MemoryFactValueKind::Email => MemoryFactValuePayload::Email {
            address: canonical_value.to_string(),
        },
        MemoryFactValueKind::Phone => MemoryFactValuePayload::Phone {
            digits: canonical_value.to_string(),
        },
        MemoryFactValueKind::City => MemoryFactValuePayload::City {
            name: canonical_value.to_string(),
        },
        MemoryFactValueKind::OrganizationName => MemoryFactValuePayload::OrganizationName {
            name: canonical_value.to_string(),
        },
        MemoryFactValueKind::PersonName => MemoryFactValuePayload::PersonName {
            name: canonical_value.to_string(),
        },
        MemoryFactValueKind::Title => MemoryFactValuePayload::Title {
            name: canonical_value.to_string(),
        },
        MemoryFactValueKind::SkillName => MemoryFactValuePayload::SkillName {
            name: canonical_value.to_string(),
        },
        MemoryFactValueKind::DateTimeLike => MemoryFactValuePayload::DateTimeLike {
            text: canonical_value.to_string(),
        },
        MemoryFactValueKind::AssetName => MemoryFactValuePayload::AssetName {
            name: canonical_value.to_string(),
        },
    }
}

fn trim_memory_fact_value(value: &str) -> Option<String> {
    let trimmed = value.trim();
    if trimmed.is_empty() {
        return None;
    }

    let mut end = trimmed.len();
    for delimiter in trim_delimiters() {
        if let Some(index) = trimmed.find(delimiter) {
            end = end.min(index);
        }
    }

    let mut candidate = trimmed[..end].trim().to_string();
    for suffix in trim_suffixes() {
        candidate = candidate.trim_end_matches(suffix).trim().to_string();
    }
    let candidate = candidate
        .trim_matches(|ch: char| {
            !ch.is_alphanumeric() && !matches!(ch, '@' | '+' | '-' | ':' | '/')
        })
        .trim();
    if candidate.is_empty() {
        None
    } else {
        Some(candidate.to_string())
    }
}

fn extract_memory_fact_value_before_markers<I1, S1, I2, S2>(
    content: &str,
    end_markers: I1,
    subject_boundaries: I2,
) -> Option<String>
where
    I1: IntoIterator<Item = S1>,
    S1: AsRef<str>,
    I2: IntoIterator<Item = S2>,
    S2: AsRef<str>,
{
    let normalized = normalize_whitespace(content);
    let mut best_candidate: Option<String> = None;
    let subject_boundaries = subject_boundaries
        .into_iter()
        .map(|boundary| boundary.as_ref().to_string())
        .collect::<Vec<_>>();

    for end_marker in end_markers {
        let end_marker = end_marker.as_ref();
        let mut search_offset = 0usize;
        while search_offset < normalized.len() {
            let Some(relative_index) = normalized[search_offset..].find(end_marker) else {
                break;
            };
            let end_index = search_offset + relative_index;
            let prefix = normalized[..end_index].trim();
            if prefix.is_empty() {
                search_offset = end_index + end_marker.len();
                continue;
            }

            let candidate_slice = subject_boundaries
                .iter()
                .filter_map(|boundary| prefix.rfind(boundary).map(|idx| idx + boundary.len()))
                .max()
                .map(|start| &prefix[start..])
                .unwrap_or(prefix);

            if let Some(candidate) = trim_memory_fact_value(candidate_slice) {
                match best_candidate.as_ref() {
                    Some(existing) if existing.len() <= candidate.len() => {}
                    _ => best_candidate = Some(candidate),
                }
            }

            search_offset = end_index + end_marker.len();
        }
    }

    best_candidate
}

fn extract_memory_fact_value_with_markers<I1, S1, I2, S2, I3, S3>(
    content: &str,
    start_markers: I1,
    end_markers: I2,
    require_end_marker: bool,
    invalid_prefixes: I3,
) -> Option<String>
where
    I1: IntoIterator<Item = S1>,
    S1: AsRef<str>,
    I2: IntoIterator<Item = S2>,
    S2: AsRef<str>,
    I3: IntoIterator<Item = S3>,
    S3: AsRef<str>,
{
    let normalized = normalize_whitespace(content);
    let mut best_candidate: Option<String> = None;
    let end_markers = end_markers
        .into_iter()
        .map(|marker| marker.as_ref().to_string())
        .collect::<Vec<_>>();
    let invalid_prefixes = invalid_prefixes
        .into_iter()
        .map(|marker| marker.as_ref().to_string())
        .collect::<Vec<_>>();
    for start_marker in start_markers {
        let start_marker = start_marker.as_ref();
        let mut search_offset = 0usize;
        while search_offset < normalized.len() {
            let Some(relative_index) = normalized[search_offset..].find(start_marker) else {
                break;
            };
            let start_index = search_offset + relative_index;
            let prefix = &normalized[..start_index];
            if invalid_prefixes
                .iter()
                .any(|invalid_prefix| prefix.ends_with(invalid_prefix))
            {
                search_offset = start_index + start_marker.len();
                continue;
            }
            let start = start_index + start_marker.len();
            if start >= normalized.len() {
                break;
            }

            let remainder = &normalized[start..];
            let matched_end = end_markers
                .iter()
                .filter_map(|marker| remainder.find(marker))
                .min();
            if require_end_marker && matched_end.is_none() {
                search_offset = start_index + start_marker.len();
                continue;
            }
            let end = matched_end.unwrap_or(remainder.len());

            if let Some(value) = trim_memory_fact_value(&remainder[..end]) {
                match best_candidate.as_ref() {
                    Some(existing) if existing.len() <= value.len() => {}
                    _ => best_candidate = Some(value),
                }
            }

            search_offset = start_index + start_marker.len();
        }
    }
    best_candidate
}

fn extract_transition_values_with_markers<I, S>(
    content: &str,
    start_markers: I,
    separator_markers: &[&str],
) -> Option<(String, String)>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let normalized = normalize_whitespace(content);
    let lowered = normalized.to_ascii_lowercase();

    for start_marker in start_markers {
        let start_marker = start_marker.as_ref();
        let Some(start_index) = lowered.find(start_marker) else {
            continue;
        };
        let remainder = &normalized[start_index + start_marker.len()..];
        let Some((separator_index, separator_marker)) = separator_markers
            .iter()
            .filter_map(|marker| remainder.find(marker).map(|index| (index, *marker)))
            .min_by_key(|(index, _)| *index)
        else {
            continue;
        };

        let old_value = trim_memory_fact_value(&remainder[..separator_index]);
        let new_value =
            trim_memory_fact_value(&remainder[separator_index + separator_marker.len()..]);
        match (old_value, new_value) {
            (Some(old_value), Some(new_value)) if old_value != new_value => {
                return Some((old_value, new_value));
            }
            _ => {}
        }
    }

    None
}

fn build_possessive_transition_start_markers(
    verbs: &[&str],
    possessives: &[&str],
    fields: &[&str],
) -> Vec<String> {
    let mut markers = Vec::new();
    let mut seen = HashSet::new();

    for field in fields {
        for verb in verbs {
            for marker in [
                format!("{verb} {field} from "),
                format!("{field} {verb} from "),
            ] {
                if seen.insert(marker.clone()) {
                    markers.push(marker);
                }
            }

            for possessive in possessives {
                for marker in [
                    format!("{verb} {possessive} {field} from "),
                    format!("{possessive} {field} {verb} from "),
                ] {
                    if seen.insert(marker.clone()) {
                        markers.push(marker);
                    }
                }
            }
        }
    }

    markers
}

fn contact_transition_start_markers() -> Vec<String> {
    let verbs = contact_transition_verbs();
    let possessives = transition_possessives();
    let fields = contact_transition_fields();
    let mut markers = build_possessive_transition_start_markers(
        &verbs,
        &possessives,
        &fields,
    );
    markers.extend(
        contact_transition_start_marker_literals()
            .into_iter()
            .map(|marker| marker.to_string()),
    );
    markers
}

fn employment_transition_start_markers() -> Vec<String> {
    let verbs = employment_transition_verbs();
    let possessives = transition_possessives();
    let fields = employment_transition_fields();
    let mut markers = build_possessive_transition_start_markers(
        &verbs,
        &possessives,
        &fields,
    );
    markers.extend(
        employment_transition_start_marker_literals()
            .into_iter()
            .map(|marker| marker.to_string()),
    );
    markers
}

fn residence_field_transition_start_markers() -> Vec<String> {
    let verbs = residence_transition_verbs();
    let possessives = transition_possessives();
    let fields = residence_transition_fields();
    let mut markers = build_possessive_transition_start_markers(
        &verbs,
        &possessives,
        &fields,
    );
    markers.extend(
        residence_field_transition_start_marker_literals()
            .into_iter()
            .map(|marker| marker.to_string()),
    );
    markers
}

fn extract_memory_fact_value<I, S>(content: &str, patterns: I) -> Option<String>
where
    I: IntoIterator<Item = S>,
    S: AsRef<str>,
{
    let normalized = normalize_whitespace(content);
    let lowered = normalized.to_ascii_lowercase();

    for pattern in patterns {
        let pattern = pattern.as_ref();
        if let Some(index) = lowered.find(pattern) {
            let start = index + pattern.len();
            if start <= normalized.len() {
                if let Some(value) = trim_memory_fact_value(&normalized[start..]) {
                    return Some(value);
                }
            }
        }
    }

    None
}

fn extract_memory_fact_value_by_attribute(
    attribute: MemoryFactAttribute,
    content: &str,
    patterns: &[&str],
) -> Option<String> {
    let normalized = normalize_whitespace(content);
    let lowered_content = normalized.to_ascii_lowercase();
    let schedule_context = schedule_context_markers_lowered()
            .iter()
            .any(|marker| lowered_content.contains(marker))
        || schedule_context_markers_raw()
            .iter()
            .any(|marker| normalized.contains(marker));
    if attribute == MemoryFactAttribute::Residence && schedule_context {
        return None;
    }

    extract_memory_fact_value(content, patterns).or_else(|| match attribute {
        MemoryFactAttribute::Residence => {
            let (end_markers, subject_boundaries) = residence_before_markers();
            extract_memory_fact_value_before_markers(content, end_markers, subject_boundaries)
        }
        MemoryFactAttribute::Employment => extract_memory_fact_value_with_markers(
            content,
            &["在", "于"],
            &["工作", "上班", "任职", "就职"],
            true,
            &["住"],
        )
        .or_else(|| {
            let (end_markers, subject_boundaries) = employment_before_markers();
            extract_memory_fact_value_before_markers(
                content,
                end_markers,
                subject_boundaries,
            )
        }),
        MemoryFactAttribute::Relationship => extract_memory_fact_value_with_markers(
            content,
            &["和"],
            &["结婚", "交往", "恋爱"],
            true,
            &[] as &[&str],
        ),
        MemoryFactAttribute::Preference => {
            let (end_markers, subject_boundaries) = preference_before_markers();
            extract_memory_fact_value_before_markers(content, end_markers, subject_boundaries)
        }
        _ => None,
    })
}

fn extract_transition_values_by_attribute(
    attribute: MemoryFactAttribute,
    content: &str,
) -> Option<(String, String)> {
    let normalized = normalize_whitespace(content);
    let lowered = normalized.to_ascii_lowercase();

    match attribute {
        MemoryFactAttribute::Contact => {
            let generic_to_markers = generic_transition_to_markers();
            extract_transition_values_with_markers(
                &normalized,
                contact_transition_start_markers(),
                &generic_to_markers,
            )
        }
        MemoryFactAttribute::Residence => {
            let field_start_markers = residence_field_transition_start_markers();
            let presence_markers = residence_transition_presence_markers();
            let source_markers = residence_transition_source_markers();
            let movement_start_markers = residence_transition_movement_start_markers();
            let separator_markers = residence_transition_separator_markers();
            let generic_to_markers = generic_transition_to_markers();
            if contains_any_marker(&lowered, presence_markers.iter().copied())
                || (contains_any_marker(&normalized, source_markers.iter().copied())
                    && contains_any_marker(&normalized, presence_markers.iter().copied()))
            {
                extract_transition_values_with_markers(
                    &normalized,
                    movement_start_markers,
                    &separator_markers,
                )
            } else if field_start_markers
                .iter()
                .any(|marker| lowered.contains(marker.as_str()) || normalized.contains(marker))
            {
                extract_transition_values_with_markers(
                    &normalized,
                    field_start_markers,
                    &generic_to_markers,
                )
            } else {
                None
            }
        }
        MemoryFactAttribute::Employment => {
            let start_markers = employment_transition_start_markers();
            let presence_markers = employment_transition_presence_markers();
            let source_markers = employment_transition_source_markers();
            let separator_markers = employment_transition_separator_markers();
            if start_markers
                .iter()
                .any(|marker| lowered.contains(marker.as_str()))
                || contains_any_marker(&lowered, presence_markers.iter().copied())
                || (contains_any_marker(&normalized, source_markers.iter().copied())
                    && contains_any_marker(&normalized, presence_markers.iter().copied()))
            {
                extract_transition_values_with_markers(
                    &normalized,
                    start_markers,
                    &separator_markers,
                )
            } else {
                None
            }
        }
        _ => None,
    }
}

fn attribute_supports_inline_list(attribute: MemoryFactAttribute) -> bool {
    matches!(
        attribute,
        MemoryFactAttribute::Preference
            | MemoryFactAttribute::Skill
            | MemoryFactAttribute::Ownership
    )
}

fn consume_inline_list_prefix(text: &str) -> Option<&str> {
    let mut rest = text.trim_start();
    let mut removed = false;

    loop {
        let mut matched = false;
        for prefix in [
            ", and ", ", or ", "and ", "or ", ",", "，", "、", "和", "以及", "及", "&", "/",
        ] {
            if rest.starts_with(prefix) {
                rest = rest[prefix.len()..].trim_start();
                removed = true;
                matched = true;
                break;
            }
        }

        if !matched {
            break;
        }
    }

    if removed {
        Some(rest)
    } else {
        None
    }
}

fn next_inline_list_boundary(text: &str) -> usize {
    let mut end = text.len();
    for delimiter in [
        ", and ",
        ", or ",
        " and ",
        " or ",
        ",",
        "，",
        "、",
        "和",
        "以及",
        "及",
        "&",
        "/",
        ";",
        "；",
        ". ",
        "! ",
        "? ",
        "。",
        "！",
        "？",
        " but ",
        " because ",
        " while ",
        "但是",
        "因为",
        "同时",
    ] {
        if let Some(index) = text.find(delimiter) {
            end = end.min(index);
        }
    }
    end
}

fn looks_like_inline_list_value(attribute: MemoryFactAttribute, value: &str) -> bool {
    let normalized = normalize_whitespace(value);
    if normalized.is_empty() {
        return false;
    }

    let lowered = format!(" {} ", normalized.to_ascii_lowercase());
    if ATTRIBUTE_RULES
        .iter()
        .filter(|rule| rule.attribute != attribute)
        .any(|rule| {
            attribute_patterns(rule.attribute)
                .iter()
                .any(|pattern| lowered.contains(pattern))
                || attribute_search_phrases(rule.attribute)
                    .iter()
                    .any(|phrase| lowered.contains(phrase))
        })
    {
        return false;
    }

    if [
        " is ",
        " are ",
        " am ",
        " was ",
        " were ",
        " work ",
        " works ",
        " worked ",
        " live ",
        " lives ",
        " lived ",
        " joined ",
        " moved ",
        " scheduled ",
        " rescheduled ",
        " email ",
        " phone ",
        " meeting ",
        "住在",
        "工作",
        "加入",
        "邮箱",
        "电话",
        "会议",
        "安排",
    ]
    .iter()
    .any(|marker| lowered.contains(marker) || normalized.contains(marker))
    {
        return false;
    }

    contains_cjk(&normalized) || normalized.split_whitespace().count() <= 4
}

fn extract_inline_list_values(
    attribute: MemoryFactAttribute,
    content: &str,
    first_value: &str,
) -> Vec<String> {
    if !attribute_supports_inline_list(attribute) {
        return Vec::new();
    }

    let normalized = normalize_whitespace(content);
    let Some(first_index) = normalized.find(first_value) else {
        return Vec::new();
    };

    let mut rest = &normalized[first_index + first_value.len()..];
    let mut values = Vec::new();
    let mut seen = HashSet::new();

    while let Some(after_prefix) = consume_inline_list_prefix(rest) {
        if after_prefix.is_empty() {
            break;
        }

        let boundary = next_inline_list_boundary(after_prefix);
        let candidate_slice = &after_prefix[..boundary];
        let Some(candidate) = trim_memory_fact_value(candidate_slice) else {
            break;
        };
        if !looks_like_inline_list_value(attribute, &candidate) {
            break;
        }
        if seen.insert(candidate.clone()) {
            values.push(candidate);
        }

        if boundary >= after_prefix.len() {
            break;
        }
        rest = &after_prefix[boundary..];
    }

    values
}

fn infer_memory_fact_attribute_from_keywords(keywords: &[String]) -> Option<MemoryFactAttribute> {
    for keyword in keywords {
        let normalized = keyword.to_ascii_lowercase();
        for rule in ATTRIBUTE_RULES {
            if attribute_keyword_hints(rule.attribute)
                .iter()
                .any(|hint| normalized.contains(hint))
            {
                return Some(rule.attribute);
            }
        }
    }
    None
}

fn explicit_memory_fact_subject_descriptor(
    unit: &MemoryUnit,
    content: &str,
    lowered_content: &str,
) -> Option<(MemoryFactSubject, String)> {
    if content.contains("和")
        && (content.contains("结婚") || content.contains("交往") || content.contains("恋爱"))
    {
        if let Some((left, _)) = content.split_once('和') {
            let candidate = left.trim();
            if !candidate.is_empty() && !is_pronoun_subject(candidate) {
                let key = normalize_subject_key_fragment(candidate)
                    .unwrap_or_else(|| "unknown".to_string());
                return Some((MemoryFactSubject::External, format!("external:{key}")));
            }
        }
    }

    if is_agent_self_reference_content(content, lowered_content) {
        let agent_key = unit
            .agent_id
            .as_deref()
            .and_then(normalize_subject_key_fragment)
            .unwrap_or_else(|| "self".to_string());
        return Some((MemoryFactSubject::Agent, format!("agent:{agent_key}")));
    }

    if unit.domain == MemoryDomain::Organization
        || lowered_content.contains(" we ")
        || lowered_content.starts_with("we ")
        || lowered_content.contains(" our ")
        || content.contains("我们")
        || content.contains("本公司")
    {
        let org_key = unit
            .org_id
            .as_deref()
            .and_then(normalize_subject_key_fragment)
            .unwrap_or_else(|| "self".to_string());
        Some((
            MemoryFactSubject::Organization,
            format!("organization:{org_key}"),
        ))
    } else if unit.domain == MemoryDomain::Agent || unit.agent_id.is_some() {
        let agent_key = unit
            .agent_id
            .as_deref()
            .and_then(normalize_subject_key_fragment)
            .unwrap_or_else(|| "self".to_string());
        Some((MemoryFactSubject::Agent, format!("agent:{agent_key}")))
    } else if lowered_content.contains(" i ")
        || lowered_content.starts_with("i ")
        || lowered_content.contains(" my ")
        || lowered_content.contains(" me ")
        || user_self_markers_lowered()
            .iter()
            .any(|marker| lowered_content.contains(marker))
        || content.contains('我')
        || content.contains("我的")
        || user_self_markers_raw()
            .iter()
            .any(|marker| content.contains(marker))
    {
        Some((MemoryFactSubject::User, "user:self".to_string()))
    } else {
        let external_name = extract_leading_named_subject(content);
        match external_name {
            Some(name) if looks_like_organization_subject(&name) => {
                let key =
                    normalize_subject_key_fragment(&name).unwrap_or_else(|| "unknown".to_string());
                Some((
                    MemoryFactSubject::Organization,
                    format!("organization:{key}"),
                ))
            }
            Some(name) => {
                let key =
                    normalize_subject_key_fragment(&name).unwrap_or_else(|| "unknown".to_string());
                Some((MemoryFactSubject::External, format!("external:{key}")))
            }
            None => None,
        }
    }
}

fn default_memory_fact_subject_descriptor(
    unit: &MemoryUnit,
    attribute: MemoryFactAttribute,
) -> (MemoryFactSubject, String) {
    match unit.domain {
        MemoryDomain::User if !matches!(attribute, MemoryFactAttribute::Schedule) => {
            (MemoryFactSubject::User, "user:self".to_string())
        }
        MemoryDomain::User => (MemoryFactSubject::External, "external:unknown".to_string()),
        MemoryDomain::Agent => (MemoryFactSubject::Agent, "agent:self".to_string()),
        MemoryDomain::Organization => (
            MemoryFactSubject::Organization,
            "organization:unknown".to_string(),
        ),
    }
}

fn infer_memory_fact_subject_descriptor(
    unit: &MemoryUnit,
    content: &str,
    lowered_content: &str,
    attribute: MemoryFactAttribute,
) -> (MemoryFactSubject, String) {
    explicit_memory_fact_subject_descriptor(unit, content, lowered_content)
        .unwrap_or_else(|| default_memory_fact_subject_descriptor(unit, attribute))
}

fn select_subject_override(
    content: &str,
    lowered_content: &str,
    explicit_subject: Option<(MemoryFactSubject, String)>,
    last_explicit_subject: Option<(MemoryFactSubject, String)>,
) -> Option<(MemoryFactSubject, String)> {
    match (explicit_subject, last_explicit_subject.clone()) {
        (
            Some((MemoryFactSubject::User, _)),
            Some((MemoryFactSubject::Agent, _)) | Some((MemoryFactSubject::Organization, _)),
        ) if lowered_content.contains(" i ")
            || lowered_content.starts_with("i ")
            || lowered_content.contains(" my ")
            || content.contains('我')
            || content.contains("我的") =>
        {
            last_explicit_subject
        }
        (Some(explicit), _) => Some(explicit),
        (None, last) => last,
    }
}

fn infer_memory_fact_change_type(
    lowered_content: &str,
    attribute: MemoryFactAttribute,
) -> MemoryFactChangeType {
    if contains_any_marker(lowered_content, negation_change_markers()) {
        return if matches!(attribute, MemoryFactAttribute::Preference) {
            MemoryFactChangeType::Contradiction
        } else {
            MemoryFactChangeType::Negation
        };
    }

    if contains_any_marker(lowered_content, addition_change_markers()) {
        return MemoryFactChangeType::Addition;
    }

    if contains_any_marker(lowered_content, update_change_markers()) {
        return MemoryFactChangeType::Update;
    }

    if contains_any_marker(lowered_content, historical_change_markers()) {
        return MemoryFactChangeType::Historical;
    }

    MemoryFactChangeType::Reaffirm
}

fn sanitize_extracted_memory_fact_value(attribute: MemoryFactAttribute, value: &str) -> String {
    let trimmed = normalize_whitespace(value);
    match attribute {
        MemoryFactAttribute::Ownership => strip_leading_articles(&trimmed),
        MemoryFactAttribute::Status => trimmed
            .trim_start_matches("a ")
            .trim_start_matches("an ")
            .trim_start_matches("the ")
            .trim_start_matches("一名")
            .trim_start_matches("一位")
            .trim_start_matches("一个")
            .trim()
            .to_string(),
        _ => trimmed,
    }
}

fn build_memory_fact_descriptor(
    unit: &MemoryUnit,
    source_text: &str,
    attribute: MemoryFactAttribute,
    value: String,
    confidence: u8,
    subject_override: Option<(MemoryFactSubject, String)>,
) -> MemoryFactDescriptor {
    let content = normalize_whitespace(source_text);
    let lowered_content = format!(" {} ", content.to_ascii_lowercase());
    let value = sanitize_extracted_memory_fact_value(attribute, &value);
    let canonical_value = canonicalize_memory_fact_value(attribute, &value);
    let value_kind = infer_memory_fact_value_kind(attribute, &canonical_value);
    let value_payload = infer_memory_fact_value_payload(&value_kind, &canonical_value);
    let (subject, subject_key) = subject_override.unwrap_or_else(|| {
        infer_memory_fact_subject_descriptor(unit, &content, &lowered_content, attribute)
    });
    MemoryFactDescriptor {
        subject,
        subject_key,
        attribute,
        value,
        canonical_value,
        value_kind,
        value_payload,
        change_type: infer_memory_fact_change_type(&lowered_content, attribute),
        confidence,
    }
}

fn build_memory_fact_descriptor_with_change_type(
    unit: &MemoryUnit,
    source_text: &str,
    attribute: MemoryFactAttribute,
    value: String,
    confidence: u8,
    subject_override: Option<(MemoryFactSubject, String)>,
    change_type: MemoryFactChangeType,
) -> MemoryFactDescriptor {
    let mut descriptor = build_memory_fact_descriptor(
        unit,
        source_text,
        attribute,
        value,
        confidence,
        subject_override,
    );
    descriptor.change_type = change_type;
    descriptor
}

pub(crate) fn detect_memory_fact(unit: &MemoryUnit) -> Option<MemoryFactDescriptor> {
    detect_memory_facts(unit)
        .into_iter()
        .max_by_key(|descriptor| {
            (
                memory_fact_change_type_priority(descriptor.change_type),
                descriptor.confidence,
            )
        })
}

fn memory_fact_change_type_priority(change_type: MemoryFactChangeType) -> u8 {
    match change_type {
        MemoryFactChangeType::Update => 6,
        MemoryFactChangeType::Historical | MemoryFactChangeType::Negation => 5,
        MemoryFactChangeType::Contradiction => 4,
        MemoryFactChangeType::Addition => 3,
        MemoryFactChangeType::Reaffirm => 2,
    }
}

fn prefer_memory_fact_descriptor(
    current: &MemoryFactDescriptor,
    candidate: &MemoryFactDescriptor,
) -> bool {
    (current.change_type == MemoryFactChangeType::Reaffirm
        && candidate.change_type != MemoryFactChangeType::Reaffirm)
        || (current.change_type == candidate.change_type
            && candidate.confidence > current.confidence)
}

pub(crate) fn detect_memory_facts(unit: &MemoryUnit) -> Vec<MemoryFactDescriptor> {
    let content = normalize_whitespace(&unit.content);
    let lowered = content.to_ascii_lowercase();
    let keywords = normalize_memory_keywords(&unit.keywords, 8);
    let segments = split_fact_candidate_segments(&content);
    let mut descriptors = Vec::new();
    let mut seen: HashMap<String, usize> = HashMap::new();
    let mut last_explicit_subject: Option<(MemoryFactSubject, String)> = None;

    for segment in segments.into_iter().chain(std::iter::once(content.clone())) {
        let lowered_segment = format!(" {} ", segment.to_ascii_lowercase());
        let explicit_subject =
            explicit_memory_fact_subject_descriptor(unit, &segment, &lowered_segment);
        for rule in ATTRIBUTE_RULES {
            let subject_override = select_subject_override(
                &segment,
                &lowered_segment,
                explicit_subject.clone(),
                last_explicit_subject.clone(),
            );
            if let Some((old_value, new_value)) =
                extract_transition_values_by_attribute(rule.attribute, &segment)
            {
                for descriptor in [
                    build_memory_fact_descriptor_with_change_type(
                        unit,
                        &segment,
                        rule.attribute,
                        old_value,
                        rule.default_confidence.saturating_sub(5),
                        subject_override.clone(),
                        MemoryFactChangeType::Historical,
                    ),
                    build_memory_fact_descriptor_with_change_type(
                        unit,
                        &segment,
                        rule.attribute,
                        new_value,
                        rule.default_confidence,
                        subject_override.clone(),
                        MemoryFactChangeType::Update,
                    ),
                ] {
                    let key = format!(
                        "{:?}|{}|{:?}|{}",
                        descriptor.subject,
                        descriptor.subject_key,
                        descriptor.attribute,
                        descriptor.value_payload.comparison_key()
                    );
                    if let Some(index) = seen.get(&key).copied() {
                        if prefer_memory_fact_descriptor(&descriptors[index], &descriptor) {
                            descriptors[index] = descriptor;
                        }
                    } else {
                        seen.insert(key, descriptors.len());
                        descriptors.push(descriptor);
                    }
                }
                continue;
            }

            let patterns = attribute_patterns(rule.attribute);
            if let Some(value) =
                extract_memory_fact_value_by_attribute(rule.attribute, &segment, &patterns)
            {
                let descriptor = build_memory_fact_descriptor(
                    unit,
                    &segment,
                    rule.attribute,
                    value,
                    rule.default_confidence,
                    subject_override.clone(),
                );
                let additional_values =
                    extract_inline_list_values(rule.attribute, &segment, &descriptor.value);
                for descriptor in
                    std::iter::once(descriptor).chain(additional_values.into_iter().map(|value| {
                        build_memory_fact_descriptor(
                            unit,
                            &segment,
                            rule.attribute,
                            value,
                            rule.default_confidence.saturating_sub(5),
                            subject_override.clone(),
                        )
                    }))
                {
                    let key = format!(
                        "{:?}|{}|{:?}|{}",
                        descriptor.subject,
                        descriptor.subject_key,
                        descriptor.attribute,
                        descriptor.value_payload.comparison_key()
                    );
                    if let Some(index) = seen.get(&key).copied() {
                        if prefer_memory_fact_descriptor(&descriptors[index], &descriptor) {
                            descriptors[index] = descriptor;
                        }
                    } else {
                        seen.insert(key, descriptors.len());
                        descriptors.push(descriptor);
                    }
                }
            }
        }
        if explicit_subject.is_some() {
            last_explicit_subject = explicit_subject;
        }
    }

    if descriptors.is_empty() {
        if let Some(attribute) = infer_memory_fact_attribute_from_keywords(&keywords) {
            let confidence = if lowered.contains("not ") || lowered.contains("no longer ") {
                60
            } else {
                55
            };
            descriptors.push(build_memory_fact_descriptor(
                unit,
                &content,
                attribute,
                content.clone(),
                confidence,
                None,
            ));
        }
    }

    descriptors
}

pub(crate) fn fact_change_supports_obsolete(change_type: MemoryFactChangeType) -> bool {
    matches!(
        change_type,
        MemoryFactChangeType::Update
            | MemoryFactChangeType::Negation
            | MemoryFactChangeType::Historical
    )
}

pub(crate) fn fact_change_supports_contradiction(change_type: MemoryFactChangeType) -> bool {
    matches!(
        change_type,
        MemoryFactChangeType::Contradiction | MemoryFactChangeType::Negation
    )
}

pub(crate) fn is_memory_correction_focus_token(token: &str) -> bool {
    (contains_cjk(token) && token.chars().count() >= 2 || token.len() >= 3)
        && !matches!(
            token,
            "a" | "an"
                | "and"
                | "are"
                | "as"
                | "at"
                | "be"
                | "been"
                | "but"
                | "by"
                | "for"
                | "from"
                | "had"
                | "has"
                | "have"
                | "her"
                | "his"
                | "i"
                | "in"
                | "into"
                | "is"
                | "it"
                | "its"
                | "me"
                | "my"
                | "now"
                | "of"
                | "on"
                | "or"
                | "our"
                | "she"
                | "that"
                | "the"
                | "their"
                | "them"
                | "there"
                | "they"
                | "this"
                | "to"
                | "updated"
                | "was"
                | "we"
                | "were"
                | "with"
                | "you"
                | "your"
                | "我们"
                | "我的"
                | "现在"
                | "之前"
                | "已经"
        )
}

pub(crate) fn build_memory_correction_focus_terms_with_fact(
    unit: &MemoryUnit,
    fact: Option<&MemoryFactDescriptor>,
) -> Vec<String> {
    let mut terms = Vec::new();
    let mut seen = HashSet::new();

    if let Some(fact) = fact {
        for phrase in fact.attribute.search_phrases() {
            let phrase = phrase.to_string();
            if seen.insert(phrase.clone()) {
                terms.push(phrase);
            }
        }
        for token in tokenize_search_text(&fact.value) {
            if is_memory_correction_focus_token(&token) && seen.insert(token.clone()) {
                terms.push(token);
            }
        }
        for token in tokenize_search_text(&fact.canonical_value) {
            if is_memory_correction_focus_token(&token) && seen.insert(token.clone()) {
                terms.push(token);
            }
        }
        for token in tokenize_search_text(fact.value_payload.comparison_key()) {
            if is_memory_correction_focus_token(&token) && seen.insert(token.clone()) {
                terms.push(token);
            }
        }
    }

    for keyword in normalize_memory_keywords(&unit.keywords, 6) {
        let keyword_key = keyword.to_ascii_lowercase();
        if seen.insert(keyword_key) {
            terms.push(keyword.clone());
        }
        for token in tokenize_search_text(&keyword) {
            if is_memory_correction_focus_token(&token) && seen.insert(token.clone()) {
                terms.push(token);
            }
        }
    }

    let content_tokens = tokenize_search_text(&unit.content)
        .into_iter()
        .filter(|token| is_memory_correction_focus_token(token))
        .collect::<Vec<_>>();

    for token in &content_tokens {
        if seen.insert(token.clone()) {
            terms.push(token.clone());
        }
    }

    for window in content_tokens.windows(2) {
        let phrase = format!("{} {}", window[0], window[1]);
        if seen.insert(phrase.clone()) {
            terms.push(phrase);
        }
        if terms.len() >= 10 {
            break;
        }
    }

    terms.truncate(10);
    terms
}

pub(crate) fn memory_correction_candidate_score(
    unit: &MemoryUnit,
    candidate: &MemoryUnit,
    focus_terms: &[String],
    query_fact: Option<&MemoryFactDescriptor>,
) -> f32 {
    let focus_query = focus_terms.join(" ");
    let focus_overlap = if focus_query.is_empty() {
        0.0
    } else {
        keyword_overlap_score(&focus_query, &candidate.content, &candidate.keywords)
    };

    let query_keywords = normalize_memory_keywords(&unit.keywords, 6);
    let keyword_query = query_keywords.join(" ");
    let keyword_overlap = if keyword_query.is_empty() {
        0.0
    } else {
        keyword_overlap_score(&keyword_query, &candidate.content, &candidate.keywords)
    };

    let content_overlap =
        keyword_overlap_score(&unit.content, &candidate.content, &candidate.keywords);

    let candidate_keywords = normalize_memory_keywords(&candidate.keywords, 6);
    let exact_keyword_matches = query_keywords
        .iter()
        .filter(|keyword| {
            candidate_keywords
                .iter()
                .any(|existing| existing == *keyword)
        })
        .count();
    let exact_keyword_bonus = if exact_keyword_matches == 0 || query_keywords.is_empty() {
        0.0
    } else {
        0.25 * (exact_keyword_matches as f32 / query_keywords.len() as f32)
    };
    let candidate_facts = detect_memory_facts(candidate);
    let slot_alignment = match query_fact {
        Some(query_fact) if !candidate_facts.is_empty() => candidate_facts
            .iter()
            .map(|candidate_fact| {
                if subject_keys_compatible(&query_fact.subject_key, &candidate_fact.subject_key)
                    && query_fact.subject == candidate_fact.subject
                    && query_fact.attribute == candidate_fact.attribute
                {
                    0.55
                } else if query_fact.attribute == candidate_fact.attribute
                    && query_fact.value_kind == candidate_fact.value_kind
                    && query_fact.value_payload.comparison_key()
                        == candidate_fact.value_payload.comparison_key()
                {
                    0.32
                } else if query_fact.attribute == candidate_fact.attribute
                    && query_fact.value_kind == candidate_fact.value_kind
                {
                    0.28
                } else if query_fact.attribute == candidate_fact.attribute {
                    0.2
                } else {
                    -0.25
                }
            })
            .fold(-0.25, f32::max),
        _ => 0.0,
    };

    let stream_bonus = if candidate.stream_id == unit.stream_id {
        0.05
    } else {
        0.0
    };

    (focus_overlap * 0.45
        + keyword_overlap * 0.35
        + content_overlap * 0.20
        + exact_keyword_bonus
        + slot_alignment
        + stream_bonus)
        .max(0.0)
}

fn parse_memory_fact_subject(subject: &str) -> Option<MemoryFactSubject> {
    match subject
        .trim()
        .to_ascii_lowercase()
        .replace([' ', '-'], "_")
        .as_str()
    {
        "user" | "self" | "person" => Some(MemoryFactSubject::User),
        "organization" | "org" | "company" | "team" => Some(MemoryFactSubject::Organization),
        "agent" | "assistant" | "system" => Some(MemoryFactSubject::Agent),
        "external" | "other" | "third_party" => Some(MemoryFactSubject::External),
        _ => None,
    }
}

fn memory_fact_subject_label(subject: MemoryFactSubject) -> &'static str {
    match subject {
        MemoryFactSubject::User => "user",
        MemoryFactSubject::Organization => "organization",
        MemoryFactSubject::Agent => "agent",
        MemoryFactSubject::External => "external",
    }
}

fn memory_fact_subject_prefix(subject: MemoryFactSubject) -> &'static str {
    match subject {
        MemoryFactSubject::User => "user",
        MemoryFactSubject::Organization => "organization",
        MemoryFactSubject::Agent => "agent",
        MemoryFactSubject::External => "external",
    }
}

fn default_subject_key(subject: MemoryFactSubject) -> String {
    match subject {
        MemoryFactSubject::User => "user:self".to_string(),
        MemoryFactSubject::Organization => "organization:unknown".to_string(),
        MemoryFactSubject::Agent => "agent:self".to_string(),
        MemoryFactSubject::External => "external:unknown".to_string(),
    }
}

fn normalized_subject_key(
    subject: MemoryFactSubject,
    subject_ref: Option<&str>,
    subject_name: Option<&str>,
) -> String {
    if let Some(subject_ref) = subject_ref {
        let normalized = normalize_whitespace(subject_ref);
        let trimmed = normalized.trim();
        if !trimmed.is_empty() {
            if let Some((_, suffix)) = trimmed.split_once(':') {
                if let Some(suffix_key) = normalize_subject_key_fragment(suffix) {
                    return format!("{}:{suffix_key}", memory_fact_subject_prefix(subject));
                }
            } else if let Some(suffix_key) = normalize_subject_key_fragment(trimmed) {
                return format!("{}:{suffix_key}", memory_fact_subject_prefix(subject));
            }
        }
    }

    if let Some(subject_name) = subject_name.and_then(normalize_subject_key_fragment) {
        return format!("{}:{subject_name}", memory_fact_subject_prefix(subject));
    }

    default_subject_key(subject)
}

pub(crate) fn subject_keys_compatible(left: &str, right: &str) -> bool {
    left == right || left.ends_with(":unknown") || right.ends_with(":unknown")
}

fn parse_memory_fact_attribute(attribute: &str) -> Option<MemoryFactAttribute> {
    match attribute
        .trim()
        .to_ascii_lowercase()
        .replace([' ', '-'], "_")
        .as_str()
    {
        "residence" | "location" | "city" | "home" => Some(MemoryFactAttribute::Residence),
        "preference" | "favorite" | "likes" | "dislikes" => Some(MemoryFactAttribute::Preference),
        "employment" | "job" | "work" | "company" => Some(MemoryFactAttribute::Employment),
        "relationship" | "family" | "partner" => Some(MemoryFactAttribute::Relationship),
        "status" | "identity" | "role" => Some(MemoryFactAttribute::Status),
        "contact" | "email" | "phone" => Some(MemoryFactAttribute::Contact),
        "ownership" | "asset" | "device" | "property" => Some(MemoryFactAttribute::Ownership),
        "skill" | "expertise" | "language" | "capability" => Some(MemoryFactAttribute::Skill),
        "schedule" | "appointment" | "meeting" | "calendar" => Some(MemoryFactAttribute::Schedule),
        _ => None,
    }
}

fn parse_memory_fact_change_type(change_type: &str) -> Option<MemoryFactChangeType> {
    match change_type
        .trim()
        .to_ascii_lowercase()
        .replace([' ', '-'], "_")
        .as_str()
    {
        "update" | "updated" | "replacement" | "replace" => Some(MemoryFactChangeType::Update),
        "contradiction" | "contradicts" | "conflict" => Some(MemoryFactChangeType::Contradiction),
        "negation" | "negative" | "denial" => Some(MemoryFactChangeType::Negation),
        "historical" | "history" | "past" | "previous" => Some(MemoryFactChangeType::Historical),
        "reaffirm" | "reaffirmed" | "confirm" | "confirmed" => Some(MemoryFactChangeType::Reaffirm),
        "addition" | "added" | "additional" => Some(MemoryFactChangeType::Addition),
        _ => None,
    }
}

fn memory_fact_attribute_label(attribute: MemoryFactAttribute) -> &'static str {
    match attribute {
        MemoryFactAttribute::Residence => "residence",
        MemoryFactAttribute::Preference => "preference",
        MemoryFactAttribute::Employment => "employment",
        MemoryFactAttribute::Relationship => "relationship",
        MemoryFactAttribute::Status => "status",
        MemoryFactAttribute::Contact => "contact",
        MemoryFactAttribute::Ownership => "ownership",
        MemoryFactAttribute::Skill => "skill",
        MemoryFactAttribute::Schedule => "schedule",
    }
}

fn memory_fact_change_type_label(change_type: MemoryFactChangeType) -> &'static str {
    match change_type {
        MemoryFactChangeType::Update => "update",
        MemoryFactChangeType::Contradiction => "contradiction",
        MemoryFactChangeType::Negation => "negation",
        MemoryFactChangeType::Historical => "historical",
        MemoryFactChangeType::Reaffirm => "reaffirm",
        MemoryFactChangeType::Addition => "addition",
    }
}

fn normalize_temporal_status(temporal_status: Option<&str>) -> Option<&'static str> {
    let normalized = temporal_status?
        .trim()
        .to_ascii_lowercase()
        .replace([' ', '-'], "_");
    match normalized.as_str() {
        "current" | "present" | "active" => Some("current"),
        "historical" | "history" | "past" | "previous" | "former" | "formerly" => {
            Some("historical")
        }
        "negated" | "negative" | "inactive" | "removed" => Some("negated"),
        _ => None,
    }
}

fn normalize_polarity(polarity: Option<&str>) -> Option<&'static str> {
    let normalized = polarity?
        .trim()
        .to_ascii_lowercase()
        .replace([' ', '-'], "_");
    match normalized.as_str() {
        "positive" | "affirmative" | "true" => Some("positive"),
        "negative" | "false" | "negated" => Some("negative"),
        _ => None,
    }
}

fn derived_temporal_status(change_type: MemoryFactChangeType) -> Option<&'static str> {
    match change_type {
        MemoryFactChangeType::Historical => Some("historical"),
        MemoryFactChangeType::Negation => Some("negated"),
        MemoryFactChangeType::Update
        | MemoryFactChangeType::Reaffirm
        | MemoryFactChangeType::Addition
        | MemoryFactChangeType::Contradiction => Some("current"),
    }
}

fn derived_polarity(change_type: MemoryFactChangeType) -> &'static str {
    match change_type {
        MemoryFactChangeType::Negation | MemoryFactChangeType::Contradiction => "negative",
        MemoryFactChangeType::Update
        | MemoryFactChangeType::Historical
        | MemoryFactChangeType::Reaffirm
        | MemoryFactChangeType::Addition => "positive",
    }
}

fn resolve_memory_fact_change_type(
    attribute: MemoryFactAttribute,
    change_type: Option<&str>,
    temporal_status: Option<&str>,
    polarity: Option<&str>,
) -> Option<MemoryFactChangeType> {
    let normalized_temporal_status = normalize_temporal_status(temporal_status);
    let normalized_polarity = normalize_polarity(polarity);

    if matches!(normalized_temporal_status, Some("negated"))
        || matches!(normalized_polarity, Some("negative"))
    {
        return Some(if matches!(attribute, MemoryFactAttribute::Preference) {
            MemoryFactChangeType::Contradiction
        } else {
            MemoryFactChangeType::Negation
        });
    }

    if matches!(normalized_temporal_status, Some("historical")) {
        return Some(MemoryFactChangeType::Historical);
    }

    if let Some(change_type) = change_type.and_then(parse_memory_fact_change_type) {
        return Some(change_type);
    }

    if matches!(normalized_temporal_status, Some("current")) {
        return Some(MemoryFactChangeType::Update);
    }

    Some(MemoryFactChangeType::Reaffirm)
}

fn build_descriptor_from_structured_fact(
    subject: &str,
    subject_ref: Option<&str>,
    subject_name: Option<&str>,
    attribute: &str,
    value: &str,
    canonical_value: Option<&str>,
    change_type: Option<&str>,
    temporal_status: Option<&str>,
    polarity: Option<&str>,
    confidence: f32,
) -> Option<MemoryFactDescriptor> {
    let subject = parse_memory_fact_subject(subject)?;
    let attribute = parse_memory_fact_attribute(attribute)?;
    let change_type =
        resolve_memory_fact_change_type(attribute, change_type, temporal_status, polarity)?;
    let value = normalize_whitespace(value);
    if value.is_empty() {
        return None;
    }
    let canonical_value = canonical_value
        .map(normalize_whitespace)
        .filter(|value| !value.is_empty())
        .unwrap_or_else(|| canonicalize_memory_fact_value(attribute, &value));
    let value_kind = infer_memory_fact_value_kind(attribute, &canonical_value);
    let value_payload = infer_memory_fact_value_payload(&value_kind, &canonical_value);
    let subject_key = normalized_subject_key(subject, subject_ref, subject_name);

    Some(MemoryFactDescriptor {
        subject,
        subject_key,
        attribute,
        value,
        canonical_value,
        value_kind,
        value_payload,
        change_type,
        confidence: (confidence.clamp(0.0, 1.0) * 100.0).round() as u8,
    })
}

pub(crate) fn descriptor_from_extracted_fact(
    fact: ExtractedMemoryFact,
) -> Option<MemoryFactDescriptor> {
    build_descriptor_from_structured_fact(
        &fact.subject,
        fact.subject_ref.as_deref(),
        fact.subject_name.as_deref(),
        &fact.attribute,
        &fact.value,
        None,
        Some(&fact.change_type),
        fact.temporal_status.as_deref(),
        fact.polarity.as_deref(),
        fact.confidence,
    )
}

pub(crate) fn descriptor_from_stored_fact(fact: &StoredMemoryFact) -> Option<MemoryFactDescriptor> {
    build_descriptor_from_structured_fact(
        &fact.subject,
        fact.subject_ref.as_deref(),
        fact.subject_name.as_deref(),
        &fact.attribute,
        &fact.value,
        fact.canonical_value.as_deref(),
        Some(&fact.change_type),
        fact.temporal_status.as_deref(),
        fact.polarity.as_deref(),
        fact.confidence,
    )
}

pub(crate) fn stored_fact_from_descriptor(descriptor: &MemoryFactDescriptor) -> StoredMemoryFact {
    StoredMemoryFact {
        subject: memory_fact_subject_label(descriptor.subject).to_string(),
        subject_ref: Some(descriptor.subject_key.clone()),
        subject_name: None,
        attribute: memory_fact_attribute_label(descriptor.attribute).to_string(),
        value: descriptor.value.clone(),
        canonical_value: Some(descriptor.canonical_value.clone()),
        change_type: memory_fact_change_type_label(descriptor.change_type).to_string(),
        temporal_status: derived_temporal_status(descriptor.change_type).map(str::to_string),
        polarity: Some(derived_polarity(descriptor.change_type).to_string()),
        evidence_span: None,
        confidence: descriptor.confidence as f32 / 100.0,
    }
}

pub(crate) fn stored_fact_from_extracted_fact(
    fact: ExtractedMemoryFact,
) -> Option<StoredMemoryFact> {
    let descriptor = descriptor_from_extracted_fact(fact.clone())?;
    Some(StoredMemoryFact {
        subject: memory_fact_subject_label(descriptor.subject).to_string(),
        subject_ref: Some(descriptor.subject_key.clone()),
        subject_name: fact
            .subject_name
            .map(|value| normalize_whitespace(&value))
            .filter(|value| !value.is_empty()),
        attribute: memory_fact_attribute_label(descriptor.attribute).to_string(),
        value: descriptor.value.clone(),
        canonical_value: Some(descriptor.canonical_value.clone()),
        change_type: memory_fact_change_type_label(descriptor.change_type).to_string(),
        temporal_status: normalize_temporal_status(fact.temporal_status.as_deref())
            .or_else(|| derived_temporal_status(descriptor.change_type))
            .map(str::to_string),
        polarity: normalize_polarity(fact.polarity.as_deref())
            .unwrap_or_else(|| derived_polarity(descriptor.change_type))
            .to_string()
            .into(),
        evidence_span: fact
            .evidence_span
            .map(|value| normalize_whitespace(&value))
            .filter(|value| !value.is_empty()),
        confidence: fact.confidence.clamp(0.0, 1.0),
    })
}

impl MemoryFactAttribute {
    pub(crate) fn search_phrases(&self) -> Vec<&'static str> {
        attribute_search_phrases(*self)
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use memorose_common::MemoryType;
    use serde::Deserialize;
    use std::collections::{BTreeMap, HashSet as StdHashSet};
    use uuid::Uuid;

    fn test_unit(content: &str) -> MemoryUnit {
        MemoryUnit::new(
            None,
            "test-user".into(),
            None,
            Uuid::new_v4(),
            MemoryType::Factual,
            content.into(),
            None,
        )
    }

    fn test_agent_unit(content: &str) -> MemoryUnit {
        let mut unit = MemoryUnit::new(
            None,
            "test-user".into(),
            Some("assistant-main".into()),
            Uuid::new_v4(),
            MemoryType::Factual,
            content.into(),
            None,
        );
        unit.domain = MemoryDomain::Agent;
        unit
    }

    #[test]
    fn test_descriptor_from_extracted_fact_uses_subject_ref_and_temporal_status() {
        let descriptor = descriptor_from_extracted_fact(ExtractedMemoryFact {
            subject: "organization".into(),
            subject_ref: Some("organization:OpenAI".into()),
            subject_name: Some("OpenAI".into()),
            attribute: "employment".into(),
            value: "Research".into(),
            change_type: "reaffirm".into(),
            temporal_status: Some("historical".into()),
            polarity: Some("positive".into()),
            evidence_span: Some("worked at OpenAI before".into()),
            confidence: 0.84,
        })
        .expect("descriptor should be parsed");

        assert_eq!(descriptor.subject, MemoryFactSubject::Organization);
        assert_eq!(descriptor.subject_key, "organization:openai");
        assert_eq!(descriptor.attribute, MemoryFactAttribute::Employment);
        assert_eq!(descriptor.change_type, MemoryFactChangeType::Historical);
    }

    #[test]
    fn test_descriptor_from_stored_fact_uses_subject_name_when_ref_missing() {
        let descriptor = descriptor_from_stored_fact(&StoredMemoryFact {
            subject: "external".into(),
            subject_ref: None,
            subject_name: Some("John Doe".into()),
            attribute: "relationship".into(),
            value: "friend".into(),
            canonical_value: Some("friend".into()),
            change_type: "reaffirm".into(),
            temporal_status: Some("current".into()),
            polarity: Some("positive".into()),
            evidence_span: None,
            confidence: 0.77,
        })
        .expect("stored fact should be parsed");

        assert_eq!(descriptor.subject, MemoryFactSubject::External);
        assert_eq!(descriptor.subject_key, "external:john_doe");
    }

    #[test]
    fn test_stored_fact_from_extracted_fact_preserves_richer_fields() {
        let stored = stored_fact_from_extracted_fact(ExtractedMemoryFact {
            subject: "user".into(),
            subject_ref: Some("user:self".into()),
            subject_name: None,
            attribute: "contact".into(),
            value: "Dylan@Example.com".into(),
            change_type: "update".into(),
            temporal_status: Some("current".into()),
            polarity: Some("positive".into()),
            evidence_span: Some("email is Dylan@Example.com".into()),
            confidence: 0.91,
        })
        .expect("stored fact should be created");

        assert_eq!(stored.subject_ref.as_deref(), Some("user:self"));
        assert_eq!(stored.canonical_value.as_deref(), Some("dylan@example.com"));
        assert_eq!(stored.temporal_status.as_deref(), Some("current"));
        assert_eq!(stored.polarity.as_deref(), Some("positive"));
        assert_eq!(
            stored.evidence_span.as_deref(),
            Some("email is Dylan@Example.com")
        );
    }

    #[test]
    fn test_tokenize_search_text_includes_cjk_bigrams() {
        let tokens = tokenize_search_text("我现在住在北京");
        assert!(tokens.iter().any(|token| token == "北京"));
        assert!(tokens.iter().any(|token| token == "住在"));
    }

    #[derive(Debug, Deserialize)]
    struct EvalExpectedFact {
        attribute: String,
        value: String,
        subject: String,
        subject_key: String,
        change_type: String,
    }

    #[derive(Debug, Deserialize)]
    struct EvalCase {
        content: String,
        expected: Vec<EvalExpectedFact>,
    }

    #[derive(Default)]
    struct EvalScorecard {
        expected_total: usize,
        matched_total: usize,
        attribute_hits: usize,
        subject_hits: usize,
        subject_key_hits: usize,
        change_type_hits: usize,
        full_hits: usize,
    }

    #[derive(Default, Clone, Debug)]
    struct EvalBucketStats {
        expected: usize,
        exact_hits: usize,
        false_positives: usize,
    }

    fn expected_fact_key(expected: &EvalExpectedFact) -> String {
        format!(
            "{}|{}|{}|{}|{}",
            expected.attribute,
            expected.value,
            expected.subject,
            expected.subject_key,
            expected.change_type
        )
    }

    fn actual_fact_key(actual: &MemoryFactDescriptor) -> String {
        format!(
            "{}|{}|{}|{}|{}",
            memory_fact_attribute_label(actual.attribute),
            actual.value,
            memory_fact_subject_label(actual.subject),
            actual.subject_key,
            memory_fact_change_type_label(actual.change_type)
        )
    }

    fn eval_case_language(content: &str) -> &'static str {
        if contains_cjk(content) {
            "zh"
        } else {
            "en"
        }
    }

    fn bucket_rate(stats: &EvalBucketStats) -> f32 {
        if stats.expected == 0 {
            1.0
        } else {
            stats.exact_hits as f32 / stats.expected as f32
        }
    }

    #[test]
    fn test_detect_memory_fact_eval_fixtures_bilingual() {
        struct Fixture {
            content: &'static str,
            attribute: MemoryFactAttribute,
            value: &'static str,
            change_type: MemoryFactChangeType,
            subject: MemoryFactSubject,
            subject_key: &'static str,
        }

        let fixtures = vec![
            Fixture {
                content: "I now live in Beijing",
                attribute: MemoryFactAttribute::Residence,
                value: "Beijing",
                change_type: MemoryFactChangeType::Update,
                subject: MemoryFactSubject::User,
                subject_key: "user:self",
            },
            Fixture {
                content: "我现在住在北京",
                attribute: MemoryFactAttribute::Residence,
                value: "北京",
                change_type: MemoryFactChangeType::Update,
                subject: MemoryFactSubject::User,
                subject_key: "user:self",
            },
            Fixture {
                content: "我不喜欢香菜",
                attribute: MemoryFactAttribute::Preference,
                value: "香菜",
                change_type: MemoryFactChangeType::Contradiction,
                subject: MemoryFactSubject::User,
                subject_key: "user:self",
            },
            Fixture {
                content: "我的邮箱是 dylan@example.com",
                attribute: MemoryFactAttribute::Contact,
                value: "dylan@example.com",
                change_type: MemoryFactChangeType::Reaffirm,
                subject: MemoryFactSubject::User,
                subject_key: "user:self",
            },
            Fixture {
                content: "我擅长Rust和Go",
                attribute: MemoryFactAttribute::Skill,
                value: "Rust",
                change_type: MemoryFactChangeType::Reaffirm,
                subject: MemoryFactSubject::User,
                subject_key: "user:self",
            },
            Fixture {
                content: "会议改到明天下午三点",
                attribute: MemoryFactAttribute::Schedule,
                value: "明天下午三点",
                change_type: MemoryFactChangeType::Update,
                subject: MemoryFactSubject::External,
                subject_key: "external:unknown",
            },
            Fixture {
                content: "张三住在上海",
                attribute: MemoryFactAttribute::Residence,
                value: "上海",
                change_type: MemoryFactChangeType::Reaffirm,
                subject: MemoryFactSubject::External,
                subject_key: "external:张三",
            },
            Fixture {
                content: "阿里巴巴公司位于杭州",
                attribute: MemoryFactAttribute::Residence,
                value: "杭州",
                change_type: MemoryFactChangeType::Reaffirm,
                subject: MemoryFactSubject::Organization,
                subject_key: "organization:阿里巴巴公司",
            },
            Fixture {
                content: "Vivo en Madrid",
                attribute: MemoryFactAttribute::Residence,
                value: "Madrid",
                change_type: MemoryFactChangeType::Reaffirm,
                subject: MemoryFactSubject::User,
                subject_key: "user:self",
            },
            Fixture {
                content: "Mi correo es dylan@example.com",
                attribute: MemoryFactAttribute::Contact,
                value: "dylan@example.com",
                change_type: MemoryFactChangeType::Reaffirm,
                subject: MemoryFactSubject::User,
                subject_key: "user:self",
            },
            Fixture {
                content: "Trabajo en OpenAI",
                attribute: MemoryFactAttribute::Employment,
                value: "OpenAI",
                change_type: MemoryFactChangeType::Reaffirm,
                subject: MemoryFactSubject::User,
                subject_key: "user:self",
            },
            Fixture {
                content: "私は東京に住んでいます",
                attribute: MemoryFactAttribute::Residence,
                value: "東京",
                change_type: MemoryFactChangeType::Reaffirm,
                subject: MemoryFactSubject::User,
                subject_key: "user:self",
            },
            Fixture {
                content: "私は寿司が好きです",
                attribute: MemoryFactAttribute::Preference,
                value: "寿司",
                change_type: MemoryFactChangeType::Reaffirm,
                subject: MemoryFactSubject::User,
                subject_key: "user:self",
            },
        ];

        for fixture in fixtures {
            let fact = detect_memory_fact(&test_unit(fixture.content))
                .unwrap_or_else(|| panic!("missing fact for content: {}", fixture.content));
            assert_eq!(fact.attribute, fixture.attribute, "{}", fixture.content);
            assert_eq!(fact.value, fixture.value, "{}", fixture.content);
            assert_eq!(fact.change_type, fixture.change_type, "{}", fixture.content);
            assert_eq!(fact.subject, fixture.subject, "{}", fixture.content);
            assert_eq!(fact.subject_key, fixture.subject_key, "{}", fixture.content);
        }
    }

    #[test]
    fn test_detect_memory_facts_extracts_multiple_rule_facts() {
        let facts = detect_memory_facts(&test_unit(
            "I now live in Beijing and my email is dylan@example.com",
        ));

        assert_eq!(facts.len(), 2);
        assert!(facts.iter().any(|fact| {
            fact.attribute == MemoryFactAttribute::Residence
                && fact.value == "Beijing"
                && fact.subject == MemoryFactSubject::User
        }));
        assert!(facts.iter().any(|fact| {
            fact.attribute == MemoryFactAttribute::Contact
                && fact.value == "dylan@example.com"
                && fact.subject == MemoryFactSubject::User
        }));
    }

    #[test]
    fn test_detect_memory_facts_extracts_inline_list_values_bilingual() {
        let english_facts = detect_memory_facts(&test_unit("I like sushi and ramen"));
        assert!(english_facts.iter().any(|fact| {
            fact.attribute == MemoryFactAttribute::Preference
                && fact.value == "sushi"
                && fact.subject == MemoryFactSubject::User
        }));
        assert!(english_facts.iter().any(|fact| {
            fact.attribute == MemoryFactAttribute::Preference
                && fact.value == "ramen"
                && fact.subject == MemoryFactSubject::User
        }));

        let chinese_facts = detect_memory_facts(&test_unit("我擅长Rust和Go"));
        assert!(chinese_facts.iter().any(|fact| {
            fact.attribute == MemoryFactAttribute::Skill
                && fact.value == "Rust"
                && fact.subject == MemoryFactSubject::User
        }));
        assert!(chinese_facts.iter().any(|fact| {
            fact.attribute == MemoryFactAttribute::Skill
                && fact.value == "Go"
                && fact.subject == MemoryFactSubject::User
        }));
    }

    #[test]
    fn test_detect_memory_facts_supports_spanish_and_japanese_patterns() {
        let spanish_facts =
            detect_memory_facts(&test_unit("Juan Perez vive en Madrid y trabaja en OpenAI"));

        assert!(spanish_facts.iter().any(|fact| {
            fact.attribute == MemoryFactAttribute::Residence
                && fact.value == "Madrid"
                && fact.subject == MemoryFactSubject::External
                && fact.subject_key == "external:juan_perez"
        }));
        assert!(spanish_facts.iter().any(|fact| {
            fact.attribute == MemoryFactAttribute::Employment
                && fact.value == "OpenAI"
                && fact.subject == MemoryFactSubject::External
                && fact.subject_key == "external:juan_perez"
        }));

        let japanese_facts = detect_memory_facts(&test_unit(
            "私は東京に住んでいます。メールは dylan@example.com です。私はOpenAIで働いています。",
        ));
        assert!(japanese_facts.iter().any(|fact| {
            fact.attribute == MemoryFactAttribute::Residence
                && fact.value == "東京"
                && fact.subject == MemoryFactSubject::User
        }));
        assert!(japanese_facts.iter().any(|fact| {
            fact.attribute == MemoryFactAttribute::Contact
                && fact.value == "dylan@example.com"
                && fact.subject == MemoryFactSubject::User
        }));
        assert!(japanese_facts.iter().any(|fact| {
            fact.attribute == MemoryFactAttribute::Employment
                && fact.value == "OpenAI"
                && fact.subject == MemoryFactSubject::User
        }));
    }

    #[test]
    fn test_detect_memory_facts_supports_additional_relationship_status_schedule_patterns() {
        let relationship = detect_memory_fact(&test_unit("John Doe is engaged to Jane Smith"))
            .expect("relationship fact should exist");
        assert_eq!(relationship.attribute, MemoryFactAttribute::Relationship);
        assert_eq!(relationship.value, "Jane Smith");
        assert_eq!(relationship.subject, MemoryFactSubject::External);
        assert_eq!(relationship.subject_key, "external:john_doe");

        let status = detect_memory_fact(&test_unit("I work as a designer"))
            .expect("status fact should exist");
        assert_eq!(status.attribute, MemoryFactAttribute::Status);
        assert_eq!(status.value, "designer");
        assert_eq!(status.subject, MemoryFactSubject::User);

        let schedule = detect_memory_fact(&test_unit("The call is at 5pm tomorrow"))
            .expect("schedule fact should exist");
        assert_eq!(schedule.attribute, MemoryFactAttribute::Schedule);
        assert_eq!(schedule.value, "5pm tomorrow");
        assert_eq!(schedule.subject, MemoryFactSubject::External);

        let chinese_relationship =
            detect_memory_fact(&test_unit("张三老婆是李四")).expect("chinese relationship fact");
        assert_eq!(
            chinese_relationship.attribute,
            MemoryFactAttribute::Relationship
        );
        assert_eq!(chinese_relationship.value, "李四");
        assert_eq!(chinese_relationship.subject_key, "external:张三");
    }

    #[test]
    fn test_detect_memory_facts_supports_contact_and_employment_negation_variants() {
        let historical_job = detect_memory_fact(&test_unit("I used to work at OpenAI"))
            .expect("historical job fact should exist");
        assert_eq!(historical_job.attribute, MemoryFactAttribute::Employment);
        assert_eq!(historical_job.value, "OpenAI");
        assert_eq!(historical_job.change_type, MemoryFactChangeType::Historical);

        let negated_job = detect_memory_fact(&test_unit("我不再在OpenAI工作"))
            .expect("negated chinese employment fact should exist");
        assert_eq!(negated_job.attribute, MemoryFactAttribute::Employment);
        assert_eq!(negated_job.value, "OpenAI");
        assert_eq!(negated_job.change_type, MemoryFactChangeType::Negation);

        let phone = detect_memory_fact(&test_unit("My phone number is +1 415 555 2671"))
            .expect("phone fact should exist");
        assert_eq!(phone.attribute, MemoryFactAttribute::Contact);
        assert_eq!(phone.value, "+1 415 555 2671");
        assert_eq!(
            phone.value_payload,
            MemoryFactValuePayload::Phone {
                digits: "14155552671".into()
            }
        );

        let skill = detect_memory_fact(&test_unit("I also speak Japanese"))
            .expect("skill fact should exist");
        assert_eq!(skill.attribute, MemoryFactAttribute::Skill);
        assert_eq!(skill.value, "Japanese");
        assert_eq!(skill.change_type, MemoryFactChangeType::Addition);

        let contact = detect_memory_fact(&test_unit("联系我：dylan@example.com"))
            .expect("contact fact should exist");
        assert_eq!(contact.attribute, MemoryFactAttribute::Contact);
        assert_eq!(contact.value, "dylan@example.com");
        assert_eq!(contact.subject, MemoryFactSubject::User);
    }

    #[test]
    fn test_detect_memory_facts_supports_org_agent_subjects_and_multi_clause_updates() {
        let org_fact = detect_memory_fact(&test_unit("Acme Labs is based in Berlin"))
            .expect("org fact should exist");
        assert_eq!(org_fact.attribute, MemoryFactAttribute::Residence);
        assert_eq!(org_fact.subject, MemoryFactSubject::Organization);
        assert_eq!(org_fact.subject_key, "organization:acme_labs");
        assert_eq!(org_fact.value, "Berlin");

        let agent_fact = detect_memory_fact(&test_agent_unit("作为助手，我是一名研究助理"))
            .expect("agent fact should exist");
        assert_eq!(agent_fact.attribute, MemoryFactAttribute::Status);
        assert_eq!(agent_fact.subject, MemoryFactSubject::Agent);
        assert_eq!(agent_fact.subject_key, "agent:assistant_main");
        assert_eq!(agent_fact.value, "研究助理");

        let facts = detect_memory_facts(&test_unit(
            "I used to work at OpenAI, now work at Anthropic",
        ));
        assert!(facts.iter().any(|fact| {
            fact.attribute == MemoryFactAttribute::Employment
                && fact.value == "OpenAI"
                && fact.change_type == MemoryFactChangeType::Historical
        }));
        assert!(facts.iter().any(|fact| {
            fact.attribute == MemoryFactAttribute::Employment
                && fact.value == "Anthropic"
                && fact.change_type == MemoryFactChangeType::Update
        }));

        let zh_facts = detect_memory_facts(&test_unit("我之前住在上海，现在住在北京"));
        assert!(zh_facts.iter().any(|fact| {
            fact.attribute == MemoryFactAttribute::Residence
                && fact.value == "上海"
                && fact.change_type == MemoryFactChangeType::Historical
        }));
        assert!(zh_facts.iter().any(|fact| {
            fact.attribute == MemoryFactAttribute::Residence
                && fact.value == "北京"
                && fact.change_type == MemoryFactChangeType::Update
        }));
    }

    #[test]
    fn test_detect_memory_facts_supports_explicit_transition_pairs() {
        let email_facts = detect_memory_facts(&test_unit(
            "My email changed from old@example.com to new@example.com",
        ));
        assert!(email_facts.iter().any(|fact| {
            fact.attribute == MemoryFactAttribute::Contact
                && fact.value == "old@example.com"
                && fact.change_type == MemoryFactChangeType::Historical
        }));
        assert!(email_facts.iter().any(|fact| {
            fact.attribute == MemoryFactAttribute::Contact
                && fact.value == "new@example.com"
                && fact.change_type == MemoryFactChangeType::Update
        }));

        let zh_email_facts = detect_memory_facts(&test_unit(
            "我的邮箱从 old@example.com 改成 new@example.com",
        ));
        assert!(zh_email_facts.iter().any(|fact| {
            fact.attribute == MemoryFactAttribute::Contact
                && fact.value == "old@example.com"
                && fact.change_type == MemoryFactChangeType::Historical
        }));
        assert!(zh_email_facts.iter().any(|fact| {
            fact.attribute == MemoryFactAttribute::Contact
                && fact.value == "new@example.com"
                && fact.change_type == MemoryFactChangeType::Update
        }));
    }

    #[test]
    fn test_detect_memory_facts_supports_same_sentence_mixed_slot_transitions() {
        let facts = detect_memory_facts(&test_unit(
            "I moved from Shanghai to Beijing and changed my email from old@example.com to new@example.com",
        ));
        assert_eq!(facts.len(), 4);
        assert!(facts.iter().any(|fact| {
            fact.attribute == MemoryFactAttribute::Residence
                && fact.value == "Shanghai"
                && fact.change_type == MemoryFactChangeType::Historical
        }));
        assert!(facts.iter().any(|fact| {
            fact.attribute == MemoryFactAttribute::Residence
                && fact.value == "Beijing"
                && fact.change_type == MemoryFactChangeType::Update
        }));
        assert!(facts.iter().any(|fact| {
            fact.attribute == MemoryFactAttribute::Contact
                && fact.value == "old@example.com"
                && fact.change_type == MemoryFactChangeType::Historical
        }));
        assert!(facts.iter().any(|fact| {
            fact.attribute == MemoryFactAttribute::Contact
                && fact.value == "new@example.com"
                && fact.change_type == MemoryFactChangeType::Update
        }));

        let zh_facts = detect_memory_facts(&test_unit(
            "我从上海搬到北京，并且把邮箱从 old@example.com 改成 new@example.com",
        ));
        assert_eq!(zh_facts.len(), 4);
        assert!(zh_facts.iter().any(|fact| {
            fact.attribute == MemoryFactAttribute::Residence
                && fact.value == "上海"
                && fact.change_type == MemoryFactChangeType::Historical
        }));
        assert!(zh_facts.iter().any(|fact| {
            fact.attribute == MemoryFactAttribute::Residence
                && fact.value == "北京"
                && fact.change_type == MemoryFactChangeType::Update
        }));
        assert!(zh_facts.iter().any(|fact| {
            fact.attribute == MemoryFactAttribute::Contact
                && fact.value == "old@example.com"
                && fact.change_type == MemoryFactChangeType::Historical
        }));
        assert!(zh_facts.iter().any(|fact| {
            fact.attribute == MemoryFactAttribute::Contact
                && fact.value == "new@example.com"
                && fact.change_type == MemoryFactChangeType::Update
        }));
    }

    #[test]
    fn test_detect_memory_facts_supports_pronoun_aware_transition_patterns() {
        let facts = detect_memory_facts(&test_unit(
            "John Doe moved from Shanghai to Beijing and changed his email from old@example.com to new@example.com",
        ));

        assert_eq!(facts.len(), 4);
        assert!(facts.iter().any(|fact| {
            fact.attribute == MemoryFactAttribute::Residence
                && fact.value == "Shanghai"
                && fact.subject == MemoryFactSubject::External
                && fact.subject_key == "external:john_doe"
                && fact.change_type == MemoryFactChangeType::Historical
        }));
        assert!(facts.iter().any(|fact| {
            fact.attribute == MemoryFactAttribute::Residence
                && fact.value == "Beijing"
                && fact.subject == MemoryFactSubject::External
                && fact.subject_key == "external:john_doe"
                && fact.change_type == MemoryFactChangeType::Update
        }));
        assert!(facts.iter().any(|fact| {
            fact.attribute == MemoryFactAttribute::Contact
                && fact.value == "old@example.com"
                && fact.subject == MemoryFactSubject::External
                && fact.subject_key == "external:john_doe"
                && fact.change_type == MemoryFactChangeType::Historical
        }));
        assert!(facts.iter().any(|fact| {
            fact.attribute == MemoryFactAttribute::Contact
                && fact.value == "new@example.com"
                && fact.subject == MemoryFactSubject::External
                && fact.subject_key == "external:john_doe"
                && fact.change_type == MemoryFactChangeType::Update
        }));

        let employment_facts = detect_memory_facts(&test_unit(
            "John Doe changed his job from OpenAI to Anthropic",
        ));
        assert_eq!(employment_facts.len(), 2);
        assert!(employment_facts.iter().any(|fact| {
            fact.attribute == MemoryFactAttribute::Employment
                && fact.value == "OpenAI"
                && fact.subject == MemoryFactSubject::External
                && fact.subject_key == "external:john_doe"
                && fact.change_type == MemoryFactChangeType::Historical
        }));
        assert!(employment_facts.iter().any(|fact| {
            fact.attribute == MemoryFactAttribute::Employment
                && fact.value == "Anthropic"
                && fact.subject == MemoryFactSubject::External
                && fact.subject_key == "external:john_doe"
                && fact.change_type == MemoryFactChangeType::Update
        }));

        let organization_facts = detect_memory_facts(&test_unit(
            "Acme Labs changed its headquarters from Berlin to Paris",
        ));
        assert_eq!(organization_facts.len(), 2);
        assert!(organization_facts.iter().any(|fact| {
            fact.attribute == MemoryFactAttribute::Residence
                && fact.value == "Berlin"
                && fact.subject == MemoryFactSubject::Organization
                && fact.subject_key == "organization:acme_labs"
                && fact.change_type == MemoryFactChangeType::Historical
        }));
        assert!(organization_facts.iter().any(|fact| {
            fact.attribute == MemoryFactAttribute::Residence
                && fact.value == "Paris"
                && fact.subject == MemoryFactSubject::Organization
                && fact.subject_key == "organization:acme_labs"
                && fact.change_type == MemoryFactChangeType::Update
        }));

        let zh_residence_facts =
            detect_memory_facts(&test_unit("我把地址从上海改成北京"));
        assert_eq!(zh_residence_facts.len(), 2);
        assert!(zh_residence_facts.iter().any(|fact| {
            fact.attribute == MemoryFactAttribute::Residence
                && fact.value == "上海"
                && fact.subject == MemoryFactSubject::User
                && fact.subject_key == "user:self"
                && fact.change_type == MemoryFactChangeType::Historical
        }));
        assert!(zh_residence_facts.iter().any(|fact| {
            fact.attribute == MemoryFactAttribute::Residence
                && fact.value == "北京"
                && fact.subject == MemoryFactSubject::User
                && fact.subject_key == "user:self"
                && fact.change_type == MemoryFactChangeType::Update
        }));
    }

    #[test]
    fn test_detect_memory_facts_does_not_misread_residence_move_as_employment_transition() {
        let facts = detect_memory_facts(&test_unit("John Doe moved from Shanghai to Beijing"));
        assert_eq!(facts.len(), 2);
        assert!(facts
            .iter()
            .all(|fact| fact.attribute == MemoryFactAttribute::Residence));
    }

    #[test]
    fn test_detect_memory_facts_carries_english_subject_across_segments() {
        let facts =
            detect_memory_facts(&test_unit("John Doe lives in Shanghai and works at OpenAI"));

        assert_eq!(facts.len(), 2);
        assert!(facts.iter().any(|fact| {
            fact.attribute == MemoryFactAttribute::Residence
                && fact.subject == MemoryFactSubject::External
                && fact.subject_key == "external:john_doe"
                && fact.value == "Shanghai"
        }));
        assert!(facts.iter().any(|fact| {
            fact.attribute == MemoryFactAttribute::Employment
                && fact.subject == MemoryFactSubject::External
                && fact.subject_key == "external:john_doe"
                && fact.value == "OpenAI"
        }));
    }

    #[test]
    fn test_detect_memory_facts_carries_chinese_subject_across_segments() {
        let facts = detect_memory_facts(&test_unit("张三住在上海，并且在OpenAI工作"));

        assert_eq!(facts.len(), 2);
        assert!(facts.iter().any(|fact| {
            fact.attribute == MemoryFactAttribute::Residence
                && fact.subject == MemoryFactSubject::External
                && fact.subject_key == "external:张三"
                && fact.value == "上海"
        }));
        assert!(facts.iter().any(|fact| {
            fact.attribute == MemoryFactAttribute::Employment
                && fact.subject == MemoryFactSubject::External
                && fact.subject_key == "external:张三"
                && fact.value == "OpenAI"
        }));
    }

    #[test]
    fn test_detect_memory_facts_eval_scorecard_meets_threshold() {
        let cases: Vec<EvalCase> =
            serde_json::from_str(include_str!("../testdata/fact_extraction_eval_cases.json"))
                .expect("eval fixture should parse");

        let mut scorecard = EvalScorecard::default();

        for case in cases {
            let facts = detect_memory_facts(&test_unit(&case.content));
            for expected in &case.expected {
                scorecard.expected_total += 1;

                let matched = facts.iter().find(|fact| fact.value == expected.value);
                let Some(matched) = matched else {
                    continue;
                };
                scorecard.matched_total += 1;

                if memory_fact_attribute_label(matched.attribute) == expected.attribute {
                    scorecard.attribute_hits += 1;
                }
                if memory_fact_subject_label(matched.subject) == expected.subject {
                    scorecard.subject_hits += 1;
                }
                if matched.subject_key == expected.subject_key {
                    scorecard.subject_key_hits += 1;
                }
                if memory_fact_change_type_label(matched.change_type) == expected.change_type {
                    scorecard.change_type_hits += 1;
                }
                if memory_fact_attribute_label(matched.attribute) == expected.attribute
                    && memory_fact_subject_label(matched.subject) == expected.subject
                    && matched.subject_key == expected.subject_key
                    && memory_fact_change_type_label(matched.change_type) == expected.change_type
                {
                    scorecard.full_hits += 1;
                }
            }
        }

        let expected_total = scorecard.expected_total as f32;
        let coverage = scorecard.matched_total as f32 / expected_total;
        let attribute_rate = scorecard.attribute_hits as f32 / expected_total;
        let subject_rate = scorecard.subject_hits as f32 / expected_total;
        let subject_key_rate = scorecard.subject_key_hits as f32 / expected_total;
        let change_type_rate = scorecard.change_type_hits as f32 / expected_total;
        let full_rate = scorecard.full_hits as f32 / expected_total;

        assert!(
            coverage >= 0.95,
            "coverage too low: {coverage:.2} with scorecard matched={} expected={}",
            scorecard.matched_total,
            scorecard.expected_total
        );
        assert!(
            attribute_rate >= 0.95,
            "attribute_rate too low: {attribute_rate:.2}"
        );
        assert!(
            subject_rate >= 0.95,
            "subject_rate too low: {subject_rate:.2}"
        );
        assert!(
            subject_key_rate >= 0.90,
            "subject_key_rate too low: {subject_key_rate:.2}"
        );
        assert!(
            change_type_rate >= 0.90,
            "change_type_rate too low: {change_type_rate:.2}"
        );
        assert!(full_rate >= 0.85, "full_rate too low: {full_rate:.2}");
    }

    #[test]
    fn test_detect_memory_facts_eval_false_positive_false_negative_budget() {
        let cases: Vec<EvalCase> =
            serde_json::from_str(include_str!("../testdata/fact_extraction_eval_cases.json"))
                .expect("eval fixture should parse");

        let mut false_negatives = Vec::new();
        let mut false_positives = Vec::new();
        let mut expected_total = 0usize;

        for case in &cases {
            let facts = detect_memory_facts(&test_unit(&case.content));
            let expected_keys = case
                .expected
                .iter()
                .map(expected_fact_key)
                .collect::<HashSet<_>>();
            let actual_keys = facts.iter().map(actual_fact_key).collect::<HashSet<_>>();

            expected_total += expected_keys.len();

            for missing in expected_keys.difference(&actual_keys) {
                false_negatives.push(format!("{} => {}", case.content, missing));
            }
            for extra in actual_keys.difference(&expected_keys) {
                false_positives.push(format!("{} => {}", case.content, extra));
            }
        }

        let fn_rate = false_negatives.len() as f32 / expected_total as f32;
        let fp_rate = false_positives.len() as f32 / expected_total as f32;

        assert!(
            fn_rate <= 0.10,
            "false negative rate too high: {fn_rate:.2}\n{}",
            false_negatives.join("\n")
        );
        assert!(
            fp_rate <= 0.15,
            "false positive rate too high: {fp_rate:.2}\n{}",
            false_positives.join("\n")
        );
    }

    #[test]
    fn test_detect_memory_facts_eval_dimension_report_meets_thresholds() {
        let cases: Vec<EvalCase> =
            serde_json::from_str(include_str!("../testdata/fact_extraction_eval_cases.json"))
                .expect("eval fixture should parse");

        let mut by_language: BTreeMap<String, EvalBucketStats> = BTreeMap::new();
        let mut by_attribute: BTreeMap<String, EvalBucketStats> = BTreeMap::new();
        let mut by_change_type: BTreeMap<String, EvalBucketStats> = BTreeMap::new();

        for case in &cases {
            let language_key = eval_case_language(&case.content).to_string();
            let facts = detect_memory_facts(&test_unit(&case.content));
            let expected_keys = case
                .expected
                .iter()
                .map(expected_fact_key)
                .collect::<StdHashSet<_>>();
            let actual_keys = facts.iter().map(actual_fact_key).collect::<StdHashSet<_>>();

            for expected in &case.expected {
                by_language
                    .entry(language_key.clone())
                    .or_default()
                    .expected += 1;
                by_attribute
                    .entry(expected.attribute.clone())
                    .or_default()
                    .expected += 1;
                by_change_type
                    .entry(expected.change_type.clone())
                    .or_default()
                    .expected += 1;

                let expected_key = expected_fact_key(expected);
                if actual_keys.contains(&expected_key) {
                    by_language
                        .entry(language_key.clone())
                        .or_default()
                        .exact_hits += 1;
                    by_attribute
                        .entry(expected.attribute.clone())
                        .or_default()
                        .exact_hits += 1;
                    by_change_type
                        .entry(expected.change_type.clone())
                        .or_default()
                        .exact_hits += 1;
                }
            }

            let false_positive_count = actual_keys.difference(&expected_keys).count();
            if false_positive_count > 0 {
                by_language.entry(language_key).or_default().false_positives +=
                    false_positive_count;
            }
        }

        for (language, stats) in &by_language {
            let exact_rate = bucket_rate(stats);
            let fp_rate = if stats.expected == 0 {
                0.0
            } else {
                stats.false_positives as f32 / stats.expected as f32
            };
            assert!(
                exact_rate >= 0.90,
                "language bucket {language} exact rate too low: {exact_rate:.2} stats={stats:?}"
            );
            assert!(
                fp_rate <= 0.20,
                "language bucket {language} false positive rate too high: {fp_rate:.2} stats={stats:?}"
            );
        }

        for (attribute, stats) in &by_attribute {
            let exact_rate = bucket_rate(stats);
            assert!(
                exact_rate >= 0.80,
                "attribute bucket {attribute} exact rate too low: {exact_rate:.2} stats={stats:?}"
            );
        }

        for (change_type, stats) in &by_change_type {
            let exact_rate = bucket_rate(stats);
            assert!(
                exact_rate >= 0.80,
                "change_type bucket {change_type} exact rate too low: {exact_rate:.2} stats={stats:?}"
            );
        }
    }

}
