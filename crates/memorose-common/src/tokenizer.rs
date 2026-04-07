/// Count retrieval-context tokens with a small deterministic estimator.
///
/// This intentionally avoids model-specific network-loaded tokenizers in the hot path. It is
/// conservative enough for retrieval budgeting: CJK characters count as individual tokens,
/// punctuation counts separately, ASCII runs are approximated at ~4 chars/token, and other
/// alphabetic runs at ~2 chars/token.
pub fn count_tokens(text: &str) -> usize {
    fn flush_run(total: &mut usize, run_len: &mut usize, chars_per_token: usize) {
        if *run_len > 0 {
            *total += (*run_len).div_ceil(chars_per_token);
            *run_len = 0;
        }
    }

    let mut total = 0;
    let mut ascii_run = 0;
    let mut unicode_run = 0;

    for ch in text.chars() {
        if ch.is_whitespace() {
            flush_run(&mut total, &mut ascii_run, 4);
            flush_run(&mut total, &mut unicode_run, 2);
        } else if is_cjk(ch) {
            flush_run(&mut total, &mut ascii_run, 4);
            flush_run(&mut total, &mut unicode_run, 2);
            total += 1;
        } else if ch.is_ascii_alphanumeric() {
            flush_run(&mut total, &mut unicode_run, 2);
            ascii_run += 1;
        } else if ch.is_alphanumeric() {
            flush_run(&mut total, &mut ascii_run, 4);
            unicode_run += 1;
        } else {
            flush_run(&mut total, &mut ascii_run, 4);
            flush_run(&mut total, &mut unicode_run, 2);
            total += 1;
        }
    }

    flush_run(&mut total, &mut ascii_run, 4);
    flush_run(&mut total, &mut unicode_run, 2);
    total
}

fn is_cjk(ch: char) -> bool {
    matches!(
        ch as u32,
        0x3400..=0x4DBF
            | 0x4E00..=0x9FFF
            | 0xF900..=0xFAFF
            | 0x3040..=0x30FF
            | 0xAC00..=0xD7AF
    )
}

#[cfg(test)]
mod tests {
    use super::count_tokens;

    #[test]
    fn count_tokens_handles_plain_english() {
        assert_eq!(count_tokens(""), 0);
        assert_eq!(count_tokens("Rust"), 1);
        assert_eq!(count_tokens("Memorose runtime"), 4);
    }

    #[test]
    fn count_tokens_handles_cjk_and_punctuation() {
        assert_eq!(count_tokens("北京"), 2);
        assert!(count_tokens("我住在北京。") >= 6);
    }
}
