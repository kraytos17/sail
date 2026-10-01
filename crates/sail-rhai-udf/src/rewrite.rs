use std::borrow::Cow;

/// Rewrites bare integer literals as floats when the rule performs division.
///
/// Rhai divides integers with truncation (`7 / 2 == 3`). DQ rules expect
/// Spark-like semantics (`7 / 2 == 3.5`), so when `expr` contains `/` every
/// bare decimal integer literal gains `.0` before compilation.
///
/// When `expr` contains no `/`, the input is returned borrowed and unchanged.
/// Otherwise the rewritten text follows these rules:
/// - Quoted (`"..."`) and backtick (`` `...` ``) string contents are untouched,
///   including `${...}` interpolations.
/// - `//` line comments and `/* ... */` block comments are untouched.
/// - Digits continuing an identifier are untouched (`IV1` stays `IV1`).
/// - Numbers already containing `.` pass through (`3.5` stays `3.5`;
///   a trailing `5.` is also left alone, as is property access like `5.foo`).
/// - Scientific notation passes through (`1e3` stays `1e3`).
/// - Hex-style prefixes and unit suffixes are left alone (`0x1F` stays `0x1F`).
/// - A bare `-` before digits is a separator, so `x-5` becomes `x-5.0`.
///
/// Non-ASCII bytes pass through untouched (UTF-8 is never split).
/// Rewriting is idempotent: applying it twice yields the same text.
///
/// ```rust
/// # use sail_rhai_udf::maybe_rewrite_integers;
/// # use std::borrow::Cow;
/// assert!(matches!(maybe_rewrite_integers("IV1 + 1"), Cow::Borrowed(_)));
/// assert_eq!(maybe_rewrite_integers("7 / 2").as_ref(), "7.0 / 2.0");
/// assert_eq!(
///     maybe_rewrite_integers("IV1 + 1 / 2").as_ref(),
///     "IV1 + 1.0 / 2.0"
/// );
/// ```
#[must_use]
pub fn maybe_rewrite_integers(expr: &str) -> Cow<'_, str> {
    if !expr.contains('/') {
        return Cow::Borrowed(expr);
    }

    #[derive(Clone, Copy, PartialEq, Eq)]
    enum State {
        Normal,
        InString(char),
        LineComment,
        BlockComment,
    }

    let mut state = State::Normal;
    let mut out = String::with_capacity(expr.len() + 8);
    let mut chars = expr.chars().peekable();
    let mut prev: Option<char> = None;
    while let Some(ch) = chars.next() {
        match state {
            State::Normal => match ch {
                '"' | '`' => {
                    state = State::InString(ch);
                    out.push(ch);
                    prev = Some(ch);
                }
                '/' if chars.peek().is_some_and(|&c| c == '/') => {
                    state = State::LineComment;
                    out.push(ch);
                    prev = Some(ch);
                }
                '/' if chars.peek().is_some_and(|&c| c == '*') => {
                    state = State::BlockComment;
                    out.push(ch);
                    prev = Some(ch);
                }
                '0'..='9' if !continues_token(prev) => {
                    let mut run = String::from(ch);
                    while chars
                        .peek()
                        .is_some_and(|&c| c.is_ascii_digit() || c == '_')
                    {
                        if let Some(c) = chars.next() {
                            run.push(c);
                        }
                    }

                    let mut lookahead = chars.clone();
                    let n1 = lookahead.next();
                    let n2 = lookahead.next();
                    let n3 = lookahead.next();
                    if is_already_float(n1, n2, n3) {
                        out.push_str(&run);
                    } else {
                        out.push_str(&run);
                        out.push_str(".0");
                    }
                    prev = run.chars().last();
                }
                _ => {
                    out.push(ch);
                    prev = Some(ch);
                }
            },
            State::InString(quote) => {
                out.push(ch);
                prev = Some(ch);
                if ch == '\\' {
                    if let Some(e) = chars.next() {
                        out.push(e);
                        prev = Some(e);
                    }
                } else if ch == quote {
                    state = State::Normal;
                }
            }
            State::LineComment => {
                out.push(ch);
                prev = Some(ch);
                if ch == '\n' {
                    state = State::Normal;
                }
            }
            State::BlockComment => {
                out.push(ch);
                prev = Some(ch);
                if ch == '*' && chars.peek().is_some_and(|&c| c == '/') {
                    if let Some(slash) = chars.next() {
                        out.push(slash);
                        prev = Some(slash);
                    }
                    state = State::Normal;
                }
            }
        }
    }
    Cow::Owned(out)
}

/// Whether `prev` continues a token, so a digit is not a fresh literal.
/// Covers identifier tails (`IV1`), float fraction tails (`.0`),
/// and `$`-style identifier characters.
fn continues_token(prev: Option<char>) -> bool {
    matches!(prev, Some(c) if c.is_ascii_alphanumeric() || c == '_' || c == '$' || c == '.')
}

/// Whether the characters after a digit run already make it a float
/// (or something else that must not gain `.0`).
fn is_already_float(n1: Option<char>, n2: Option<char>, n3: Option<char>) -> bool {
    match n1 {
        // `3.5`, trailing `5.`, or property access `5.foo`.
        Some('.') => true,
        // Hex prefixes (`0x1F`), unit suffixes (`2nd`).
        Some(c) if c.is_ascii_alphabetic() || c == '_' || c == '$' => true,
        // Scientific notation: `1e3`, `1E+3`, `1e-3`.
        Some('e' | 'E')
            if matches!(n2, Some(d) if d.is_ascii_digit())
                || matches!((n2, n3), (Some('+' | '-'), Some(d)) if d.is_ascii_digit()) =>
        {
            true
        }
        _ => false,
    }
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    #[test]
    fn borrow_when_no_division() {
        for expr in [
            "IV1 + 1",
            "EMP_ID - 5",
            "\"7\"",
            "X in [1,2,3]",
            "is_null(YEAR)",
        ] {
            assert!(
                matches!(maybe_rewrite_integers(expr), Cow::Borrowed(_)),
                "{expr} should borrow"
            );
        }
    }

    #[test]
    fn rewrites_division_operands() {
        assert_eq!(maybe_rewrite_integers("7 / 2").as_ref(), "7.0 / 2.0");
        assert_eq!(
            maybe_rewrite_integers("EMP_ID / 2").as_ref(),
            "EMP_ID / 2.0"
        );
        assert_eq!(maybe_rewrite_integers("x-5 / y").as_ref(), "x-5.0 / y");
        assert_eq!(maybe_rewrite_integers("-5 / 2").as_ref(), "-5.0 / 2.0");
    }

    #[test]
    fn leaves_identifiers_strings_and_floats_alone() {
        assert_eq!(maybe_rewrite_integers("IV1 / IV2").as_ref(), "IV1 / IV2");
        assert_eq!(
            maybe_rewrite_integers("\"7 / 2\" == \"7 / 2\"").as_ref(),
            "\"7 / 2\" == \"7 / 2\""
        );
        assert_eq!(maybe_rewrite_integers("3.5 / 2").as_ref(), "3.5 / 2.0");
        assert_eq!(maybe_rewrite_integers("1e3 / 2").as_ref(), "1e3 / 2.0");
        assert_eq!(
            maybe_rewrite_integers("X // comment 75\n/ 2").as_ref(),
            "X // comment 75\n/ 2.0"
        );
        assert_eq!(
            maybe_rewrite_integers("X /* 75 */ / 2").as_ref(),
            "X /* 75 */ / 2.0"
        );
    }

    #[test]
    fn preserves_unicode_bytes() {
        let expr = "café / 2";
        assert_eq!(maybe_rewrite_integers(expr).as_ref(), "café / 2.0");
        let expr = "\"héllo 7\" / 2";
        assert_eq!(maybe_rewrite_integers(expr).as_ref(), "\"héllo 7\" / 2.0");
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(256))]
        #[test]
        fn rewrite_is_idempotent(s in "\\PC*") {
            let once = maybe_rewrite_integers(&s).into_owned();
            let twice = maybe_rewrite_integers(&once).into_owned();
            prop_assert_eq!(once, twice);
        }

        #[test]
        fn rewrite_never_touches_strings(
            prefix in "[A-Za-z ]{0,8}",
            digits in "[0-9]{1,4}",
            suffix in "[A-Za-z ]{0,8}",
        ) {
            // A digit run fully inside quotes survives even when `/` forces a rewrite.
            let expr = format!("\"{prefix}{digits}{suffix}\" / 1");
            let out = maybe_rewrite_integers(&expr).into_owned();
            prop_assert!(out.contains(&format!("\"{prefix}{digits}{suffix}\"")), "{out}");
        }
    }
}
