use std::iter::Peekable;
use std::str::Chars;

/// Statically extracts variable names referenced by a rule expression.
///
/// The scan is purely syntactic — it never evaluates the rule — so variables
/// in untaken `&&`/`||`/`if` branches are still found. The following are
/// never reported:
/// - contents of `"..."`/`` `...` `` strings (except `${...}` interpolations,
///   whose embedded expressions *are* scanned),
/// - `//` line comments and `/* ... */` block comments,
/// - function-call names (an identifier immediately followed by `(` or `!`),
/// - property names (an identifier immediately preceded by `.`),
/// - Rhai reserved keywords (`in` included) and booleans.
///
/// Only ASCII identifiers (`[A-Za-z_$][A-Za-z0-9_$]*`) are reported;
/// non-ASCII identifiers are rejected by the Rhai parser anyway.
/// Names are returned in first-seen order, deduplicated.
///
/// ```rust
/// # use sail_rhai_udf::extract_variables;
/// assert_eq!(
///     extract_variables("X in [1,2,3]"),
///     vec!["X".to_string()]
/// );
/// assert_eq!(
///     extract_variables("if A { foo(B) } else { C }"),
///     vec!["A".to_string(), "B".to_string(), "C".to_string()]
/// );
/// ```
#[must_use]
pub fn extract_variables(expr: &str) -> Vec<String> {
    let mut scanner = Scanner::new(expr);
    scanner.scan_top();
    scanner.names
}

/// Rhai reserved words that can never be variable references.
const KEYWORDS: &[&str] = &[
    "as", "break", "catch", "const", "continue", "do", "else", "export", "false", "fn", "for",
    "if", "import", "in", "is", "let", "loop", "match", "of", "private", "return", "switch",
    "throw", "throws", "true", "try", "until", "while", "with",
];

struct Scanner<'a> {
    chars: Peekable<Chars<'a>>,
    names: Vec<String>,
    /// Previous input character, for `.prop` detection.
    prev: Option<char>,
}

impl<'a> Scanner<'a> {
    fn new(expr: &'a str) -> Self {
        Self {
            chars: expr.chars().peekable(),
            names: Vec::new(),
            prev: None,
        }
    }

    fn scan_top(&mut self) {
        while let Some(ch) = self.chars.next() {
            match ch {
                '"' | '`' => self.scan_string(ch),
                '/' if self.chars.peek() == Some(&'/') => self.scan_line_comment(),
                '/' if self.chars.peek() == Some(&'*') => {
                    self.prev = Some(ch);
                    self.chars.next();
                    self.scan_block_comment();
                }
                c if is_ident_start(c) => self.scan_identifier(c, self.prev),
                _ => self.prev = Some(ch),
            }
        }
    }

    /// Scans `${...}` interpolation content with brace-depth counting.
    /// Called after consuming `$`; the `{` may or may not have been consumed.
    fn scan_interpolation(&mut self) {
        let mut depth = 0_usize;
        // Consume the opening brace if present.
        if self.chars.peek() == Some(&'{') {
            self.chars.next();
            depth = 1;
        }
        if depth == 0 {
            self.prev = Some('$');
            return;
        }
        while let Some(ch) = self.chars.next() {
            match ch {
                '{' => {
                    depth += 1;
                    self.prev = Some(ch);
                }
                '}' => {
                    depth -= 1;
                    self.prev = Some(ch);
                    if depth == 0 {
                        return;
                    }
                }
                '"' | '`' => self.scan_string(ch),
                '/' if self.chars.peek() == Some(&'/') => self.scan_line_comment(),
                '/' if self.chars.peek() == Some(&'*') => {
                    self.prev = Some(ch);
                    self.chars.next();
                    self.scan_block_comment();
                }
                c if is_ident_start(c) => self.scan_identifier(c, self.prev),
                _ => self.prev = Some(ch),
            }
        }
    }

    fn scan_string(&mut self, quote: char) {
        self.prev = Some(quote);
        while let Some(ch) = self.chars.next() {
            if ch == '\\' {
                if let Some(e) = self.chars.next() {
                    self.prev = Some(e);
                }
                continue;
            }
            if ch == '$' && quote == '"' && self.chars.peek() == Some(&'{') {
                self.scan_interpolation();
                continue;
            }

            self.prev = Some(ch);
            if ch == quote {
                return;
            }
        }
    }

    fn scan_line_comment(&mut self) {
        for ch in self.chars.by_ref() {
            self.prev = Some(ch);
            if ch == '\n' {
                return;
            }
        }
    }

    fn scan_block_comment(&mut self) {
        while let Some(ch) = self.chars.next() {
            self.prev = Some(ch);
            if ch == '*' && self.chars.peek() == Some(&'/') {
                self.chars.next();
                self.prev = Some('/');
                return;
            }
        }
    }

    fn scan_identifier(&mut self, first: char, before: Option<char>) {
        let mut ident = String::from(first);
        while self
            .chars
            .peek()
            .is_some_and(|&c| c.is_ascii_alphanumeric() || c == '_' || c == '$')
        {
            if let Some(c) = self.chars.next() {
                ident.push(c);
            }
        }

        self.prev = ident.chars().last();
        // Property access (`.foo`): not a variable.
        if before == Some('.') {
            return;
        }
        // Function-call name (`foo(`, `foo!(`).
        if self.chars.peek().is_some_and(|&c| c == '(' || c == '!') {
            return;
        }
        self.record_identifier(&ident);
    }

    fn record_identifier(&mut self, ident: &str) {
        if KEYWORDS.contains(&ident) {
            return;
        }
        if !self.names.iter().any(|n| n == ident) {
            self.names.push(ident.to_string());
        }
    }
}

fn is_ident_start(c: char) -> bool {
    c.is_ascii_alphabetic() || c == '_' || c == '$'
}

#[cfg(test)]
mod tests {
    use proptest::prelude::*;

    use super::*;

    #[test]
    fn table_cases() {
        let cases: &[(&str, &[&str])] = &[
            ("X in [1,2,3]", &["X"]),
            ("\"X\" + Y", &["Y"]),
            ("`X` + Y", &["Y"]),
            ("X // Y", &["X"]),
            ("X /* Y */ + Z", &["X", "Z"]),
            ("if X { Y } else { Z }", &["X", "Y", "Z"]),
            ("X && Y || Z", &["X", "Y", "Z"]),
            ("foo(X) + Y", &["X", "Y"]),
            ("foo!(X) + Y", &["X", "Y"]),
            ("YEAR != ()", &["YEAR"]),
            ("type_of(YEAR) == \"()\"", &["YEAR"]),
            ("is_null(YEAR)", &["YEAR"]),
            ("B + A + B", &["B", "A"]),
            ("é + X", &["X"]),
            ("\"prefix ${CODE}\" == X", &["CODE", "X"]),
            ("m.foo + X", &["m", "X"]),
            ("let a = X + 1", &["a", "X"]),
            ("7 / 2", &[]),
            ("true && X", &["X"]),
            ("in", &[]),
        ];
        for (expr, expected) in cases {
            let got = extract_variables(expr);
            let want: Vec<String> = expected.iter().map(ToString::to_string).collect();
            assert_eq!(got, want, "expr: {expr}");
        }
    }

    proptest! {
        #![proptest_config(ProptestConfig::with_cases(256))]
        #[test]
        fn extraction_is_deterministic_and_keyword_free(
            vars in proptest::collection::vec("[A-Z][A-Z0-9_]{0,5}", 1..6),
        ) {
            // Join with an operator so pool entries stay separate identifiers.
            let expr = vars.join(" + ");
            let first = extract_variables(&expr);
            let second = extract_variables(&expr);
            prop_assert_eq!(&first, &second);
            for name in &first {
                prop_assert!(!KEYWORDS.contains(&name.as_str()), "{name}");
                prop_assert!(vars.contains(name), "{name} not in {vars:?}");
            }
            // Every pool var placed as a bare operand must be found.
            for v in &vars {
                prop_assert!(first.contains(v), "{v} missing from {first:?} for {expr}");
            }
        }
    }
}
