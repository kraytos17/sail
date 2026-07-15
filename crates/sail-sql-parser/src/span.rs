use chumsky::prelude::SimpleSpan;

/// A span in the source code.
/// The offsets are measured in the number of characters from the beginning of the input,
/// starting from 0.
#[derive(Debug, Clone, Copy, Default, PartialEq, Eq)]
pub struct TokenSpan {
    /// The start offset of the span.
    pub start: usize,
    /// The end (exclusive) offset of the span.
    pub end: usize,
}

impl TokenSpan {
    #[must_use]
    pub const fn is_empty(&self) -> bool {
        self.start >= self.end
    }

    #[must_use]
    pub fn union(&self, other: &Self) -> Self {
        match (self.is_empty(), other.is_empty()) {
            (true, true) => Self::default(),
            (true, false) => *other,
            (false, true) => *self,
            (false, false) => Self {
                start: self.start.min(other.start),
                end: self.end.max(other.end),
            },
        }
    }

    pub fn union_all<I>(iter: I) -> Self
    where
        I: IntoIterator<Item = Self>,
    {
        iter.into_iter()
            .reduce(|acc, span| acc.union(&span))
            .unwrap_or_default()
    }
}

impl<C> From<SimpleSpan<usize, C>> for TokenSpan {
    fn from(span: SimpleSpan<usize, C>) -> Self {
        Self {
            start: span.start,
            end: span.end,
        }
    }
}
