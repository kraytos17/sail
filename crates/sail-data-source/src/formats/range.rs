use std::sync::Arc;

use datafusion_common::{Result, exec_datafusion_err};
use datafusion_datasource::{FileRange, PartitionedFile, RangeCalculation};
use futures::StreamExt;
use object_store::path::Path;
use object_store::{GetOptions, GetRange, ObjectStore};

/// Maximum bytes fetched in a single probe request when searching for a
/// record boundary. Newlines in CSV/text sources are normally within a few
/// hundred bytes of a split point; 1MB covers pathological rows while keeping
/// every request small.
const PROBE_CHUNK_SIZE: u64 = 1024 * 1024;

/// Drop-in replacement for `datafusion_datasource::calculate_range` with
/// identical boundary semantics but bounded probe requests.
///
/// Upstream `calculate_range` searches for the first newline with
/// `start..file_size` GETs, so splitting a 10GB file issues dozens of
/// concurrent multi-GB streams that object stores tear down mid-response
/// (`Generic S3 error: ... request or response body error`). This instead
/// fetches at most `PROBE_CHUNK_SIZE` bytes per request, issuing follow-up
/// requests only while no newline has been found yet.
pub async fn calculate_range_bounded(
    file: &PartitionedFile,
    store: &Arc<dyn ObjectStore>,
    terminator: Option<u8>,
) -> Result<RangeCalculation> {
    let file_size = file.object_meta.size;
    let newline = terminator.unwrap_or(b'\n');

    match file.range {
        None => Ok(RangeCalculation::Range(None)),
        Some(FileRange { start, end }) => {
            let start: u64 = start.try_into().map_err(|_| {
                exec_datafusion_err!("Expect start range to fit in u64, got {start}")
            })?;
            let end: u64 = end
                .try_into()
                .map_err(|_| exec_datafusion_err!("Expect end range to fit in u64, got {end}"))?;

            let start_delta = if start != 0 {
                find_first_newline_bounded(
                    store,
                    &file.object_meta.location,
                    start - 1,
                    file_size,
                    newline,
                )
                .await?
            } else {
                0
            };

            if start + start_delta > end {
                return Ok(RangeCalculation::TerminateEarly);
            }

            let end_delta = if end != file_size {
                find_first_newline_bounded(
                    store,
                    &file.object_meta.location,
                    end - 1,
                    file_size,
                    newline,
                )
                .await?
            } else {
                0
            };

            let range = start + start_delta..end + end_delta;

            if range.start >= range.end {
                return Ok(RangeCalculation::TerminateEarly);
            }

            Ok(RangeCalculation::Range(Some(range)))
        }
    }
}

/// Position of the first `newline` byte in `start..end`, or the number of
/// bytes scanned when no newline is found before `end`.
///
/// Same contract as DataFusion's internal `find_first_newline`, except every
/// GET is capped at `PROBE_CHUNK_SIZE` bytes instead of spanning to `end` in
/// a single request.
async fn find_first_newline_bounded(
    object_store: &Arc<dyn ObjectStore>,
    location: &Path,
    start: u64,
    end: u64,
    newline: u8,
) -> Result<u64> {
    let mut scanned: u64 = 0;
    while start + scanned < end {
        let fetch_end = (start + scanned + PROBE_CHUNK_SIZE).min(end);
        let options = GetOptions {
            range: Some(GetRange::Bounded(start + scanned..fetch_end)),
            ..Default::default()
        };
        let result = object_store.get_opts(location, options).await?;
        let mut stream = result.into_stream();
        let mut consumed: u64 = 0;
        let mut found = None;
        while let Some(chunk) = stream.next().await.transpose()? {
            if let Some(position) = chunk.iter().position(|&byte| byte == newline) {
                found = Some(scanned + consumed + position as u64);
                break;
            }
            consumed += chunk.len() as u64;
        }
        if let Some(position) = found {
            return Ok(position);
        }
        scanned += consumed;
        if start + scanned < fetch_end {
            // Short read: the store returned fewer bytes than requested.
            // Treat as EOF, mirroring upstream (which returns bytes scanned).
            break;
        }
    }
    Ok(scanned)
}

#[cfg(test)]
mod tests {
    use datafusion_datasource::calculate_range;
    use object_store::memory::InMemory;
    use object_store::{ObjectStore, ObjectStoreExt, PutPayload};

    use super::*;

    fn partitioned_file(path: &str, size: u64, range: Option<(i64, i64)>) -> PartitionedFile {
        let mut file = PartitionedFile::new(path.to_string(), size);
        if let Some((start, end)) = range {
            file = file.with_range(start, end);
        }
        file
    }

    fn normalized(calc: &RangeCalculation) -> Option<Option<std::ops::Range<u64>>> {
        match calc {
            RangeCalculation::Range(range) => Some(range.clone()),
            RangeCalculation::TerminateEarly => None,
        }
    }

    fn assert_same(actual: RangeCalculation, expected: RangeCalculation) {
        assert_eq!(normalized(&actual), normalized(&expected));
    }

    async fn check_equivalence(
        store: &InMemory,
        path: &str,
        size: u64,
        ranges: &[(i64, i64)],
    ) -> Result<()> {
        let store = Arc::new(store.clone()) as Arc<dyn ObjectStore>;
        for (start, end) in ranges {
            let file = partitioned_file(path, size, Some((*start, *end)));
            let bounded = calculate_range_bounded(&file, &store, None).await?;
            let upstream = calculate_range(&file, &store, None).await?;
            assert_same(bounded, upstream);
        }
        // Whole-file reads take no range and issue no probe.
        let whole = partitioned_file(path, size, None);
        let bounded = calculate_range_bounded(&whole, &store, None).await?;
        assert_same(bounded, RangeCalculation::Range(None));
        Ok(())
    }

    #[tokio::test]
    async fn bounded_probes_match_upstream_on_short_lines() -> Result<()> {
        let store = InMemory::new();
        let data = b"aaa\nbb\nc\ndddd\n".to_vec();
        let path = Path::from("short.csv");
        store
            .put(&path, PutPayload::from(bytes::Bytes::from(data.clone())))
            .await?;
        let size = data.len() as u64;
        // Split points at every byte offset, plus degenerate ranges.
        // Note: (0, 0) is excluded: `end - 1` underflows for empty ranges
        // in upstream `calculate_range` as well; the partitioner never
        // emits empty ranges.
        let mut ranges = vec![(0, 1), (4, 4)];
        for start in 0..size as i64 {
            for end in [start, start + 1, size as i64] {
                // (0, 0) excluded: see note above.
                if end > 0 {
                    ranges.push((start, end));
                }
            }
        }
        check_equivalence(&store, "short.csv", size, &ranges).await
    }

    #[tokio::test]
    async fn bounded_probes_match_upstream_across_long_lines() -> Result<()> {
        let store = InMemory::new();
        // A 3MB line forces multi-chunk probing (chunk = 1MB); no trailing newline.
        let mut data = b"head\n".to_vec();
        data.extend(vec![b'x'; 3 * 1024 * 1024]);
        data.extend(b"\ntail");
        let path = Path::from("long.csv");
        store
            .put(&path, PutPayload::from(bytes::Bytes::from(data.clone())))
            .await?;
        let size = data.len() as u64;
        let chunk = PROBE_CHUNK_SIZE as i64;
        let ranges = vec![
            (0, size as i64),
            (1, size as i64),
            (5, size as i64),
            (5, 5 + chunk),
            (5, 5 + 2 * chunk),
            (chunk, 2 * chunk),
            (2 * chunk, size as i64),
            (size as i64 - 4, size as i64),
            (size as i64, size as i64),
            (10, 5),
        ];
        check_equivalence(&store, "long.csv", size, &ranges).await
    }
}
