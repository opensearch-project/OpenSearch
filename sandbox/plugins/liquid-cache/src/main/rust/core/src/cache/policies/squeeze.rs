//! Squeeze policies for liquid cache.

use crate::cache::{cached_batch::CacheEntry, transcode_liquid_inner};

/// What to do when we need to squeeze an entry?
#[derive(Debug, Clone)]
pub enum SqueezeOutcome {
    /// Replace the cache entry with a smaller in-memory representation.
    Replace(CacheEntry),
    /// Remove the entry entirely.
    Remove,
}

/// Policy that chooses the next representation for an entry under memory pressure.
pub trait SqueezePolicy: std::fmt::Debug + Send + Sync {
    /// Squeeze the entry.
    fn squeeze(&self, entry: &CacheEntry) -> SqueezeOutcome;
}

/// Evict the entry from memory.
#[derive(Debug, Default, Clone)]
pub struct Evict;

impl SqueezePolicy for Evict {
    fn squeeze(&self, _entry: &CacheEntry) -> SqueezeOutcome {
        SqueezeOutcome::Remove
    }
}

/// Transcode Arrow entries to liquid memory; evict liquid entries.
#[derive(Debug, Default, Clone)]
pub struct TranscodeEvict;

impl SqueezePolicy for TranscodeEvict {
    fn squeeze(&self, entry: &CacheEntry) -> SqueezeOutcome {
        match entry {
            CacheEntry::MemoryArrow(array) => match transcode_liquid_inner(array) {
                Ok(liquid_array) => {
                    SqueezeOutcome::Replace(CacheEntry::memory_liquid(liquid_array))
                }
                Err(_) => SqueezeOutcome::Remove,
            },
            CacheEntry::MemoryLiquid(_) => SqueezeOutcome::Remove,
        }
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::cache::cached_batch::CacheEntry;
    use arrow::array::{ArrayRef, BooleanArray, Int32Array};
    use std::sync::Arc;

    fn int_array(n: i32) -> ArrayRef {
        Arc::new(Int32Array::from_iter_values(0..n))
    }

    #[test]
    fn test_evict_policy_always_removes() {
        let policy = Evict;

        let arrow_entry = CacheEntry::memory_arrow(int_array(8));
        assert!(matches!(
            policy.squeeze(&arrow_entry),
            SqueezeOutcome::Remove
        ));

        let arr = int_array(8);
        let liquid = transcode_liquid_inner(&arr).unwrap();
        let liquid_entry = CacheEntry::memory_liquid(liquid);
        assert!(matches!(
            policy.squeeze(&liquid_entry),
            SqueezeOutcome::Remove
        ));
    }

    #[test]
    fn test_transcode_evict_policy() {
        let policy = TranscodeEvict;

        // MemoryArrow -> MemoryLiquid
        let arr = int_array(8);
        let outcome = policy.squeeze(&CacheEntry::memory_arrow(arr.clone()));
        match outcome {
            SqueezeOutcome::Replace(CacheEntry::MemoryLiquid(liq)) => {
                assert_eq!(liq.to_arrow_array().as_ref(), arr.as_ref());
            }
            other => panic!("unexpected: {other:?}"),
        }

        // MemoryLiquid -> Remove
        let liquid = transcode_liquid_inner(&arr).unwrap();
        let outcome = policy.squeeze(&CacheEntry::memory_liquid(liquid));
        assert!(matches!(outcome, SqueezeOutcome::Remove));
    }

    #[test]
    fn transcode_evict_untranscodable_removes() {
        let policy = TranscodeEvict;
        // Boolean arrays are not supported by the transcoder.
        let arr: ArrayRef = Arc::new(BooleanArray::from(vec![true, false, true]));
        let outcome = policy.squeeze(&CacheEntry::memory_arrow(arr));
        assert!(matches!(outcome, SqueezeOutcome::Remove));
    }
}
