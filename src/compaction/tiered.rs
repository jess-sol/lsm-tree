// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use super::{state::hidden_set::HiddenSet, Input};
use crate::version::Version;

/// Tiered compaction: a level that holds `fan_in` runs is merged whole into the next.
///
/// Every level holds up to `fan_in` runs, and a merge writes one run. So a row is
/// rewritten once for each level it passes, where leveled compaction rewrites it
/// about `fan_in` times; a read pays for it with more runs to look in.
///
/// It is given to [`crate::Tree::compaction`] alone and is not a
/// [`super::CompactionStrategy`]: a level here holds several runs, and the whole
/// merge of a strategy drops tombstones wherever it writes to the last level,
/// which is wrong where that level holds another run.
#[derive(Clone, Debug)]
pub struct Tiered {
    fan_in: usize,
    target_size: u64,
}

impl Tiered {
    /// A level that holds `fan_in` runs is merged whole into the next, into tables
    /// of about `target_size` bytes. The last level merges into itself.
    ///
    /// # Panics
    ///
    /// Panics if `fan_in` is less than 2.
    #[must_use]
    pub fn new(fan_in: usize, target_size: u64) -> Self {
        assert!(fan_in >= 2, "a merge takes at least two runs");

        Self {
            fan_in,
            target_size,
        }
    }

    /// The first level from the top that holds `fan_in` runs, whole.
    ///
    /// A level that holds a table of another merge is passed over, since its runs
    /// are about to change.
    pub(super) fn choose(&self, version: &Version, hidden: &HiddenSet) -> Option<Input> {
        let last_level = version.level_count().checked_sub(1)?;

        let (level_idx, level) = version.iter_levels().enumerate().find(|(_, level)| {
            level.run_count() >= self.fan_in && !hidden.is_blocked(level.list_ids())
        })?;

        #[expect(
            clippy::cast_possible_truncation,
            reason = "a tree holds far fewer than 256 levels"
        )]
        let dest_level = (level_idx + 1).min(last_level) as u8;

        Some(Input {
            table_ids: level.list_ids(),
            dest_level,
            canonical_level: dest_level,
            target_size: self.target_size,
        })
    }
}
