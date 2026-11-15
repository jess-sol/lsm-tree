// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use super::Tree;
use crate::{AbstractTree, InternalValue, Memtable, SeqNo, UserKey, UserValue, ValueType};
use std::sync::Arc;

/// A batch of this many rows is sealed as a memtable of its own.
///
/// Below it, a memtable for each batch would cost every point read a bisection
/// more than the rows are worth.
const SEALED_BATCH_ROWS: usize = 8_192;

impl Tree {
    /// Writes the rows of a batch at one `seqno`, read all at once.
    ///
    /// `rows` come in key order with no key twice, and a row without a value is a
    /// removal. `seqno` is at or above every seqno the tree holds. Both are the
    /// duty of the caller, asserted in debug builds only.
    ///
    /// Many rows become a sealed memtable of their own, read by bisection; few go
    /// into the active memtable. A rotation never splits a batch. One thread
    /// writes a tree.
    ///
    /// Returns the bytes the rows add, counted as [`AbstractTree::insert`] counts them.
    ///
    /// # Panics
    ///
    /// Panics if a lock of the tree is poisoned.
    #[must_use = "the caller counts what its trees hold in memory by this"]
    pub fn write_sorted(&self, rows: Vec<(UserKey, Option<UserValue>)>, seqno: SeqNo) -> u64 {
        if rows.is_empty() {
            return 0;
        }

        debug_assert!(
            self.get_highest_seqno().is_none_or(|held| held <= seqno),
            "a batch is written at or above every seqno its tree holds",
        );

        if rows.len() < SEALED_BATCH_ROWS {
            debug_assert!(
                rows.is_sorted_by(|a, b| a.0 < b.0),
                "the rows of a batch come in key order, no key twice",
            );

            // A rotation takes the write lock, so it waits for the whole batch
            #[expect(clippy::expect_used, reason = "lock is expected to not be poisoned")]
            let super_version = self.super_version.read().expect("lock is poisoned");

            return rows
                .into_iter()
                .map(|(key, value)| {
                    let item = match value {
                        Some(value) => {
                            InternalValue::from_components(key, value, seqno, ValueType::Value)
                        }
                        None => InternalValue::new_tombstone(key, seqno),
                    };
                    super_version.active_memtable.insert(item).0
                })
                .sum();
        }

        let batch = Arc::new(Memtable::from_sorted(
            self.memtable_id_counter.next(),
            rows,
            seqno,
        ));
        let bytes = batch.size();

        #[expect(clippy::expect_used, reason = "lock is expected to not be poisoned")]
        let mut super_version = self.super_version.write().expect("lock is poisoned");

        // A point read takes its first hit from the newest memtable down, so the
        // rows written before the batch are sealed beneath it
        if !super_version.active_memtable.is_empty() {
            let older = std::mem::replace(
                &mut super_version.active_memtable,
                Arc::new(Memtable::new(self.memtable_id_counter.next())),
            );
            super_version.sealed_memtables = Arc::new(super_version.sealed_memtables.add(older));
        }

        super_version.sealed_memtables = Arc::new(super_version.sealed_memtables.add(batch));

        bytes
    }
}
