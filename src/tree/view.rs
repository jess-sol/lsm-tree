// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use super::Tree;
use crate::{
    key::InternalKey, range::prefix_upper_range, version::SuperVersion, Memtable, SeqNo, UserValue,
    ValueType,
};
use std::ops::Bound;

/// The tree as a read at one seqno sees it, held.
///
/// A view holds the memtables and the tables it was taken over, so a read through
/// it takes no lock of the tree, and it answers the same while later writes, a
/// flush and a merge land beneath it. It keeps what it holds from being freed for
/// as long as it lives.
pub struct View {
    super_version: SuperVersion,
    seqno: SeqNo,
}

impl Tree {
    /// Holds the tree as a read at `seqno` sees it.
    #[must_use]
    pub fn view(&self, seqno: SeqNo) -> View {
        View {
            super_version: self.current_super_version(),
            seqno,
        }
    }
}

impl View {
    /// Reads the value of `key`.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn get(&self, key: &[u8]) -> crate::Result<Option<UserValue>> {
        Ok(
            Tree::get_internal_entry_from_version(&self.super_version, key, self.seqno)?
                .map(|entry| entry.value),
        )
    }

    /// Whether a key under `prefix` may lie in the tree.
    ///
    /// False only where no memtable holds such a key and every table rules the
    /// prefix out, by its key range or by its filter (see
    /// [`crate::Config::filter_prefix_len`]). A row above the view's seqno or a
    /// removal counts as a key.
    ///
    /// # Errors
    ///
    /// Will return `Err` if a filter block is not read.
    pub fn may_hold_prefix(&self, prefix: &[u8]) -> crate::Result<bool> {
        let version = &self.super_version;

        let in_memory = std::iter::once(&version.active_memtable)
            .chain(version.sealed_memtables.iter())
            .any(|memtable| holds_prefix(memtable, prefix));

        if in_memory {
            return Ok(true);
        }

        let end = if prefix.is_empty() {
            Bound::Unbounded
        } else {
            prefix_upper_range(prefix)
        };
        let bounds = (Bound::Included(prefix), end.as_ref().map(|end| &**end));

        for table in version.version.iter_tables() {
            if !table.rules_out_range(&bounds)? {
                return Ok(true);
            }
        }

        Ok(false)
    }
}

fn holds_prefix(memtable: &Memtable, prefix: &[u8]) -> bool {
    // A key's newest version sorts first, so this is before every key under `prefix`
    let first = InternalKey::new(prefix, SeqNo::MAX, ValueType::Value);

    memtable
        .range(first..)
        .next()
        .is_some_and(|item| item.key.user_key.starts_with(prefix))
}
