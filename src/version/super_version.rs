// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::{
    memtable::Memtable,
    tree::sealed::SealedMemtables,
    version::{persist_version, Version},
};
use std::{path::Path, sync::Arc};

/// A super version is a point-in-time snapshot of memtables and a [`Version`] (list of disk files)
#[derive(Clone)]
pub struct SuperVersion {
    /// Active memtable that is being written to
    #[doc(hidden)]
    pub active_memtable: Arc<Memtable>,

    /// Frozen memtables that are being flushed
    pub(crate) sealed_memtables: Arc<SealedMemtables>,

    /// Current tree version
    pub(crate) version: Version,
}

impl SuperVersion {
    pub fn new(version: Version) -> Self {
        Self {
            active_memtable: Arc::new(Memtable::new(0)),
            sealed_memtables: Arc::default(),
            version,
        }
    }

    pub fn memtable_size_sum(&self) -> u64 {
        self.active_memtable.size()
            + self
                .sealed_memtables
                .iter()
                .map(|sealed| sealed.size())
                .sum::<u64>()
    }

    /// Modifies the level manifest atomically.
    ///
    /// The function accepts a transition function that receives the current version
    /// and returns a new version.
    ///
    /// The function takes care of persisting the version changes on disk,
    /// and of removing the version file it replaces.
    pub(crate) fn upgrade_version<F: FnOnce(&Self) -> crate::Result<Self>>(
        &mut self,
        tree_path: &Path,
        f: F,
    ) -> crate::Result<()> {
        let next_version = f(self)?;

        persist_version(tree_path, &next_version.version)?;

        let replaced = std::mem::replace(self, next_version).version.id();

        // A file left here by a failure is removed when the tree is next opened
        if replaced != self.version.id() {
            let path = tree_path.join(format!("v{replaced}"));

            if let Err(e) = crate::file::retry_transient_io(|| std::fs::remove_file(&path)) {
                log::warn!("Removing version #{replaced} failed: {e:?}");
            }
        }

        Ok(())
    }
}
