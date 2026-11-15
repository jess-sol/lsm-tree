// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use super::{
    flavour::prepare_table_writer, stream::CompactionStream, worker::CompactionReader, Input,
    Tiered,
};
use crate::{merge::Merger, AbstractTree, HashSet, SeqNo, Table, TableId, Tree, UserKey};
use std::{ops::Bound, sync::Arc};

type Range = (Bound<UserKey>, Bound<UserKey>);

/// What a merge's job, its parts and their results hold in common.
struct Job {
    tree: Tree,

    payload: Input,

    /// The tables the merge reads, by first key
    tables: Vec<Table>,

    seqno_threshold: SeqNo,

    /// No table outside the merge lies at its level or deeper, so a removal has
    /// nothing left to hide and is dropped
    nothing_beneath: bool,

    level_count: usize,
}

/// One merge, whose parts the caller runs on its own threads.
///
/// The tables it reads are no other merge's until it is installed or dropped.
/// Dropped uninstalled, it gives them back.
pub struct Compaction {
    job: Arc<Job>,
    parts: usize,
    installed: bool,
}

/// The rows of a merge between two first keys of its tables, about one output
/// table of them.
pub struct Part {
    job: Arc<Job>,
    index: usize,
    range: Range,
}

/// The tables one part wrote. Dropped uninstalled, it removes them.
pub struct Merged {
    job: Arc<Job>,
    index: usize,
    tables: Vec<Table>,
    installed: bool,
}

impl Tree {
    /// Chooses one merge and hands out its parts, or `None` where `strategy`
    /// chooses none.
    ///
    /// Every part is merged, on any thread, and the results are handed to
    /// [`Compaction::install`]. `seqno_threshold` is the seqno at or below which
    /// only the newest version of a key is kept.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    ///
    /// # Panics
    ///
    /// Panics if the tree has a compaction filter, which a merge handed out in
    /// parts does not run, or if a lock of the tree is poisoned.
    pub fn compaction(
        &self,
        strategy: &Tiered,
        seqno_threshold: SeqNo,
    ) -> crate::Result<Option<(Compaction, Vec<Part>)>> {
        assert!(
            self.config.compaction_filter_factory.is_none(),
            "a merge handed out in parts runs no compaction filter",
        );

        #[expect(clippy::expect_used, reason = "lock is expected to not be poisoned")]
        let mut state = self.compaction_state.lock().expect("lock is poisoned");
        let version = self.current_version();

        let Some(payload) = strategy.choose(&version, state.hidden_set()) else {
            return Ok(None);
        };

        let reads = |table: &&Table| payload.table_ids.contains(&table.id());

        let mut tables: Vec<Table> = version.iter_tables().filter(reads).cloned().collect();
        tables.sort_by(|a, b| a.metadata.key_range.min().cmp(b.metadata.key_range.min()));

        let level_of_merge = version.iter_levels().position(|level| {
            level
                .iter()
                .flat_map(|run| run.iter())
                .any(|table| reads(&table))
        });

        let nothing_beneath = level_of_merge.is_some_and(|level| {
            version
                .iter_levels()
                .skip(level)
                .flat_map(|level| level.iter())
                .flat_map(|run| run.iter())
                .all(|table| reads(&table))
        });

        state
            .hidden_set_mut()
            .hide(payload.table_ids.iter().copied());
        drop(state);

        let ranges = cut(&tables, payload.target_size);

        let job = Arc::new(Job {
            tree: self.clone(),
            payload,
            tables,
            seqno_threshold,
            nothing_beneath,
            level_count: version.level_count(),
        });

        let parts: Vec<Part> = ranges
            .into_iter()
            .enumerate()
            .map(|(index, range)| Part {
                job: job.clone(),
                index,
                range,
            })
            .collect();

        let compaction = Compaction {
            job,
            parts: parts.len(),
            installed: false,
        };

        Ok(Some((compaction, parts)))
    }
}

impl Part {
    /// Merges the part's rows into tables of their own, on the calling thread.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    pub fn merge(self) -> crate::Result<Merged> {
        let Self { job, index, range } = self;
        let config = &job.tree.config;

        let bounds = (
            range.0.as_ref().map(|key| &**key),
            range.1.as_ref().map(|key| &**key),
        );

        let readers: Vec<CompactionReader<'_>> = job
            .tables
            .iter()
            .filter(|table| table.check_key_range_overlap(&bounds))
            .map(|table| Box::new(table.range(range.clone())) as CompactionReader<'_>)
            .collect();

        let stream = CompactionStream::new(Merger::new(readers), job.seqno_threshold)
            .evict_tombstones(job.nothing_beneath)
            .zero_seqnos(false);

        let mut writer = prepare_table_writer(
            job.level_count,
            config,
            &job.tree.table_id_counter,
            &job.payload,
        )?;

        for item in stream {
            writer.write(item?)?;
        }

        let level = usize::from(job.payload.canonical_level);
        let pin_filter = config.filter_block_pinning_policy.get(level);
        let pin_index = config.index_block_pinning_policy.get(level);
        let folder = writer.base_path.clone();

        let tables = writer
            .finish()?
            .into_iter()
            .map(|(table_id, checksum)| {
                Table::recover(
                    folder.join(table_id.to_string()),
                    checksum,
                    0,
                    job.tree.id,
                    config.cache.clone(),
                    config.descriptor_table.clone(),
                    pin_filter,
                    pin_index,
                    #[cfg(feature = "metrics")]
                    job.tree.metrics.clone(),
                )
            })
            .collect::<crate::Result<Vec<_>>>()?;

        Ok(Merged {
            job,
            index,
            tables,
            installed: false,
        })
    }
}

impl Compaction {
    /// Puts the tables the parts wrote in the place of the tables the merge read,
    /// as one change of the tree.
    ///
    /// `merged` holds the result of every part, in any order.
    ///
    /// # Errors
    ///
    /// Will return `Err` if an IO error occurs.
    ///
    /// # Panics
    ///
    /// Panics if `merged` lacks a part's result, holds one twice or holds another
    /// job's, or if a lock of the tree is poisoned.
    pub fn install(mut self, mut merged: Vec<Merged>) -> crate::Result<()> {
        assert!(
            merged.iter().all(|part| Arc::ptr_eq(&part.job, &self.job)),
            "a job installs the results of its own parts",
        );

        merged.sort_by_key(|part| part.index);

        assert!(
            merged.len() == self.parts
                && merged.iter().enumerate().all(|(i, part)| part.index == i),
            "a job installs the result of every one of its parts, once",
        );

        let job = self.job.clone();
        let tree = &job.tree;

        #[expect(clippy::expect_used, reason = "lock is expected to not be poisoned")]
        let mut state = tree.compaction_state.lock().expect("lock is poisoned");
        #[expect(clippy::expect_used, reason = "lock is expected to not be poisoned")]
        let mut super_version = tree.super_version.write().expect("lock is poisoned");

        let read: Vec<TableId> = job.payload.table_ids.iter().copied().collect();

        // The tables may be gone since the merge began: a `clear` drops every
        // table. What the parts wrote would bring cleared rows back, so it is
        // left to be removed
        if read
            .iter()
            .any(|&id| super_version.version.get_table(id).is_none())
        {
            drop(super_version);
            drop(state);
            return Ok(());
        }

        let written: Vec<Table> = merged
            .iter()
            .flat_map(|part| part.tables.iter().cloned())
            .collect();

        super_version.upgrade_version(&tree.config.path, |current| {
            let mut copy = current.clone();

            copy.version = copy.version.with_merge(
                &read,
                &written,
                usize::from(job.payload.dest_level),
                None,
                vec![],
                &HashSet::default(),
            );

            Ok(copy)
        })?;

        for table in &job.tables {
            table.mark_as_deleted();
        }
        for part in &mut merged {
            part.installed = true;
        }

        state.hidden_set_mut().show(read);
        self.installed = true;

        drop(super_version);
        drop(state);

        Ok(())
    }
}

impl Drop for Compaction {
    fn drop(&mut self) {
        if self.installed {
            return;
        }

        // A poisoned lock means a merge panicked while it held the tables; they
        // stay reserved, which keeps a broken tree from merging further
        if let Ok(mut state) = self.job.tree.compaction_state.lock() {
            state
                .hidden_set_mut()
                .show(self.job.payload.table_ids.iter().copied());
        }
    }
}

impl Drop for Merged {
    fn drop(&mut self) {
        if !self.installed {
            for table in &self.tables {
                table.mark_as_deleted();
            }
        }
    }
}

/// Cuts the key space at first keys of `tables`, which come by first key, so that
/// each range holds about `target_size` bytes of them.
fn cut(tables: &[Table], target_size: u64) -> Vec<Range> {
    let first_key = |table: &Table| table.metadata.key_range.min().clone();
    let lowest = tables.first().map(first_key);

    let mut cuts: Vec<UserKey> = vec![];
    let mut bytes = 0;

    for table in tables {
        let key = first_key(table);

        // A cut at the lowest key would leave a range with no row before it
        if bytes >= target_size.max(1) && cuts.last() != Some(&key) && lowest.as_ref() != Some(&key)
        {
            cuts.push(key);
            bytes = 0;
        }

        bytes += table.metadata.file_size;
    }

    let starts = std::iter::once(Bound::Unbounded).chain(cuts.iter().cloned().map(Bound::Included));
    let ends = cuts
        .iter()
        .cloned()
        .map(Bound::Excluded)
        .chain(std::iter::once(Bound::Unbounded));

    starts.zip(ends).collect()
}
