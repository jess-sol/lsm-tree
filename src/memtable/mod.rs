// Copyright (c) 2024-present, fjall-rs
// This source code is licensed under both the Apache 2.0 and MIT License
// (found in the LICENSE-* files in the repository)

use crate::key::InternalKey;
use crate::{
    value::{InternalValue, SeqNo, UserValue},
    UserKey, ValueType,
};
use crossbeam_skiplist::SkipMap;
use std::ops::{Bound, RangeBounds};
use std::sync::atomic::{AtomicBool, AtomicU64};

pub use crate::tree::inner::MemtableId;

/// The rows of a memtable.
#[expect(
    clippy::large_enum_variant,
    reason = "a memtable holds one; a boxed skiplist would cost every insert a pointer"
)]
enum Items {
    /// Rows written one at a time, in a lock-free skiplist.
    Skiplist(SkipMap<InternalKey, UserValue>),

    /// The rows of one batch as they came, in key order, read by bisection.
    Sorted(Vec<(InternalKey, UserValue)>),
}

/// The rows of either form, as one iterator type.
enum Rows<S, V> {
    Skiplist(S),
    Sorted(V),
}

impl<S, V> Iterator for Rows<S, V>
where
    S: Iterator<Item = InternalValue>,
    V: Iterator<Item = InternalValue>,
{
    type Item = InternalValue;

    fn next(&mut self) -> Option<Self::Item> {
        match self {
            Self::Skiplist(rows) => rows.next(),
            Self::Sorted(rows) => rows.next(),
        }
    }
}

impl<S, V> DoubleEndedIterator for Rows<S, V>
where
    S: DoubleEndedIterator<Item = InternalValue>,
    V: DoubleEndedIterator<Item = InternalValue>,
{
    fn next_back(&mut self) -> Option<Self::Item> {
        match self {
            Self::Skiplist(rows) => rows.next_back(),
            Self::Sorted(rows) => rows.next_back(),
        }
    }
}

/// The memtable serves as an intermediary, ephemeral, sorted storage for new items
///
/// When the Memtable exceeds some size, it should be flushed to a table.
pub struct Memtable {
    #[doc(hidden)]
    pub id: MemtableId,

    /// The actual content.
    items: Items,

    /// Approximate active memtable size.
    ///
    /// If this grows too large, a flush is triggered.
    pub(crate) approximate_size: AtomicU64,

    /// Highest encountered sequence number.
    ///
    /// This is used so that `get_highest_seqno` has O(1) complexity.
    pub(crate) highest_seqno: AtomicU64,

    pub(crate) requested_rotation: AtomicBool,
}

impl Memtable {
    /// Returns the memtable ID.
    pub fn id(&self) -> MemtableId {
        self.id
    }

    /// Returns `true` if the memtable was already flagged for rotation.
    pub fn is_flagged_for_rotation(&self) -> bool {
        self.requested_rotation
            .load(std::sync::atomic::Ordering::Relaxed)
    }

    /// Flags the memtable as requested for rotation.
    pub fn flag_rotated(&self) {
        self.requested_rotation
            .store(true, std::sync::atomic::Ordering::Relaxed);
    }

    #[doc(hidden)]
    #[must_use]
    pub fn new(id: MemtableId) -> Self {
        Self {
            id,
            items: Items::Skiplist(SkipMap::default()),
            approximate_size: AtomicU64::default(),
            highest_seqno: AtomicU64::default(),
            requested_rotation: AtomicBool::default(),
        }
    }

    /// A memtable over the rows of one batch, all at `seqno`, that takes no further row.
    ///
    /// `rows` come in key order with no key twice. A row without a value is a removal.
    pub(crate) fn from_sorted(
        id: MemtableId,
        rows: Vec<(UserKey, Option<UserValue>)>,
        seqno: SeqNo,
    ) -> Self {
        debug_assert!(
            rows.is_sorted_by(|a, b| a.0 < b.0),
            "the rows of a batch come in key order, no key twice",
        );

        let mut size = 0;

        let items = rows
            .into_iter()
            .map(|(key, value)| {
                let item = match value {
                    Some(value) => {
                        InternalValue::from_components(key, value, seqno, ValueType::Value)
                    }
                    None => InternalValue::new_tombstone(key, seqno),
                };
                size += item_size(&item);
                (item.key, item.value)
            })
            .collect();

        Self {
            id,
            items: Items::Sorted(items),
            approximate_size: AtomicU64::new(size),
            highest_seqno: AtomicU64::new(seqno),
            requested_rotation: AtomicBool::default(),
        }
    }

    /// Creates an iterator over all items.
    pub fn iter(&self) -> impl DoubleEndedIterator<Item = InternalValue> + '_ {
        match &self.items {
            Items::Skiplist(items) => Rows::Skiplist(items.iter().map(|entry| InternalValue {
                key: entry.key().clone(),
                value: entry.value().clone(),
            })),
            Items::Sorted(items) => Rows::Sorted(items.iter().map(row)),
        }
    }

    /// Creates an iterator over a range of items.
    pub(crate) fn range<'a, R: RangeBounds<InternalKey> + 'a>(
        &'a self,
        range: R,
    ) -> impl DoubleEndedIterator<Item = InternalValue> + 'a {
        match &self.items {
            Items::Skiplist(items) => {
                Rows::Skiplist(items.range(range).map(|entry| InternalValue {
                    key: entry.key().clone(),
                    value: entry.value().clone(),
                }))
            }
            Items::Sorted(items) => {
                let start = match range.start_bound() {
                    Bound::Included(bound) => items.partition_point(|(key, _)| key < bound),
                    Bound::Excluded(bound) => items.partition_point(|(key, _)| key <= bound),
                    Bound::Unbounded => 0,
                };
                let end = match range.end_bound() {
                    Bound::Included(bound) => items.partition_point(|(key, _)| key <= bound),
                    Bound::Excluded(bound) => items.partition_point(|(key, _)| key < bound),
                    Bound::Unbounded => items.len(),
                };

                // A range whose end lies before its start holds nothing
                Rows::Sorted(items.get(start..end).unwrap_or_default().iter().map(row))
            }
        }
    }

    /// Returns the item by key if it exists.
    ///
    /// The item with the highest seqno will be returned, if `seqno` is None.
    #[doc(hidden)]
    pub fn get(&self, key: &[u8], seqno: SeqNo) -> Option<InternalValue> {
        if seqno == 0 {
            return None;
        }

        // NOTE: This range start deserves some explanation...
        // InternalKeys are multi-sorted by 2 categories: user_key and Reverse(seqno). (tombstone doesn't really matter)
        // We search for the lowest entry that is greater or equal the user's prefix key
        // and has the seqno (or lower) we want (because the seqno is stored in reverse order)
        //
        // Example: We search for "abc"
        //
        // key -> seqno
        //
        // a   -> 7
        // abc -> 5 <<< This is the lowest key (highest seqno) that matches the key with seqno=None
        // abc -> 4
        // abc -> 3 <<< If searching for abc and seqno=4, we would get this
        // abcdef -> 6
        // abcdef -> 5
        //
        let lower_bound = InternalKey::new(key, seqno - 1, ValueType::Value);

        match &self.items {
            Items::Skiplist(items) => {
                let mut iter = items
                    .range(lower_bound..)
                    .take_while(|entry| &*entry.key().user_key == key);

                iter.next().map(|entry| InternalValue {
                    key: entry.key().clone(),
                    value: entry.value().clone(),
                })
            }
            Items::Sorted(items) => items
                .get(items.partition_point(|(found, _)| found < &lower_bound))
                .filter(|(found, _)| &*found.user_key == key)
                .map(row),
        }
    }

    /// Gets approximate size of memtable in bytes.
    pub fn size(&self) -> u64 {
        self.approximate_size
            .load(std::sync::atomic::Ordering::Acquire)
    }

    /// Counts the number of items in the memtable.
    pub fn len(&self) -> usize {
        match &self.items {
            Items::Skiplist(items) => items.len(),
            Items::Sorted(items) => items.len(),
        }
    }

    /// Returns `true` if the memtable is empty.
    #[must_use]
    pub fn is_empty(&self) -> bool {
        self.len() == 0
    }

    /// Inserts an item into the memtable
    ///
    /// # Panics
    ///
    /// Panics if the memtable was made from the rows of a batch: it is sealed from birth.
    #[doc(hidden)]
    pub fn insert(&self, item: InternalValue) -> (u64, u64) {
        let Items::Skiplist(items) = &self.items else {
            panic!("a memtable over the rows of a batch takes no further row");
        };

        let item_size = item_size(&item);

        let size_before = self
            .approximate_size
            .fetch_add(item_size, std::sync::atomic::Ordering::AcqRel);

        let key = InternalKey::new(item.key.user_key, item.key.seqno, item.key.value_type);
        items.insert(key, item.value);

        self.highest_seqno
            .fetch_max(item.key.seqno, std::sync::atomic::Ordering::AcqRel);

        (item_size, size_before + item_size)
    }

    /// Returns the highest sequence number in the memtable.
    pub fn get_highest_seqno(&self) -> Option<SeqNo> {
        if self.is_empty() {
            None
        } else {
            Some(
                self.highest_seqno
                    .load(std::sync::atomic::Ordering::Acquire),
            )
        }
    }
}

/// The bytes a memtable counts for one row.
fn item_size(item: &InternalValue) -> u64 {
    #[expect(
        clippy::expect_used,
        reason = "keys are limited to 16-bit length + values are limited to 32-bit length"
    )]
    (item.key.user_key.len() + item.value.len() + std::mem::size_of::<InternalValue>())
        .try_into()
        .expect("should fit into u64")
}

fn row((key, value): &(InternalKey, UserValue)) -> InternalValue {
    InternalValue {
        key: key.clone(),
        value: value.clone(),
    }
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::ValueType;
    use test_log::test;

    #[test]
    #[expect(clippy::unwrap_used)]
    fn memtable_mvcc_point_read() {
        let memtable = Memtable::new(0);

        memtable.insert(InternalValue::from_components(
            *b"hello-key-999991",
            *b"hello-value-999991",
            0,
            ValueType::Value,
        ));

        let item = memtable.get(b"hello-key-99999", SeqNo::MAX);
        assert_eq!(None, item);

        let item = memtable.get(b"hello-key-999991", SeqNo::MAX);
        assert_eq!(*b"hello-value-999991", &*item.unwrap().value);

        memtable.insert(InternalValue::from_components(
            *b"hello-key-999991",
            *b"hello-value-999991-2",
            1,
            ValueType::Value,
        ));

        let item = memtable.get(b"hello-key-99999", SeqNo::MAX);
        assert_eq!(None, item);

        let item = memtable.get(b"hello-key-999991", SeqNo::MAX);
        assert_eq!((*b"hello-value-999991-2"), &*item.unwrap().value);

        let item = memtable.get(b"hello-key-99999", 1);
        assert_eq!(None, item);

        let item = memtable.get(b"hello-key-999991", 1);
        assert_eq!((*b"hello-value-999991"), &*item.unwrap().value);

        let item = memtable.get(b"hello-key-99999", 2);
        assert_eq!(None, item);

        let item = memtable.get(b"hello-key-999991", 2);
        assert_eq!((*b"hello-value-999991-2"), &*item.unwrap().value);
    }

    #[test]
    fn memtable_get() {
        let memtable = Memtable::new(0);

        let value =
            InternalValue::from_components(b"abc".to_vec(), b"abc".to_vec(), 0, ValueType::Value);

        memtable.insert(value.clone());

        assert_eq!(Some(value), memtable.get(b"abc", SeqNo::MAX));
    }

    #[test]
    fn memtable_get_highest_seqno() {
        let memtable = Memtable::new(0);

        memtable.insert(InternalValue::from_components(
            b"abc".to_vec(),
            b"abc".to_vec(),
            0,
            ValueType::Value,
        ));
        memtable.insert(InternalValue::from_components(
            b"abc".to_vec(),
            b"abc".to_vec(),
            1,
            ValueType::Value,
        ));
        memtable.insert(InternalValue::from_components(
            b"abc".to_vec(),
            b"abc".to_vec(),
            2,
            ValueType::Value,
        ));
        memtable.insert(InternalValue::from_components(
            b"abc".to_vec(),
            b"abc".to_vec(),
            3,
            ValueType::Value,
        ));
        memtable.insert(InternalValue::from_components(
            b"abc".to_vec(),
            b"abc".to_vec(),
            4,
            ValueType::Value,
        ));

        assert_eq!(
            Some(InternalValue::from_components(
                b"abc".to_vec(),
                b"abc".to_vec(),
                4,
                ValueType::Value,
            )),
            memtable.get(b"abc", SeqNo::MAX)
        );
    }

    #[test]
    fn memtable_get_prefix() {
        let memtable = Memtable::new(0);

        memtable.insert(InternalValue::from_components(
            b"abc0".to_vec(),
            b"abc".to_vec(),
            0,
            ValueType::Value,
        ));
        memtable.insert(InternalValue::from_components(
            b"abc".to_vec(),
            b"abc".to_vec(),
            255,
            ValueType::Value,
        ));

        assert_eq!(
            Some(InternalValue::from_components(
                b"abc".to_vec(),
                b"abc".to_vec(),
                255,
                ValueType::Value,
            )),
            memtable.get(b"abc", SeqNo::MAX)
        );

        assert_eq!(
            Some(InternalValue::from_components(
                b"abc0".to_vec(),
                b"abc".to_vec(),
                0,
                ValueType::Value,
            )),
            memtable.get(b"abc0", SeqNo::MAX)
        );
    }

    #[test]
    fn memtable_get_old_version() {
        let memtable = Memtable::new(0);

        memtable.insert(InternalValue::from_components(
            b"abc".to_vec(),
            b"abc".to_vec(),
            0,
            ValueType::Value,
        ));
        memtable.insert(InternalValue::from_components(
            b"abc".to_vec(),
            b"abc".to_vec(),
            99,
            ValueType::Value,
        ));
        memtable.insert(InternalValue::from_components(
            b"abc".to_vec(),
            b"abc".to_vec(),
            255,
            ValueType::Value,
        ));

        assert_eq!(
            Some(InternalValue::from_components(
                b"abc".to_vec(),
                b"abc".to_vec(),
                255,
                ValueType::Value,
            )),
            memtable.get(b"abc", SeqNo::MAX)
        );

        assert_eq!(
            Some(InternalValue::from_components(
                b"abc".to_vec(),
                b"abc".to_vec(),
                99,
                ValueType::Value,
            )),
            memtable.get(b"abc", 100)
        );

        assert_eq!(
            Some(InternalValue::from_components(
                b"abc".to_vec(),
                b"abc".to_vec(),
                0,
                ValueType::Value,
            )),
            memtable.get(b"abc", 50)
        );
    }

    #[test]
    fn a_sorted_run_answers_as_a_skiplist_of_the_same_rows() {
        use std::ops::Bound::{Excluded, Included, Unbounded};

        let rows: Vec<(UserKey, Option<UserValue>)> = vec![
            (b"a".into(), Some(b"1".into())),
            (b"abc".into(), None),
            (b"abd".into(), Some(b"3".into())),
            (b"b".into(), Some(b"4".into())),
        ];

        let skiplist = Memtable::new(0);
        for (key, value) in &rows {
            skiplist.insert(match value {
                Some(value) => {
                    InternalValue::from_components(key.clone(), value.clone(), 7, ValueType::Value)
                }
                None => InternalValue::new_tombstone(key.clone(), 7),
            });
        }
        let sorted = Memtable::from_sorted(1, rows, 7);

        assert_eq!(skiplist.len(), sorted.len());
        assert_eq!(skiplist.size(), sorted.size());
        assert_eq!(skiplist.get_highest_seqno(), sorted.get_highest_seqno());
        assert!(skiplist.iter().eq(sorted.iter()));
        assert!(skiplist.iter().rev().eq(sorted.iter().rev()));

        for key in [&b"a"[..], b"ab", b"abc", b"abd", b"b", b"c"] {
            for seqno in [0, 7, 8, SeqNo::MAX] {
                assert_eq!(
                    skiplist.get(key, seqno),
                    sorted.get(key, seqno),
                    "{key:?} at {seqno}",
                );
            }
        }

        // A key's newest version sorts first, so `first` is before all of a key and `last` after
        let first = |key: &[u8]| InternalKey::new(key, SeqNo::MAX, ValueType::Value);
        let last = |key: &[u8]| InternalKey::new(key, 0, ValueType::Value);

        let ranges = [
            (Unbounded, Unbounded),
            (Included(first(b"abc")), Unbounded),
            (Excluded(last(b"abc")), Unbounded),
            (Unbounded, Included(last(b"abd"))),
            (Unbounded, Excluded(first(b"abd"))),
            (Included(first(b"abc")), Included(last(b"abc"))),
            (Included(first(b"c")), Unbounded),
        ];

        for range in ranges {
            assert_eq!(
                skiplist.range(range.clone()).collect::<Vec<_>>(),
                sorted.range(range.clone()).collect::<Vec<_>>(),
                "{range:?}",
            );
        }
    }
}
