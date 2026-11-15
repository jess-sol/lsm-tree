use lsm_tree::{
    get_tmp_folder, AbstractTree, Config, Guard, KvPair, SeqNo, SequenceNumberCounter, Tree,
    UserKey, UserValue,
};
use test_log::test;

type Row = (UserKey, Option<UserValue>);

// --- Harness ---

fn open(folder: &tempfile::TempDir) -> lsm_tree::Result<Tree> {
    let tree = Config::new(
        folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    match tree {
        lsm_tree::AnyTree::Standard(tree) => Ok(tree),
        lsm_tree::AnyTree::Blob(_) => unreachable!("a tree without key-value separation"),
    }
}

fn key(i: usize) -> UserKey {
    format!("key{i:06}").as_bytes().into()
}

/// `count` rows in key order, every seventh a removal.
fn batch(count: usize, value: &str) -> Vec<Row> {
    (0..count)
        .map(|i| {
            let value = (i % 7 != 0).then(|| format!("{value}{i}").as_bytes().into());
            (key(i), value)
        })
        .collect()
}

fn write_one_by_one(tree: &Tree, rows: &[Row], seqno: SeqNo) {
    for (key, value) in rows {
        match value {
            Some(value) => tree.insert(key.clone(), value.clone(), seqno),
            None => tree.remove(key.clone(), seqno),
        };
    }
}

fn scan(tree: &Tree, seqno: SeqNo) -> lsm_tree::Result<Vec<KvPair>> {
    tree.iter(seqno, None).map(Guard::into_inner).collect()
}

fn assert_read_alike(expected: &Tree, tree: &Tree, keys: usize) -> lsm_tree::Result<()> {
    for seqno in [1, 2, 5, 6] {
        assert_eq!(
            scan(expected, seqno)?,
            scan(tree, seqno)?,
            "scan at {seqno}"
        );

        for i in (0..keys).step_by(keys.div_ceil(64)) {
            assert_eq!(
                expected.get(key(i), seqno)?,
                tree.get(key(i), seqno)?,
                "key {i} at {seqno}",
            );
        }
    }

    Ok(())
}

// --- Tests ---

#[test]
fn a_sorted_batch_reads_as_its_rows_written_one_by_one() -> lsm_tree::Result<()> {
    for count in [3, 20_000] {
        let (one_by_one_folder, sorted_folder) = (get_tmp_folder(), get_tmp_folder());
        let (one_by_one, sorted) = (open(&one_by_one_folder)?, open(&sorted_folder)?);

        // Older rows stand in the active memtable when the batch arrives
        let older: Vec<Row> = (0..count)
            .map(|i| (key(i), Some("old".as_bytes().into())))
            .collect();
        write_one_by_one(&one_by_one, &older, 1);
        write_one_by_one(&sorted, &older, 1);

        let rows = batch(count, "new");
        write_one_by_one(&one_by_one, &rows, 5);
        let _ = sorted.write_sorted(rows, 5);

        assert_read_alike(&one_by_one, &sorted, count)?;

        one_by_one.flush_active_memtable(0)?;
        sorted.flush_active_memtable(0)?;

        assert_read_alike(&one_by_one, &sorted, count)?;
    }

    Ok(())
}

#[test]
fn a_write_returns_the_bytes_a_flush_returns() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = open(&folder)?;

    assert_eq!(0, tree.write_sorted(vec![], 1));
    assert!(tree.is_empty(SeqNo::MAX, None)?);

    let written = tree.write_sorted(batch(3, "a"), 1)
        + tree.write_sorted(batch(20_000, "b"), 2)
        + tree.write_sorted(batch(5, "c"), 3);

    let lock = tree.get_flush_lock();
    tree.rotate_memtable();
    let flushed = tree.flush(&lock, 0)?;

    assert_eq!(Some(written), flushed);

    Ok(())
}

#[test]
fn a_rotation_never_splits_a_batch() -> lsm_tree::Result<()> {
    const BATCHES: usize = 3_000;
    const ROWS: usize = 4;

    let folder = get_tmp_folder();

    {
        let tree = open(&folder)?;
        let done = std::sync::atomic::AtomicBool::new(false);

        std::thread::scope(|scope| -> lsm_tree::Result<()> {
            let writer = scope.spawn(|| {
                for batch in 0..BATCHES {
                    let rows = (0..ROWS)
                        .map(|row| (key(batch * ROWS + row), Some("v".as_bytes().into())))
                        .collect();
                    let _ = tree.write_sorted(rows, batch as SeqNo + 1);
                }
                done.store(true, std::sync::atomic::Ordering::Release);
            });

            while !done.load(std::sync::atomic::Ordering::Acquire) {
                tree.flush_active_memtable(0)?;
            }

            writer.join().expect("the writer does not panic");
            Ok(())
        })?;
    }

    // What no flush reached is gone with the tree, so the disk shows where each rotation cut
    let tree = open(&folder)?;

    for batch in 0..BATCHES {
        let held = (0..ROWS)
            .filter_map(|row| tree.get(key(batch * ROWS + row), SeqNo::MAX).transpose())
            .count();
        assert!(held == 0 || held == ROWS, "batch {batch} holds {held} rows");
    }

    Ok(())
}

#[test]
#[cfg(debug_assertions)]
fn a_batch_that_breaks_a_duty_of_its_caller_panics_in_a_debug_build() -> lsm_tree::Result<()> {
    let row = |i| (key(i), Some("v".as_bytes().into()));

    let broken: [(&str, Vec<Row>, SeqNo); 3] = [
        ("rows out of key order", vec![row(2), row(1)], 9),
        ("a key twice", vec![row(1), row(1)], 9),
        ("a seqno below the tree's", vec![row(3)], 4),
    ];

    for (duty, rows, seqno) in broken {
        let folder = get_tmp_folder();
        let tree = open(&folder)?;
        let _ = tree.write_sorted(vec![row(0)], 5);

        let written = std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| {
            tree.write_sorted(rows, seqno)
        }));
        assert!(written.is_err(), "{duty}");
    }

    Ok(())
}

// --- Dependency contract ---

#[test]
fn a_flush_is_durable_when_it_returns() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let rows = batch(20_000, "v");

    {
        let tree = open(&folder)?;
        let _ = tree.write_sorted(rows.clone(), 1);
        tree.flush_active_memtable(0)?;
    }

    let tree = open(&folder)?;

    let expected: Vec<KvPair> = rows
        .into_iter()
        .filter_map(|(key, value)| Some((key, value?)))
        .collect();
    assert_eq!(expected, scan(&tree, SeqNo::MAX)?);

    Ok(())
}

#[test]
fn a_flush_keeps_every_version_above_its_threshold_and_the_newest_below() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = open(&folder)?;

    for seqno in 1..=4 {
        let value = format!("v{seqno}").as_bytes().into();
        let _ = tree.write_sorted(vec![(key(0), Some(value))], seqno);
    }

    tree.flush_active_memtable(2)?;

    let read = |seqno| {
        tree.get(key(0), seqno)
            .map(|value| value.map(|v| v.to_vec()))
    };

    assert_eq!(Some(b"v4".to_vec()), read(5)?);
    assert_eq!(Some(b"v3".to_vec()), read(4)?);
    assert_eq!(Some(b"v2".to_vec()), read(3)?);
    assert_eq!(None, read(2)?);

    Ok(())
}
