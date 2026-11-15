use lsm_tree::{
    compaction::{Compaction, Merged, Part, Tiered},
    get_tmp_folder, AbstractTree, Config, Guard, KvPair, SeqNo, SequenceNumberCounter, Tree,
    UserKey,
};
use test_log::test;

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

/// Writes `rows` keys from `first` on at `seqno` and flushes them as one run.
///
/// Runs whose keys lie apart are joined into one run, so the runs of a test overlap.
fn flush_run(tree: &Tree, first: usize, rows: usize, seqno: SeqNo) -> lsm_tree::Result<()> {
    for i in first..first + rows {
        tree.insert(key(i), format!("v{seqno}"), seqno);
    }
    tree.flush_active_memtable(0)
}

fn scan(tree: &Tree) -> lsm_tree::Result<Vec<KvPair>> {
    tree.iter(SeqNo::MAX, None).map(Guard::into_inner).collect()
}

/// Runs every part of a job on a thread of its own.
fn merge(parts: Vec<Part>) -> lsm_tree::Result<Vec<Merged>> {
    std::thread::scope(|scope| {
        let threads: Vec<_> = parts
            .into_iter()
            .map(|part| scope.spawn(|| part.merge()))
            .collect();

        threads
            .into_iter()
            .map(|thread| thread.join().expect("a part does not panic"))
            .collect()
    })
}

/// Runs the merges `tiered` chooses until it chooses none.
fn settle(tree: &Tree, tiered: &Tiered, threshold: SeqNo) -> lsm_tree::Result<()> {
    while let Some((job, parts)) = tree.compaction(tiered, threshold)? {
        job.install(merge(parts)?)?;
    }
    Ok(())
}

fn table_files(folder: &tempfile::TempDir) -> lsm_tree::Result<usize> {
    Ok(std::fs::read_dir(folder.path().join("tables"))?.count())
}

// --- Tests ---

#[test]
fn a_level_of_fan_in_runs_merges_whole_into_the_next() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = open(&folder)?;
    let tiered = Tiered::new(4, 1_024);

    for run in 0..3 {
        flush_run(&tree, run * 50, 300, run as SeqNo + 1)?;
        assert!(
            tree.compaction(&tiered, 0)?.is_none(),
            "{} runs are fewer than the fan-in",
            run + 1,
        );
    }
    flush_run(&tree, 150, 300, 4)?;
    let before = scan(&tree)?;

    let (job, parts) = tree
        .compaction(&tiered, 0)?
        .expect("the level holds four runs");
    assert!(
        parts.len() > 1,
        "four runs that begin apart give several parts"
    );
    assert!(
        tree.compaction(&tiered, 0)?.is_none(),
        "the job's tables are no other job's",
    );

    job.install(merge(parts)?)?;

    assert_eq!(Some(0), tree.level_table_count(0));
    assert!(tree.level_table_count(1) > Some(0));
    assert_eq!(450, before.len());
    assert_eq!(before, scan(&tree)?);

    Ok(())
}

#[test]
fn a_merge_keeps_every_version_above_its_threshold_and_the_newest_below() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = open(&folder)?;

    for seqno in 1..=4 {
        tree.insert(key(0), format!("v{seqno}"), seqno);
        tree.flush_active_memtable(0)?;
    }

    settle(&tree, &Tiered::new(4, 1_024), 2)?;
    assert_eq!(Some(0), tree.level_table_count(0), "the four runs merged");

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

#[test]
fn a_merge_drops_a_tombstone_only_where_nothing_lies_beneath_it() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = open(&folder)?;
    let tiered = Tiered::new(2, 64 * 1_024);
    let last_level = 6;

    // One more run in the first level, and every merge that follows from it. The
    // threshold lies above every seqno, so a merge keeps a removal for the sake of
    // what lies beneath it alone
    let step = |n: SeqNo| -> lsm_tree::Result<()> {
        flush_run(&tree, 0, 1, n)?;
        settle(&tree, &tiered, 2_000)
    };
    let mut steps = 0;

    // An old row, carried down to the last level
    tree.insert("removed", "old", 0);
    while tree.level_table_count(last_level) == Some(0) {
        steps += 1;
        step(steps)?;
        assert!(steps < 200, "a fan-in of two reaches the last level");
    }

    // Its removal comes down after it, into a level that holds the old row in another run
    tree.remove("removed", 1_000);
    steps += 1;
    step(steps)?;
    assert!(tree.tombstone_count() > 0);

    while tree.tombstone_count() > 0 {
        steps += 1;
        step(steps)?;
        assert_eq!(None, tree.get("removed", SeqNo::MAX)?, "at step {steps}");
        assert!(steps < 500, "the last level merges whole in the end");
    }

    // The last level merged whole: nothing lay beneath, and the removal went with the row
    assert_eq!(None, tree.get("removed", SeqNo::MAX)?);

    Ok(())
}

#[test]
fn a_job_dropped_uninstalled_leaves_the_tree_as_it_was() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = open(&folder)?;
    let tiered = Tiered::new(4, 1_024);

    for run in 0..4 {
        flush_run(&tree, run * 50, 300, run as SeqNo + 1)?;
    }
    let (before, files) = (scan(&tree)?, table_files(&folder)?);

    {
        let (job, mut parts) = tree
            .compaction(&tiered, 0)?
            .expect("the level holds four runs");
        let merged = parts.remove(0).merge()?;
        assert!(table_files(&folder)? > files, "the part wrote its tables");
        drop((job, parts, merged));
    }

    assert_eq!(
        files,
        table_files(&folder)?,
        "what the part wrote is removed"
    );
    assert_eq!(before, scan(&tree)?);
    assert!(
        tree.compaction(&tiered, 0)?.is_some(),
        "the job gave its tables back",
    );

    Ok(())
}

#[test]
fn an_install_that_lacks_a_part_panics() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = open(&folder)?;
    let tiered = Tiered::new(4, 1_024);

    for run in 0..4 {
        flush_run(&tree, run * 50, 300, run as SeqNo + 1)?;
    }

    let (job, mut parts) = tree
        .compaction(&tiered, 0)?
        .expect("the level holds four runs");
    assert!(parts.len() > 1);
    let merged = parts.remove(0).merge()?;

    let installed =
        std::panic::catch_unwind(std::panic::AssertUnwindSafe(|| job.install(vec![merged])));
    assert!(installed.is_err());

    Ok(())
}

#[test]
fn a_merge_crosses_threads() {
    fn sent<T: Send>() {}
    fn sent_whole<T: Send + 'static>() {}

    sent::<Compaction>();
    sent_whole::<Part>();
    sent::<Merged>();
}
