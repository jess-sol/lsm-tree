use lsm_tree::{get_tmp_folder, AbstractTree, Config, SequenceNumberCounter, Tree, View};
use test_log::test;

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

#[test]
fn a_view_answers_the_same_while_later_writes_and_a_flush_land() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = open(&folder)?;

    tree.insert("kept", "old", 1);
    tree.insert("removed", "old", 1);
    tree.insert("flushed", "old", 1);
    tree.flush_active_memtable(0)?;
    tree.insert("kept", "seen", 3);

    let view = tree.view(4);
    let read = |view: &View, key: &str| {
        view.get(key.as_bytes())
            .map(|value| value.map(|value| value.to_vec()))
    };
    let seen = |view: &View| -> lsm_tree::Result<_> {
        Ok([
            read(view, "kept")?,
            read(view, "removed")?,
            read(view, "flushed")?,
            read(view, "absent")?,
        ])
    };

    let before = seen(&view)?;
    assert_eq!(
        [
            Some(b"seen".to_vec()),
            Some(b"old".to_vec()),
            Some(b"old".to_vec()),
            None
        ],
        before,
    );

    tree.insert("kept", "later", 5);
    tree.remove("removed", 6);
    tree.insert("absent", "later", 7);
    tree.flush_active_memtable(0)?;

    // A merge that collects below a seqno above the view's drops what the view reads
    tree.major_compact(u64::MAX, 10)?;
    assert_eq!(
        None,
        tree.get("kept", 4)?,
        "a read that begins after the merge finds the rows gone",
    );

    assert_eq!(before, seen(&view)?);
    assert_eq!(
        Some(b"later".to_vec()),
        read(&tree.view(8), "kept")?,
        "a view taken later reads the later rows",
    );

    Ok(())
}

#[test]
fn a_view_crosses_threads() {
    fn crosses_threads<T: Send + Sync>() {}
    crosses_threads::<View>();
}
