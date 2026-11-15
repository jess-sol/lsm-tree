use lsm_tree::{get_tmp_folder, AbstractTree, Config, SequenceNumberCounter};
use test_log::test;

#[test]
fn a_read_at_a_seqno_answers_the_same_before_and_after_a_table_install() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    for key in ["a", "b", "c"] {
        tree.insert(key, key, 0);
        tree.flush_active_memtable(0)?;
    }
    tree.insert("d", "d", 0);

    for key in ["a", "b", "c", "d"] {
        assert_eq!(key.as_bytes(), &*tree.get(key, 1)?.unwrap());
    }

    tree.major_compact(u64::MAX, 0)?;

    for key in ["a", "b", "c", "d"] {
        assert_eq!(key.as_bytes(), &*tree.get(key, 1)?.unwrap());
    }

    Ok(())
}

#[test]
fn a_flush_and_a_merge_draw_no_number() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let seqno = SequenceNumberCounter::default();
    let visible_seqno = SequenceNumberCounter::default();

    {
        let tree = Config::new(&folder, seqno.clone(), visible_seqno.clone()).open()?;

        tree.insert("a", "a", 7);
        tree.flush_active_memtable(0)?;
        tree.insert("b", "b", 9);
        tree.flush_active_memtable(0)?;
        tree.major_compact(u64::MAX, 0)?;

        assert_eq!((0, 0), (seqno.get(), visible_seqno.get()));
    }

    let tree = Config::new(&folder, seqno, visible_seqno).open()?;
    assert_eq!(Some(9), tree.get_highest_persisted_seqno());

    Ok(())
}

#[test]
fn a_table_install_leaves_one_version_file() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();
    let tree = Config::new(
        &folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    )
    .open()?;

    for key in ["a", "b", "c"] {
        tree.insert(key, key, 0);
        tree.flush_active_memtable(0)?;
    }

    let version_files = std::fs::read_dir(folder.path())?
        .filter_map(Result::ok)
        .filter(|dirent| dirent.file_name().to_string_lossy().starts_with('v'))
        .count();
    assert_eq!(1, version_files);

    Ok(())
}
