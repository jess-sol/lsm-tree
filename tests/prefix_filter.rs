use lsm_tree::{
    config::PinningPolicy, get_tmp_folder, AbstractTree, Config, Guard, IterGuardImpl, KvPair,
    SeqNo, SequenceNumberCounter, Tree, UserKey, UserValue,
};
use test_log::test;

type Row = (UserKey, Option<UserValue>);

// --- Harness ---

/// The first bytes of a key that the filtered trees hold.
const LEN: u32 = 4;

fn open(
    folder: &tempfile::TempDir,
    configure: impl FnOnce(Config) -> Config,
) -> lsm_tree::Result<Tree> {
    let config = Config::new(
        folder,
        SequenceNumberCounter::default(),
        SequenceNumberCounter::default(),
    );

    match configure(config).open()? {
        lsm_tree::AnyTree::Standard(tree) => Ok(tree),
        lsm_tree::AnyTree::Blob(_) => unreachable!("a tree without key-value separation"),
    }
}

fn key(prefix: &str, i: usize) -> UserKey {
    format!("{prefix}{i:05}").as_bytes().into()
}

fn rows(prefix: &str, count: usize) -> Vec<Row> {
    (0..count)
        .map(|i| (key(prefix, i), Some("v".as_bytes().into())))
        .collect()
}

fn collect(rows: impl Iterator<Item = IterGuardImpl>) -> lsm_tree::Result<Vec<KvPair>> {
    rows.map(Guard::into_inner).collect()
}

fn may_hold(tree: &Tree, prefix: &str) -> lsm_tree::Result<bool> {
    tree.view(SeqNo::MAX).may_hold_prefix(prefix.as_bytes())
}

// --- Tests ---

#[test]
fn a_prefix_a_key_lies_under_is_never_ruled_out() -> lsm_tree::Result<()> {
    for partitioned in [false, true] {
        let folder = get_tmp_folder();
        let tree = open(&folder, |config| {
            config
                .filter_prefix_len(LEN)
                .filter_block_partitioning_policy(PinningPolicy::all(partitioned))
        })?;

        // The prefixes held, each asked at the length, longer and shorter
        let held = |tree: &Tree, prefixes: &[&str]| -> lsm_tree::Result<()> {
            for prefix in prefixes {
                for asked in [
                    (*prefix).to_string(),
                    format!("{prefix}0"),
                    prefix[..2].to_string(),
                ] {
                    assert!(
                        may_hold(tree, &asked)?,
                        "{asked}, partitioned {partitioned}"
                    );
                }
            }
            Ok(())
        };

        // In the active memtable
        tree.insert(key("aaaa", 1), "v", 1);
        held(&tree, &["aaaa"])?;

        // In a sealed run above it
        let _ = tree.write_sorted(rows("bbbb", 20_000), 2);
        held(&tree, &["aaaa", "bbbb"])?;

        // In a table
        tree.flush_active_memtable(0)?;
        tree.insert(key("dddd", 1), "v", 3);
        held(&tree, &["aaaa", "bbbb", "dddd"])?;

        // In the tables of a run after a merge
        tree.flush_active_memtable(0)?;
        tree.major_compact(4 * 1_024, 0)?;
        assert!(
            tree.table_count() > 1,
            "the merge writes a run of several tables"
        );
        held(&tree, &["aaaa", "bbbb", "dddd"])?;

        // And a prefix no key lies under is ruled out, between the held and past them
        for absent in ["cccc", "cccc0", "zzzz"] {
            assert!(
                !may_hold(&tree, absent)?,
                "{absent}, partitioned {partitioned}"
            );
        }
    }

    Ok(())
}

#[test]
fn a_table_written_before_the_length_was_set_rules_no_prefix_out() -> lsm_tree::Result<()> {
    let folder = get_tmp_folder();

    {
        let tree = open(&folder, |config| config)?;
        tree.insert(key("aaaa", 1), "v", 1);
        tree.insert(key("dddd", 1), "v", 1);
        tree.flush_active_memtable(0)?;
    }

    let tree = open(&folder, |config| config.filter_prefix_len(LEN))?;

    // Its filter holds no prefix, so it is not asked for one
    assert!(may_hold(&tree, "aaaa")?);
    assert!(may_hold(&tree, "cccc")?);

    Ok(())
}

#[test]
fn a_range_within_a_prefix_reads_the_same_with_the_filter() -> lsm_tree::Result<()> {
    let (plain_folder, filtered_folder) = (get_tmp_folder(), get_tmp_folder());
    let plain = open(&plain_folder, |config| config)?;
    let filtered = open(&filtered_folder, |config| config.filter_prefix_len(LEN))?;

    let read_alike = |stage: &str| -> lsm_tree::Result<()> {
        let b = |i| key("bbbb", i);

        for prefix in ["aaaa", "bbbb", "bbbb001", "cccc", "dd"] {
            assert_eq!(
                collect(plain.prefix(prefix, SeqNo::MAX, None))?,
                collect(filtered.prefix(prefix, SeqNo::MAX, None))?,
                "prefix {prefix}, {stage}",
            );
        }

        // Both bounds in one prefix, a bound past the last key of it, and a range over two
        assert_eq!(
            collect(plain.range(b(100)..=b(300), SeqNo::MAX, None))?,
            collect(filtered.range(b(100)..=b(300), SeqNo::MAX, None))?,
            "inclusive, {stage}",
        );
        assert_eq!(
            collect(plain.range(b(100)..b(300), SeqNo::MAX, None))?,
            collect(filtered.range(b(100)..b(300), SeqNo::MAX, None))?,
            "exclusive, {stage}",
        );
        assert_eq!(
            collect(plain.range(b(19_990)..=b(99_999), SeqNo::MAX, None))?,
            collect(filtered.range(b(19_990)..=b(99_999), SeqNo::MAX, None))?,
            "past the last key, {stage}",
        );
        assert_eq!(
            collect(plain.range(key("aaaa", 0)..=b(50), SeqNo::MAX, None))?,
            collect(filtered.range(key("aaaa", 0)..=b(50), SeqNo::MAX, None))?,
            "over two prefixes, {stage}",
        );

        // The range begins under a prefix no key lies under and runs on into one that keys do
        let from_absent = collect(filtered.range(key("abab", 0)..=b(50), SeqNo::MAX, None))?;
        assert_eq!(51, from_absent.len(), "from an absent prefix, {stage}");

        Ok(())
    };

    for tree in [&plain, &filtered] {
        tree.insert(key("aaaa", 1), "v", 1);
        let _ = tree.write_sorted(rows("bbbb", 20_000), 2);
        tree.insert(key("dddd", 1), "v", 3);
    }
    read_alike("in memtables")?;

    for tree in [&plain, &filtered] {
        tree.flush_active_memtable(0)?;
    }
    read_alike("in one table")?;

    for tree in [&plain, &filtered] {
        tree.major_compact(4 * 1_024, 0)?;
        assert!(
            tree.table_count() > 1,
            "a prefix lies over several tables of a run"
        );
    }
    read_alike("in a run of tables")?;

    Ok(())
}
