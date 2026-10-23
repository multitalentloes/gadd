use anyhow::{Context, Result};
use crossbeam_channel::Sender;
use git2::{Repository, StatusOptions};
use std::{
    path::PathBuf,
    time::{Duration, Instant},
};

use crate::{
    changes::change::bytes_to_path,
    event_loop::{spawn_named_thread, Event},
    statuses::WORKTREE_STATUSES,
};

pub(crate) enum StageEvent {
    Discovered(Vec<Vec<u8>>),
    BatchWritten(Vec<Vec<u8>>),
    Finished(Result<()>),
}

pub(crate) enum StageScope {
    Folder(Vec<u8>),
    All,
}

struct StageItem {
    display_path: Vec<u8>,
    remove_path: Option<Vec<u8>>,
    add_path: Option<Vec<u8>>,
}

pub(crate) fn spawn_stage_thread(
    repo_path: PathBuf,
    scope: StageScope,
    event_sender: Sender<Event>,
) {
    spawn_named_thread("Staging", move || {
        let result = stage_changes(repo_path, scope, &event_sender);
        let _ = event_sender.send(Event::Stage(StageEvent::Finished(result)));
    });
}

fn stage_changes(
    repo_path: PathBuf,
    scope: StageScope,
    event_sender: &Sender<Event>,
) -> Result<()> {
    let repo = Repository::open(repo_path).context("Failed to open repository for staging")?;
    let mut index = repo
        .index()
        .context("Failed to open Git index for staging")?;

    let mut options = StatusOptions::default();
    options.include_ignored(false);
    options.include_untracked(true);
    options.recurse_untracked_dirs(true);
    let folder_prefix = match scope {
        StageScope::Folder(mut path) => {
            if !path.ends_with(b"/") {
                path.push(b'/');
            }
            Some(path)
        }
        StageScope::All => None,
    };
    let statuses = repo
        .statuses(Some(&mut options))
        .context("Failed to list files for staging")?;
    let mut items: Vec<StageItem> = statuses
        .iter()
        .filter(|entry| {
            let status = entry.status();
            match &folder_prefix {
                Some(prefix) => {
                    status == git2::Status::WT_NEW && entry.path_bytes().starts_with(prefix)
                }
                None => {
                    !status.is_conflicted()
                        && WORKTREE_STATUSES
                            .iter()
                            .any(|worktree_status| status.intersects(*worktree_status))
                }
            }
        })
        .map(|entry| {
            let status = entry.status();
            let delta = entry.index_to_workdir();
            let old_path = delta
                .as_ref()
                .and_then(|delta| delta.old_file().path_bytes().map(Vec::from));
            let new_path = delta
                .as_ref()
                .and_then(|delta| delta.new_file().path_bytes().map(Vec::from));
            let display_path = entry.path_bytes().to_vec();
            StageItem {
                display_path: display_path.clone(),
                remove_path: if status.is_wt_deleted() || status.is_wt_renamed() {
                    old_path.or_else(|| Some(display_path.clone()))
                } else {
                    None
                },
                add_path: if status.is_wt_deleted() {
                    None
                } else {
                    new_path.or(Some(display_path))
                },
            }
        })
        .collect();
    items.sort_by(|left, right| left.display_path.cmp(&right.display_path));
    if event_sender
        .send(Event::Stage(StageEvent::Discovered(
            items.iter().map(|item| item.display_path.clone()).collect(),
        )))
        .is_err()
    {
        return Ok(());
    }

    const BATCH_SIZE: usize = 64;
    let mut written_batch = Vec::with_capacity(BATCH_SIZE);
    let mut last_write = Instant::now();
    for item in items {
        if let Some(path) = &item.remove_path {
            index.remove_path(bytes_to_path(path)).with_context(|| {
                format!(
                    "Failed to stage deletion of '{}'",
                    String::from_utf8_lossy(path)
                )
            })?;
        }
        if let Some(path) = &item.add_path {
            index
                .add_path(bytes_to_path(path))
                .with_context(|| format!("Failed to stage '{}'", String::from_utf8_lossy(path)))?;
        }
        written_batch.push(item.display_path);
        if written_batch.len() >= BATCH_SIZE || last_write.elapsed() >= Duration::from_millis(100) {
            index
                .write()
                .context("Failed to write staged files to Git index")?;
            if event_sender
                .send(Event::Stage(StageEvent::BatchWritten(std::mem::take(
                    &mut written_batch,
                ))))
                .is_err()
            {
                return Ok(());
            }
            last_write = Instant::now();
        }
    }
    if !written_batch.is_empty() {
        index
            .write()
            .context("Failed to write staged files to Git index")?;
        let _ = event_sender.send(Event::Stage(StageEvent::BatchWritten(written_batch)));
    }
    Ok(())
}

#[cfg(test)]
mod tests {
    use super::*;
    use crate::changes::change_list::ChangeList;
    use git2::Signature;
    use std::fs;

    struct TestRepository(PathBuf);

    impl Drop for TestRepository {
        fn drop(&mut self) {
            let _ = fs::remove_dir_all(&self.0);
        }
    }

    fn repository_with_folder() -> Result<(TestRepository, Repository)> {
        let path = std::env::temp_dir().join(format!(
            "gadd-folder-progress-{}-{}",
            std::process::id(),
            std::time::SystemTime::now()
                .duration_since(std::time::UNIX_EPOCH)?
                .as_nanos()
        ));
        fs::create_dir(&path)?;
        let cleanup = TestRepository(path.clone());
        let repo = Repository::init(&path)?;
        fs::write(path.join(".gitignore"), "folder/ignored.txt\n")?;
        fs::write(path.join("tracked.txt"), "before\n")?;
        let mut index = repo.index()?;
        index.add_path(std::path::Path::new(".gitignore"))?;
        index.add_path(std::path::Path::new("tracked.txt"))?;
        let tree_id = index.write_tree()?;
        let tree = repo.find_tree(tree_id)?;
        let author = Signature::now("gadd test", "test@example.invalid")?;
        repo.commit(Some("HEAD"), &author, &author, "initial", &tree, &[])?;
        index.write()?;
        drop(tree);

        fs::create_dir(path.join("folder"))?;
        for number in 0..130 {
            fs::write(
                path.join(format!("folder/file-{number:02}.txt")),
                b"content",
            )?;
        }
        fs::create_dir(path.join("folder/nested"))?;
        fs::write(path.join("folder/nested/inner.txt"), b"nested")?;
        fs::write(path.join("folder/ignored.txt"), b"ignored")?;

        Ok((cleanup, repo))
    }

    #[test]
    fn staged_folder_progress_matches_written_index() -> Result<()> {
        let (_cleanup, repo) = repository_with_folder()?;
        let mut changes = ChangeList::new(&repo)?;
        let folder = changes.selected_new_folder().expect("new folder row");
        changes.begin_staging(folder.clone());
        let (sender, receiver) = crossbeam_channel::unbounded();
        stage_changes(changes.repo_path(), StageScope::Folder(folder), &sender)?;

        let mut batches = 0;
        for event in receiver.try_iter() {
            match event {
                Event::Stage(StageEvent::Discovered(paths)) => {
                    assert_eq!(paths.len(), 131);
                    changes.expand_staging_paths(paths);
                    assert_eq!(changes.stage_progress(), Some((0, Some(131))));
                }
                Event::Stage(StageEvent::BatchWritten(paths)) => {
                    let mut written_index = repo.index()?;
                    written_index.read(true)?;
                    for path in &paths {
                        assert!(written_index.get_path(bytes_to_path(path), 0).is_some());
                        assert!(changes.is_pending_stage(path));
                    }
                    changes.complete_staging_batch(paths);
                    batches += 1;
                }
                _ => panic!("unexpected folder staging event"),
            }
        }
        assert!(batches > 1);
        assert_eq!(changes.stage_progress(), Some((131, Some(131))));
        changes.finish_staging()?;
        assert!(!changes.is_staging());
        assert_eq!(changes.changes.len(), 131);
        assert!(repo
            .index()?
            .get_path(std::path::Path::new("folder/ignored.txt"), 0)
            .is_none());
        Ok(())
    }

    #[test]
    fn stage_all_expands_new_folder_and_preserves_staged_changes() -> Result<()> {
        let (cleanup, repo) = repository_with_folder()?;
        fs::write(cleanup.0.join("tracked.txt"), "after\n")?;
        fs::write(cleanup.0.join("already-staged.txt"), "staged\n")?;
        let mut index = repo.index()?;
        index.add_path(std::path::Path::new("already-staged.txt"))?;
        index.write()?;

        let mut changes = ChangeList::new(&repo)?;
        changes.begin_all_staging();
        let (sender, receiver) = crossbeam_channel::unbounded();
        stage_changes(changes.repo_path(), StageScope::All, &sender)?;

        let mut saw_expansion = false;
        let mut batches = 0;
        for event in receiver.try_iter() {
            match event {
                Event::Stage(StageEvent::Discovered(paths)) => {
                    assert_eq!(paths.len(), 132);
                    changes.expand_staging_paths(paths);
                    assert_eq!(changes.changes.len(), 133);
                    assert_eq!(changes.stage_progress(), Some((0, Some(132))));
                    saw_expansion = true;
                }
                Event::Stage(StageEvent::BatchWritten(paths)) => {
                    let mut written_index = repo.index()?;
                    written_index.read(true)?;
                    for path in &paths {
                        assert!(written_index.get_path(bytes_to_path(path), 0).is_some());
                    }
                    changes.complete_staging_batch(paths);
                    batches += 1;
                }
                _ => panic!("unexpected stage all event"),
            }
        }
        assert!(saw_expansion);
        assert!(batches > 1);
        assert_eq!(changes.stage_progress(), Some((132, Some(132))));
        changes.finish_staging()?;
        assert_eq!(changes.changes.len(), 133);
        let mut index = repo.index()?;
        index.read(true)?;
        assert!(index
            .get_path(std::path::Path::new("already-staged.txt"), 0)
            .is_some());
        assert!(index
            .get_path(std::path::Path::new("folder/ignored.txt"), 0)
            .is_none());
        assert!(repo
            .status_file(std::path::Path::new("tracked.txt"))?
            .is_index_modified());
        Ok(())
    }

    #[test]
    fn stage_all_handles_deletion_rename_and_mixed_changes() -> Result<()> {
        let (cleanup, repo) = repository_with_folder()?;
        fs::write(cleanup.0.join("renamed.txt"), "rename me")?;
        fs::write(cleanup.0.join("deleted.txt"), "delete me")?;
        let mut index = repo.index()?;
        index.add_path(std::path::Path::new("renamed.txt"))?;
        index.add_path(std::path::Path::new("deleted.txt"))?;
        let tree_id = index.write_tree()?;
        let tree = repo.find_tree(tree_id)?;
        let parent = repo.head()?.peel_to_commit()?;
        let author = Signature::now("gadd test", "test@example.invalid")?;
        repo.commit(Some("HEAD"), &author, &author, "files", &tree, &[&parent])?;
        drop(tree);

        fs::remove_file(cleanup.0.join("deleted.txt"))?;
        fs::rename(cleanup.0.join("renamed.txt"), cleanup.0.join("moved.txt"))?;
        fs::write(cleanup.0.join("tracked.txt"), "staged\n")?;
        index.add_path(std::path::Path::new("tracked.txt"))?;
        index.write()?;
        fs::write(cleanup.0.join("tracked.txt"), "latest\n")?;

        let (sender, _receiver) = crossbeam_channel::unbounded();
        stage_changes(repo.path().to_path_buf(), StageScope::All, &sender)?;
        let mut index = repo.index()?;
        index.read(true)?;
        assert!(index
            .get_path(std::path::Path::new("deleted.txt"), 0)
            .is_none());
        assert!(index
            .get_path(std::path::Path::new("renamed.txt"), 0)
            .is_none());
        assert!(index
            .get_path(std::path::Path::new("moved.txt"), 0)
            .is_some());
        let staged = index
            .get_path(std::path::Path::new("tracked.txt"), 0)
            .unwrap();
        assert_eq!(repo.find_blob(staged.id)?.content(), b"latest\n");
        assert!(!repo
            .status_file(std::path::Path::new("tracked.txt"))?
            .is_wt_modified());
        Ok(())
    }

    #[cfg(unix)]
    #[test]
    fn stage_all_handles_type_change() -> Result<()> {
        let (cleanup, repo) = repository_with_folder()?;
        fs::remove_file(cleanup.0.join("tracked.txt"))?;
        std::os::unix::fs::symlink("folder/file-00.txt", cleanup.0.join("tracked.txt"))?;
        let (sender, _receiver) = crossbeam_channel::unbounded();
        stage_changes(repo.path().to_path_buf(), StageScope::All, &sender)?;
        let mut index = repo.index()?;
        index.read(true)?;
        let entry = index
            .get_path(std::path::Path::new("tracked.txt"), 0)
            .unwrap();
        assert_eq!(entry.mode, 0o120000);
        Ok(())
    }

    #[test]
    fn index_lock_failure_does_not_report_written_progress() -> Result<()> {
        let (_cleanup, repo) = repository_with_folder()?;
        fs::write(repo.path().join("index.lock"), b"locked")?;
        let (sender, receiver) = crossbeam_channel::unbounded();
        assert!(stage_changes(repo.path().to_path_buf(), StageScope::All, &sender).is_err());
        assert!(receiver
            .try_iter()
            .all(|event| !matches!(event, Event::Stage(StageEvent::BatchWritten(_)))));
        assert!(repo
            .index()?
            .get_path(std::path::Path::new("folder/file-00.txt"), 0)
            .is_none());
        Ok(())
    }

    #[test]
    fn folder_name_with_pathspec_characters_is_staged_literally() -> Result<()> {
        let (cleanup, repo) = repository_with_folder()?;
        fs::create_dir(cleanup.0.join("[draft]"))?;
        fs::write(cleanup.0.join("[draft]/item.txt"), "draft")?;
        let (sender, receiver) = crossbeam_channel::unbounded();
        stage_changes(
            repo.path().to_path_buf(),
            StageScope::Folder(b"[draft]".to_vec()),
            &sender,
        )?;
        let discovered = receiver.try_iter().find_map(|event| match event {
            Event::Stage(StageEvent::Discovered(paths)) => Some(paths),
            _ => None,
        });
        assert_eq!(discovered, Some(vec![b"[draft]/item.txt".to_vec()]));
        let mut index = repo.index()?;
        index.read(true)?;
        assert!(index
            .get_path(std::path::Path::new("[draft]/item.txt"), 0)
            .is_some());
        assert!(index
            .get_path(std::path::Path::new("folder/file-00.txt"), 0)
            .is_none());
        Ok(())
    }
}
