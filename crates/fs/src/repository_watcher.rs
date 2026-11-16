use crate::{Fs, PathEvent, PathEventKind, Watcher};
use anyhow::Result;
use futures::{Stream, StreamExt};
use gix::notify::{Changes, InvalidationPath, Options, PathChange, RepositoryMonitor, Scope};
use gpui::{BackgroundExecutor, Task};
use parking_lot::Mutex;
use std::{
    collections::{HashMap, HashSet},
    path::{Path, PathBuf},
    pin::Pin,
    sync::Arc,
    time::{Duration, Instant},
};
use util::ResultExt;

pub(crate) async fn watch(
    fs: Arc<dyn Fs>,
    path: &Path,
    executor: BackgroundExecutor,
    latency: Duration,
) -> Option<(
    Pin<Box<dyn Send + Stream<Item = Vec<PathEvent>>>>,
    Arc<dyn Watcher>,
)> {
    let path = path.to_path_buf();
    if path
        .components()
        .any(|component| component == std::path::Component::ParentDir)
    {
        return None;
    }
    let setup = executor
        .spawn({
            let path = path.clone();
            async move {
                let Some(parent) = path.ancestors().skip(1).find(|parent| parent.is_dir()) else {
                    return Ok(None);
                };
                let repository = match gix::discover(parent) {
                    Ok(repository) => repository,
                    Err(error) => {
                        log::debug!("no repository for file watch at {path:?}: {error:#}");
                        return Ok(None);
                    }
                };
                let physical_parent = parent.canonicalize()?;
                let Some(root) = repository.workdir() else {
                    return Ok(None);
                };
                let physical_root = root.canonicalize()?;
                let depth = physical_parent
                    .strip_prefix(&physical_root)?
                    .components()
                    .count();
                let Some(logical_root) = parent.ancestors().nth(depth) else {
                    return Ok(None);
                };
                // Additional paths resolve symlinks once; raw notifications also track changes to the link itself.
                if has_symlink(&path, logical_root) {
                    return Ok(None);
                }
                let physical_path = physical_parent.join(path.strip_prefix(parent)?);
                let relative_path =
                    gix::path::into_bstr(physical_path.strip_prefix(&physical_root)?)?;
                let relative_path =
                    gix::path::to_unix_separators_on_windows(relative_path).into_owned();
                let mut monitor = repository.monitor(Options {
                    // shortcut: metadata stays enabled until gitoxide respects disabled checkout coverage with metadata disabled.
                    worktree_coverage: None,
                    additional_paths: vec![InvalidationPath {
                        path: path.clone(),
                        changes: Changes {
                            worktree: Some(Scope::Paths(vec![PathChange {
                                path: relative_path,
                                kind: gix::notify::plumbing::EventKind::Any,
                                path_kind: gix::notify::plumbing::PathKind::File,
                            }])),
                            ..Changes::default()
                        },
                    }],
                    ..Options::default()
                })?;
                let outcome = monitor.service(Instant::now(), || gix::open(repository.git_dir()));
                for error in outcome.errors {
                    log::error!("registering file watch at {path:?}: {error:#}");
                }
                Ok::<_, anyhow::Error>(Some((
                    repository.git_dir().to_path_buf(),
                    monitor,
                    outcome.changes,
                    logical_root.to_path_buf(),
                )))
            }
        })
        .await
        .log_err()??;

    let (sender, receiver) = async_channel::bounded(1);
    let pending_paths = Arc::new(Mutex::new(Vec::new()));
    let watcher = Arc::new(RepositoryWatcher {
        fs,
        git_dir: setup.0,
        executor: executor.clone(),
        sender,
        pending_paths: pending_paths.clone(),
        tasks: Default::default(),
        roots: Arc::new(Mutex::new(HashSet::from([path.clone()]))),
    });
    if setup.2.worktree.is_some() {
        watcher.publish(&path);
    }
    watcher.start(path.clone(), setup.1, setup.3);
    let mut existed = path.try_exists().log_err().unwrap_or(true);
    let stream = crate::fs_watcher::event_stream(
        receiver,
        pending_paths,
        watcher.clone(),
        executor,
        latency,
    );
    let stream = stream.map(move |mut events| {
        let mut invalidated = false;
        let mut rescan = false;
        events.retain(|event| {
            if event.path == path {
                invalidated = true;
                rescan |= event.kind == Some(PathEventKind::Rescan);
                false
            } else {
                true
            }
        });
        if invalidated {
            // Additional paths describe invalidations; reconcile file operations after collecting the burst.
            let kind = match path.try_exists() {
                Ok(exists) => {
                    let kind = match (existed, exists) {
                        (false, true) => PathEventKind::Created,
                        (true, false) => PathEventKind::Removed,
                        _ if rescan => PathEventKind::Rescan,
                        _ => PathEventKind::Changed,
                    };
                    existed = exists;
                    kind
                }
                Err(error) => {
                    log::error!("inspecting watched file {path:?}: {error}");
                    PathEventKind::Rescan
                }
            };
            events.push(PathEvent {
                path: path.clone(),
                kind: Some(kind),
            });
        }
        events
    });
    Some((Box::pin(stream), watcher))
}

struct RepositoryWatcher {
    fs: Arc<dyn Fs>,
    git_dir: PathBuf,
    executor: BackgroundExecutor,
    sender: async_channel::Sender<()>,
    pending_paths: Arc<Mutex<Vec<PathEvent>>>,
    tasks: Mutex<HashMap<PathBuf, Registration>>,
    roots: Arc<Mutex<HashSet<PathBuf>>>,
}

struct Registration {
    active: Arc<Mutex<bool>>,
    _task: Task<()>,
}

impl RepositoryWatcher {
    fn publish(&self, path: &Path) {
        publish(
            path,
            PathEventKind::Rescan,
            &self.sender,
            &self.pending_paths,
            &self.roots,
        );
    }

    fn start(&self, path: PathBuf, mut monitor: RepositoryMonitor, logical_root: PathBuf) {
        let sender = self.sender.clone();
        let pending_paths = self.pending_paths.clone();
        let executor = self.executor.clone();
        let git_dir = self.git_dir.clone();
        let fs = self.fs.clone();
        let roots = self.roots.clone();
        let active = Arc::new(Mutex::new(true));
        let task = executor.spawn({
            let path = path.clone();
            let executor = executor.clone();
            let active = active.clone();
            async move {
                let mut initial = true;
                loop {
                    let outcome = monitor.service(Instant::now(), || gix::open(&git_dir));
                    for error in outcome.errors {
                        log::error!("file watch at {path:?}: {error:#}");
                    }
                    if (initial || outcome.changes.worktree.is_some())
                        && has_symlink(&path, &logical_root)
                    {
                        drop(monitor);
                        let events = fs.watch_file(&path, Duration::ZERO).await;
                        forward_events(events, path, active, sender, pending_paths, roots).await;
                        return;
                    }
                    initial = false;
                    if outcome.changes.worktree.is_some() {
                        let changes = &outcome.changes;
                        let kind = if changes.references
                            || changes.index
                            || changes.configuration
                            || changes.ignores
                            || changes.attributes
                            || changes.operations
                            || changes.layout
                            || matches!(changes.worktree, Some(Scope::All))
                        {
                            PathEventKind::Rescan
                        } else {
                            PathEventKind::Changed
                        };
                        let active = active.lock();
                        if !*active || !publish(&path, kind, &sender, &pending_paths, &roots) {
                            return;
                        }
                    }
                    let Some(timeout) = monitor.next_timeout(Instant::now()) else {
                        return;
                    };
                    executor.timer(timeout).await;
                }
            }
        });
        self.tasks.lock().insert(
            path,
            Registration {
                active,
                _task: task,
            },
        );
    }
}

impl Watcher for RepositoryWatcher {
    fn add(&self, path: &Path) -> Result<()> {
        let mut tasks = self.tasks.lock();
        if !tasks.contains_key(path) {
            anyhow::ensure!(!path.as_os_str().is_empty(), "cannot watch an empty path");
            let path = path.to_path_buf();
            self.roots.lock().insert(path.clone());
            let active = Arc::new(Mutex::new(true));
            let task = self.executor.spawn({
                let fs = self.fs.clone();
                let path = path.clone();
                let active = active.clone();
                let sender = self.sender.clone();
                let pending_paths = self.pending_paths.clone();
                let roots = self.roots.clone();
                async move {
                    // Additional registrations can be directories, symlinks or paths on a different mount.
                    let events = fs.watch(&path, Duration::ZERO).await;
                    forward_events(events, path, active, sender, pending_paths, roots).await;
                }
            });
            tasks.insert(
                path,
                Registration {
                    active,
                    _task: task,
                },
            );
        }
        Ok(())
    }

    fn remove(&self, path: &Path) -> Result<()> {
        if let Some(registration) = self.tasks.lock().remove(path) {
            *registration.active.lock() = false;
        }
        let remaining_roots = {
            let mut roots = self.roots.lock();
            roots.remove(path);
            roots.iter().cloned().collect::<Vec<_>>()
        };
        self.pending_paths.lock().retain(|event| {
            remaining_roots
                .iter()
                .any(|root| event.path.starts_with(root))
        });
        Ok(())
    }
}

fn has_symlink(path: &Path, logical_root: &Path) -> bool {
    path.ancestors()
        .take_while(|ancestor| ancestor.starts_with(logical_root))
        .any(|ancestor| {
            std::fs::symlink_metadata(ancestor)
                .is_ok_and(|metadata| metadata.file_type().is_symlink())
        })
}

async fn forward_events(
    events: (
        Pin<Box<dyn Send + Stream<Item = Vec<PathEvent>>>>,
        Arc<dyn Watcher>,
    ),
    path: PathBuf,
    active: Arc<Mutex<bool>>,
    sender: async_channel::Sender<()>,
    pending_paths: Arc<Mutex<Vec<PathEvent>>>,
    roots: Arc<Mutex<HashSet<PathBuf>>>,
) {
    let (mut events, _watcher) = events;
    {
        let active = active.lock();
        if !*active
            || !publish(
                &path,
                PathEventKind::Rescan,
                &sender,
                &pending_paths,
                &roots,
            )
        {
            return;
        }
    }
    while let Some(events) = events.next().await {
        let active = active.lock();
        if !*active {
            return;
        }
        crate::fs_watcher::enqueue_path_events(&sender, &pending_paths, events, || {
            roots.lock().iter().cloned().collect()
        });
    }
}

fn publish(
    path: &Path,
    kind: PathEventKind,
    sender: &async_channel::Sender<()>,
    pending_paths: &Arc<Mutex<Vec<PathEvent>>>,
    roots: &Mutex<HashSet<PathBuf>>,
) -> bool {
    if sender.is_closed() {
        return false;
    }
    crate::fs_watcher::enqueue_path_events(
        sender,
        pending_paths,
        vec![PathEvent {
            path: path.to_path_buf(),
            kind: Some(kind),
        }],
        || roots.lock().iter().cloned().collect(),
    );
    !sender.is_closed()
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn preserves_invalidations_for_multiple_files() {
        let watched = PathBuf::from("z-watched");
        let additional = PathBuf::from("a-additional");
        let roots = Mutex::new(HashSet::from([watched.clone(), additional.clone()]));
        let pending_paths = Arc::new(Mutex::new(Vec::new()));
        let (sender, receiver) = async_channel::bounded(1);
        for path in [&watched, &additional, &watched] {
            assert!(publish(
                path,
                PathEventKind::Changed,
                &sender,
                &pending_paths,
                &roots,
            ));
        }
        assert_eq!(receiver.try_recv(), Ok(()));
        assert!(receiver.try_recv().is_err());
        assert_eq!(
            *pending_paths.lock(),
            vec![
                PathEvent {
                    path: additional,
                    kind: Some(PathEventKind::Changed),
                },
                PathEvent {
                    path: watched,
                    kind: Some(PathEventKind::Changed),
                },
            ]
        );
    }
}
