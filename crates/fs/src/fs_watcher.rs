use collections::FxHashMap;
use futures::{Future, FutureExt as _, Stream};
use gpui::{BackgroundExecutor, Task};
use notify::{Event, EventKind};
use parking_lot::Mutex;
use std::{
    collections::HashMap,
    ops::DerefMut,
    path::{Path, PathBuf},
    pin::Pin,
    sync::{
        Arc, LazyLock,
        atomic::{AtomicBool, Ordering},
    },
    task::{Poll, ready},
    time::Duration,
};
use util::{ResultExt, paths::SanitizedPath};

use crate::{Fs, PathEvent, PathEventKind, Watcher};

const MAX_DISPATCH_BATCH_SIZE: usize = 256;

mod diagnostics;
pub use diagnostics::{
    WatchDiagnosticEvent, WatchRecording, WatchRoot, WatchSnapshot, WatcherSnapshot,
};

#[derive(Clone, Copy, Debug, PartialEq, Eq, serde::Serialize)]
pub enum OsWatcherKind {
    Native,
    Poll,
}

impl OsWatcherKind {
    fn is_recursive(self) -> bool {
        // Emulating recursion would visit directories beyond the scanner's depth and ignore limits.
        self == Self::Poll || gix_notify::asynchronous::Watcher::supports_native_recursive_watches()
    }
}

pub(crate) async fn watch(
    fs: Arc<dyn Fs>,
    native_watcher: Arc<OsWatcher>,
    poll_watcher: Arc<OsWatcher>,
    executor: BackgroundExecutor,
    path: &Path,
    latency: Duration,
) -> (
    Pin<Box<dyn Send + Stream<Item = Vec<PathEvent>>>>,
    Arc<dyn Watcher>,
) {
    let (tx, rx) = async_channel::bounded(1);
    let pending_paths: Arc<Mutex<Vec<PathEvent>>> = Default::default();

    let watcher: Arc<dyn Watcher> = Arc::new(FsWatcher::new(
        native_watcher.clone(),
        poll_watcher.clone(),
        fs.clone(),
        executor.clone(),
        tx,
        pending_paths.clone(),
    ));

    if let Err(e) = watcher.add(path) {
        log::warn!("Failed to watch {}:\n{e}", path.display());
    }

    if (fs.is_fake() || fs.requires_poll_watcher(path))
        && let Some(mut target) = fs.read_link(path).await.ok()
    {
        log::trace!("watch symlink {path:?} -> {target:?}");
        // Check if symlink target is relative path, if so make it absolute
        if target.is_relative()
            && let Some(parent) = path.parent()
        {
            target = parent.join(target);
            if let Ok(canonical) = fs.canonicalize(&target).await {
                target = SanitizedPath::new(&canonical).as_path().to_path_buf();
            }
        }
        watcher.add(&target).log_err();
        // Skipped for poll watchers: PollWatcher::watch() recursively scans
        // at registration, blocking on large virtual filesystem mounts
        if let Some(parent) = target.parent()
            && !fs.requires_poll_watcher(parent)
        {
            watcher.add(parent).log_err();
        }
    }

    native_watcher.synchronize_registrations().await.log_err();

    (
        event_stream(rx, pending_paths, watcher.clone(), executor, latency),
        watcher,
    )
}

pub(crate) fn event_stream(
    receiver: async_channel::Receiver<()>,
    pending_paths: Arc<Mutex<Vec<PathEvent>>>,
    watcher: Arc<dyn Watcher>,
    executor: BackgroundExecutor,
    latency: Duration,
) -> Pin<Box<dyn Send + Stream<Item = Vec<PathEvent>>>> {
    let mut receiver = Box::pin(receiver);
    let mut delay: Option<Task<()>> = None;
    let stream = futures::stream::poll_fn({
        let watcher = watcher.clone();
        move |cx| {
            let _watcher = &watcher;
            loop {
                if let Some(timer) = &mut delay {
                    ready!(Pin::new(timer).poll(cx));
                    delay = None;
                    let paths = std::mem::take(&mut *pending_paths.lock());
                    if !paths.is_empty() {
                        return Poll::Ready(Some(paths));
                    }
                }
                let Some(()) = ready!(receiver.as_mut().poll_next(cx)) else {
                    return Poll::Ready(None);
                };
                if latency.is_zero() {
                    let paths = std::mem::take(&mut *pending_paths.lock());
                    if !paths.is_empty() {
                        return Poll::Ready(Some(paths));
                    }
                } else {
                    // Keep the timer in the stream so cancelling `next()` cannot consume its wakeup.
                    delay = Some(executor.timer(latency));
                }
            }
        }
    });
    Box::pin(stream)
}

pub struct FsWatcher {
    native_watcher: Arc<OsWatcher>,
    poll_watcher: Arc<OsWatcher>,
    fs: Arc<dyn Fs>,
    executor: BackgroundExecutor,
    tx: async_channel::Sender<()>,
    pending_path_events: Arc<Mutex<Vec<PathEvent>>>,
    rescan_roots: Arc<Mutex<HashMap<WatchKey, PathBuf>>>,
    registrations: Arc<Mutex<HashMap<WatchKey, FsWatcherRegistration>>>,
    pending_registrations: Arc<Mutex<HashMap<Arc<std::path::Path>, Task<()>>>>,
}

#[derive(Clone)]
struct FsWatcherRegistration {
    id: WatcherRegistrationId,
    os_watcher: Arc<OsWatcher>,
}

impl FsWatcher {
    pub(crate) fn new(
        native_watcher: Arc<OsWatcher>,
        poll_watcher: Arc<OsWatcher>,
        fs: Arc<dyn Fs>,
        executor: BackgroundExecutor,
        tx: async_channel::Sender<()>,
        pending_path_events: Arc<Mutex<Vec<PathEvent>>>,
    ) -> Self {
        Self {
            native_watcher,
            poll_watcher,
            fs,
            executor,
            tx,
            pending_path_events,
            rescan_roots: Default::default(),
            registrations: Default::default(),
            pending_registrations: Default::default(),
        }
    }

    fn add_existing_path(&self, path: Arc<Path>) -> anyhow::Result<()> {
        let case_insensitive = !self.fs.is_path_case_sensitive(&path);
        let key = WatchKey::for_registration(SanitizedPath::new(&path), case_insensitive);
        {
            let mut roots = self.rescan_roots.lock();
            roots.remove(&WatchKey::exact(SanitizedPath::new(&path)));
            roots.remove(&WatchKey::folded(SanitizedPath::new(&path)));
            roots.insert(key.clone(), path.to_path_buf());
        }
        let existing = self.registrations.lock().get(&key).cloned();
        if existing.is_some() {
            return Ok(());
        }
        let registration = match register_existing_path(
            &self.native_watcher,
            &self.poll_watcher,
            self.fs.as_ref(),
            path.clone(),
            case_insensitive,
            self.tx.clone(),
            self.pending_path_events.clone(),
            self.rescan_roots.clone(),
        ) {
            Ok(registration) => registration,
            Err(error) => {
                self.add_pending_path(path);
                return Err(error);
            }
        };
        self.registrations.lock().insert(key, registration);
        Ok(())
    }

    fn add_pending_path(&self, path: Arc<Path>) {
        let mut pending_registrations = self.pending_registrations.lock();
        if pending_registrations.contains_key(path.as_ref()) {
            return;
        }

        let task = self.executor.spawn(poll_path_until_created(
            self.native_watcher.clone(),
            self.poll_watcher.clone(),
            self.fs.clone(),
            self.executor.clone(),
            path.clone(),
            self.tx.clone(),
            self.pending_path_events.clone(),
            self.rescan_roots.clone(),
            self.registrations.clone(),
            self.pending_registrations.clone(),
        ));
        pending_registrations.insert(path, task);
    }
}

impl Drop for FsWatcher {
    fn drop(&mut self) {
        self.pending_registrations.lock().clear();
        self.rescan_roots.lock().clear();

        let mut registrations = HashMap::new();
        {
            let old = &mut self.registrations.lock();
            std::mem::swap(old.deref_mut(), &mut registrations);
        }

        for (_, registration) in registrations {
            registration.os_watcher.remove(registration.id);
        }
    }
}

impl Watcher for FsWatcher {
    fn add(&self, path: &std::path::Path) -> anyhow::Result<()> {
        log::trace!("watcher add: {path:?}");

        let path: Arc<Path> = path.into();
        if path_covered_by_recursive_registration(
            &self.registrations.lock(),
            SanitizedPath::new(&path),
        ) {
            log::trace!("path to watch is covered by an existing registration: {path:?}");
            return Ok(());
        }

        if self
            .pending_registrations
            .lock()
            .contains_key(path.as_ref())
        {
            log::trace!("path to watch is already pending: {path:?}");
            return Ok(());
        }

        if !self.fs.path_exists(&path) {
            let key = WatchKey::for_registration(
                SanitizedPath::new(&path),
                !self.fs.is_path_case_sensitive(&path),
            );
            self.rescan_roots.lock().insert(key, path.to_path_buf());
            self.add_pending_path(path);
            return Ok(());
        }

        self.add_existing_path(path)
    }

    fn remove(&self, path: &std::path::Path) -> anyhow::Result<()> {
        log::trace!("remove watched path: {path:?}");
        self.pending_registrations.lock().remove(path);

        let sanitized = SanitizedPath::new(path);
        {
            let mut roots = self.rescan_roots.lock();
            roots.remove(&WatchKey::exact(sanitized));
            roots.remove(&WatchKey::folded(sanitized));
        }
        let registration = {
            let mut registrations = self.registrations.lock();
            registrations
                .remove(&WatchKey::exact(sanitized))
                .or_else(|| registrations.remove(&WatchKey::folded(sanitized)))
        };
        if let Some(registration) = registration {
            registration.os_watcher.remove(registration.id);
        }
        Ok(())
    }
}

fn path_covered_by_recursive_registration(
    registrations: &HashMap<WatchKey, FsWatcherRegistration>,
    path: &SanitizedPath,
) -> bool {
    if path
        .as_path()
        .components()
        .any(|component| component == std::path::Component::ParentDir)
    {
        return false;
    }
    path.as_path().ancestors().skip(1).any(|ancestor| {
        let ancestor = SanitizedPath::unchecked_new(ancestor);
        [WatchKey::exact(ancestor), WatchKey::folded(ancestor)]
            .iter()
            .any(|key| {
                registrations
                    .get(key)
                    .is_some_and(|registration| registration.os_watcher.is_recursive())
            })
    })
}

/// Detect whether a path requires polling instead of native file watching.
///
/// Returns `true` for filesystem types where inotify/FSEvents/ReadDirectoryChanges
/// silently fail to deliver events: 9P (WSL drvfs), NFS, CIFS/SMB, FUSE (sshfs), etc.
///
/// Can be overridden with the `ZED_FILE_WATCHER_MODE` environment variable:
/// - `native` — always use native OS watcher
/// - `poll` — always use polling
/// - `auto` (default) — auto-detect based on filesystem type
pub fn requires_poll_watcher(path: &Path) -> bool {
    match std::env::var("ZED_FILE_WATCHER_MODE")
        .as_deref()
        .unwrap_or("auto")
    {
        "native" => return false,
        "poll" => return true,
        _ => {}
    }

    #[cfg(target_os = "linux")]
    {
        let existing = path
            .ancestors()
            .find(|ancestor| ancestor.exists())
            .unwrap_or(path);
        return detect_requires_poll_watcher_linux(existing);
    }

    #[cfg(not(target_os = "linux"))]
    {
        let _ = path;
        false
    }
}

fn register_existing_path(
    native_watcher: &Arc<OsWatcher>,
    poll_watcher: &Arc<OsWatcher>,
    fs: &dyn Fs,
    path: Arc<Path>,
    case_insensitive: bool,
    tx: async_channel::Sender<()>,
    pending_path_events: Arc<Mutex<Vec<PathEvent>>>,
    rescan_roots: Arc<Mutex<HashMap<WatchKey, PathBuf>>>,
) -> anyhow::Result<FsWatcherRegistration> {
    let os_watcher = if fs.requires_poll_watcher(path.as_ref()) {
        log::info!(
            "Using poll watcher ({}ms interval) for {}",
            poll_interval().as_millis(),
            path.display()
        );
        telemetry::event!("fs_watcher_poll", path = path.display().to_string());
        poll_watcher
    } else {
        native_watcher
    };
    let root_path = SanitizedPath::new_arc(path.as_ref());
    let path_for_callback = path.clone();
    let registration_id =
        os_watcher.add(path, case_insensitive, move |event: &notify::Event| {
            log::trace!("watcher received event: {event:?}");
            push_notify_event(
                &tx,
                &pending_path_events,
                &root_path,
                case_insensitive,
                path_for_callback.as_ref(),
                event,
                &rescan_roots,
            );
        })?;
    Ok(FsWatcherRegistration {
        id: registration_id,
        os_watcher: os_watcher.clone(),
    })
}

#[cfg(target_os = "linux")]
fn detect_requires_poll_watcher_linux(path: &Path) -> bool {
    use std::ffi::CString;
    use std::os::unix::ffi::OsStrExt;

    let c_path = match CString::new(path.as_os_str().as_bytes()) {
        Ok(p) => p,
        Err(_) => return false,
    };

    let mut stat: libc::statfs = unsafe { std::mem::zeroed() };
    if unsafe { libc::statfs(c_path.as_ptr(), &mut stat) } != 0 {
        return false;
    }

    const V9FS_MAGIC: u64 = 0x0102_1997;
    const NFS_SUPER_MAGIC: u64 = 0x0000_6969;
    const CIFS_MAGIC: u64 = 0xFF53_4D42;
    const SMB_SUPER_MAGIC: u64 = 0x0000_517B;
    const SMB2_MAGIC: u64 = 0xFE53_4D42;
    const FUSE_SUPER_MAGIC: u64 = 0x6573_5546;

    let fs_type = (stat.f_type as u64) & 0xFFFF_FFFF;
    if fs_type == FUSE_SUPER_MAGIC && is_virtiofs(path) {
        return false;
    }

    if fs_type == V9FS_MAGIC
        || fs_type == NFS_SUPER_MAGIC
        || fs_type == CIFS_MAGIC
        || fs_type == SMB_SUPER_MAGIC
        || fs_type == SMB2_MAGIC
        || fs_type == FUSE_SUPER_MAGIC
    {
        log::info!(
            "Detected network/virtual filesystem (type 0x{:x}) at {}, using poll watcher",
            fs_type,
            path.display()
        );
        return true;
    }

    if is_wsl_drvfs_path(path) {
        log::info!(
            "Detected WSL drvfs mount at {}, using poll watcher",
            path.display()
        );
        return true;
    }

    false
}

#[cfg(target_os = "linux")]
fn is_virtiofs(path: &Path) -> bool {
    let Ok(mountinfo) = std::fs::read_to_string("/proc/self/mountinfo") else {
        return false;
    };

    let mut best_mount = None;
    for line in mountinfo.lines() {
        let fields = line.split(' ').collect::<Vec<_>>();
        let Some(separator) = fields.iter().position(|field| *field == "-") else {
            continue;
        };
        let (Some(mount_point), Some(fs_type)) = (fields.get(4), fields.get(separator + 1)) else {
            continue;
        };

        let mount_point = mount_point
            .replace("\\040", " ")
            .replace("\\011", "\t")
            .replace("\\012", "\n")
            .replace("\\134", "\\");
        if path.starts_with(&mount_point)
            && best_mount.is_none_or(|(length, _)| mount_point.len() > length)
        {
            best_mount = Some((mount_point.len(), *fs_type));
        }
    }

    best_mount.is_some_and(|(_, fs_type)| fs_type == "virtiofs" || fs_type == "fuse.virtiofs")
}

#[cfg(target_os = "linux")]
fn is_wsl_drvfs_path(path: &Path) -> bool {
    if std::env::var_os("WSL_DISTRO_NAME").is_none() {
        if let Ok(version) = std::fs::read_to_string("/proc/version") {
            let version = version.to_lowercase();
            if !version.contains("microsoft") && !version.contains("wsl") {
                return false;
            }
        } else {
            return false;
        }
    }

    let Some(path) = path.to_str() else {
        return false;
    };
    if !path.starts_with("/mnt/") || path.len() < 6 {
        return false;
    }
    let after_mnt = &path[5..];
    after_mnt.starts_with(|c: char| c.is_ascii_alphabetic())
        && (after_mnt.len() == 1 || after_mnt.as_bytes()[1] == b'/')
}

/// Whether the volume backing `path` does case-insensitive name lookups, used to
/// pick exact vs. folded matching.
#[cfg(target_os = "macos")]
pub(crate) fn case_insensitive_path(path: &Path) -> bool {
    use std::os::unix::ffi::OsStrExt as _;

    // `pathconf(_PC_CASE_SENSITIVE)` returns 1 (sensitive), 0 (insensitive), or -1
    // on error; default errors to insensitive (the APFS/HFS+ default).
    let Ok(c_path) = std::ffi::CString::new(path.as_os_str().as_bytes()) else {
        return true;
    };
    // SAFETY: We just initialized c_path, so it's a valid pointer
    unsafe { libc::pathconf(c_path.as_ptr(), libc::_PC_CASE_SENSITIVE) == 0 }
}

#[cfg(target_os = "linux")]
pub(crate) fn case_insensitive_path(_path: &Path) -> bool {
    // use std::os::unix::ffi::OsStrExt as _;

    // // Only ext4/f2fs casefold (`+F`) dirs are insensitive, reported by `statx` via
    // // STATX_ATTR_CASEFOLD; any failure (e.g. pre-4.11 ENOSYS) means case-sensitive.
    // const STATX_ATTR_CASEFOLD: u64 = 0x0000_2000;
    // let Ok(c_path) = std::ffi::CString::new(path.as_os_str().as_bytes()) else {
    //     return false;
    // };
    // let mut buf = std::mem::MaybeUninit::<libc::statx>::zeroed();

    // // SAFETY: c_path is still valid, buffer has been zeroed
    // if unsafe { libc::statx(libc::AT_FDCWD, c_path.as_ptr(), 0, 0, buf.as_mut_ptr()) } != 0 {
    //     return false;
    // }

    // // SAFETY: libc statx initialized this buffer, otherwise we would've returned on a error
    // // in that function call
    // let buf = unsafe { buf.assume_init() };
    // buf.stx_attributes_mask & STATX_ATTR_CASEFOLD != 0
    //     && buf.stx_attributes & STATX_ATTR_CASEFOLD != 0
    false
}

#[cfg(target_os = "windows")]
pub(crate) fn case_insensitive_path(_path: &Path) -> bool {
    // todo(windows): Windows defaults to case in sensitive, but
    // they can mark specific directories as case sensitive. Mainly
    // for WSL use cases
    true
}

#[cfg(not(any(target_os = "macos", target_os = "linux", target_os = "windows")))]
pub(crate) fn case_insensitive_path(_path: &Path) -> bool {
    // Other BSDs default to case-sensitive local filesystems.
    false
}

/// Whether `path` is `root` or sits beneath it, folding case on case-insensitive
/// volumes so a differently-cased spelling still matches.
fn path_is_under(path: &SanitizedPath, root: &SanitizedPath, case_insensitive: bool) -> bool {
    if case_insensitive {
        let path = path.as_path().to_string_lossy().to_lowercase();
        let root = root.as_path().to_string_lossy().to_lowercase();
        Path::new(&path).starts_with(Path::new(&root))
    } else {
        path.starts_with(root)
    }
}

/// Lookup key for a watch path, shared by add, remove, and dispatch so they all
/// agree on whether two spellings denote the same directory.
///
/// On case-sensitive volumes the exact (sanitized) path is the key, so genuinely
/// distinct directories stay distinct. On case-insensitive volumes the folded
/// (lowercased) spelling is the key, so any casing of a directory collides.
///
/// The two variants are distinct map keys, so a case-sensitive registration can
/// never be hit by a folded lookup (or vice versa); dispatch can therefore probe
/// both forms of an event path without risking a cross-rule false match.
///
/// Folded keys use path-component spelling so separators and `.` components
/// agree with exact path equality. macOS also normalizes Unicode before folding.
#[derive(Clone, Debug, PartialEq, Eq, Hash, PartialOrd, Ord)]
enum WatchKey {
    Exact(Arc<Path>),
    Folded(Arc<str>),
}

impl WatchKey {
    fn exact(path: &SanitizedPath) -> Self {
        Self::Exact(Arc::from(path.as_path()))
    }

    fn folded(path: &SanitizedPath) -> Self {
        Self::Folded(Self::folded_path(path).into())
    }

    fn folded_path(path: &SanitizedPath) -> String {
        // Event ancestors omit trailing separators, even when registration paths include them.
        let normalized_path = path.as_path().components().collect::<std::path::PathBuf>();
        let lossy = normalized_path.to_string_lossy();
        // macOS (APFS/HFS+) compares names normalization-insensitively (NFC vs
        // NFD), and FSEvents can report NFD while a config/LSP supplies NFC, so
        // normalize before folding case. Windows (NTFS) and Linux are
        // normalization-sensitive, so there we only fold case.
        #[cfg(target_os = "macos")]
        {
            use unicode_normalization::UnicodeNormalization as _;
            lossy.chars().nfc().collect::<String>().to_lowercase()
        }
        #[cfg(not(target_os = "macos"))]
        lossy.to_lowercase()
    }

    fn for_registration(path: &SanitizedPath, case_insensitive: bool) -> Self {
        if case_insensitive {
            Self::folded(path)
        } else {
            Self::exact(path)
        }
    }
}

async fn poll_path_until_created(
    native_watcher: Arc<OsWatcher>,
    poll_watcher: Arc<OsWatcher>,
    fs: Arc<dyn Fs>,
    executor: BackgroundExecutor,
    path: Arc<Path>,
    tx: async_channel::Sender<()>,
    pending_path_events: Arc<Mutex<Vec<PathEvent>>>,
    rescan_roots: Arc<Mutex<HashMap<WatchKey, PathBuf>>>,
    registrations: Arc<Mutex<HashMap<WatchKey, FsWatcherRegistration>>>,
    pending_registrations: Arc<Mutex<HashMap<Arc<Path>, Task<()>>>>,
) {
    loop {
        executor.timer(poll_interval()).await;

        if !pending_registrations.lock().contains_key(path.as_ref()) {
            return;
        }

        if !fs.path_exists(&path) {
            continue;
        }

        // Probe case sensitivity now that the path exists, rather than at add
        // time when it didn't.
        let case_insensitive = if fs.is_fake() {
            !fs.is_path_case_sensitive(&path)
        } else {
            smol::unblock({
                let path = path.clone();
                let fs = fs.clone();
                move || !fs.is_path_case_sensitive(&path)
            })
            .await
        };
        let key = WatchKey::for_registration(SanitizedPath::new(&path), case_insensitive);
        {
            let mut roots = rescan_roots.lock();
            roots.remove(&WatchKey::exact(SanitizedPath::new(&path)));
            roots.remove(&WatchKey::folded(SanitizedPath::new(&path)));
            roots.insert(key.clone(), path.to_path_buf());
        }

        if registrations.lock().contains_key(&key) {
            pending_registrations.lock().remove(path.as_ref());
            return;
        }

        let register = {
            let path = path.clone();
            let tx = tx.clone();
            let pending_path_events = pending_path_events.clone();
            let rescan_roots = rescan_roots.clone();
            let fs = fs.clone();
            let native_watcher = native_watcher.clone();
            let poll_watcher = poll_watcher.clone();
            move || {
                register_existing_path(
                    &native_watcher,
                    &poll_watcher,
                    fs.as_ref(),
                    path,
                    case_insensitive,
                    tx,
                    pending_path_events,
                    rescan_roots,
                )
            }
        };
        let registration = if fs.is_fake() {
            register()
        } else {
            smol::unblock(register).await
        };

        match registration {
            Ok(registration) => {
                {
                    let mut pending_registrations = pending_registrations.lock();
                    if pending_registrations.remove(path.as_ref()).is_none() {
                        registration.os_watcher.remove(registration.id);
                        return;
                    }
                    registrations.lock().insert(key, registration);
                }
                enqueue_path_events(
                    &tx,
                    &pending_path_events,
                    vec![
                        PathEvent {
                            path: path.to_path_buf(),
                            kind: Some(PathEventKind::Created),
                        },
                        PathEvent {
                            path: path.to_path_buf(),
                            kind: Some(PathEventKind::Rescan),
                        },
                    ],
                    || rescan_roots.lock().values().cloned().collect(),
                );
                return;
            }
            Err(error) => {
                log::warn!("failed to watch newly-created path {path:?}: {error}; retrying");
            }
        }
    }
}

pub(crate) fn enqueue_path_events(
    tx: &smol::channel::Sender<()>,
    pending_path_events: &Arc<Mutex<Vec<PathEvent>>>,
    mut path_events: Vec<PathEvent>,
    rescan_roots: impl FnOnce() -> Vec<PathBuf>,
) {
    if path_events.is_empty() {
        return;
    }

    path_events.sort();
    let mut pending_paths = pending_path_events.lock();
    let needs_wakeup = pending_paths.is_empty();
    coalesce_pending_rescans(&mut pending_paths, &mut path_events);
    util::extend_sorted(&mut *pending_paths, path_events, usize::MAX, |a, b| {
        a.path.cmp(&b.path)
    });
    if pending_paths.len() > MAX_PENDING_EVENTS
        || pending_paths
            .iter()
            .map(|event| event.path.as_os_str().len())
            .sum::<usize>()
            > MAX_PENDING_BYTES
    {
        *pending_paths = rescan_roots()
            .into_iter()
            .map(|path| PathEvent {
                path,
                kind: Some(PathEventKind::Rescan),
            })
            .collect();
        pending_paths.sort();
        pending_paths.dedup();
    }
    if needs_wakeup {
        match tx.try_send(()) {
            Ok(())
            | Err(async_channel::TrySendError::Full(()))
            | Err(async_channel::TrySendError::Closed(())) => {}
        }
    }
}

fn push_notify_event(
    tx: &smol::channel::Sender<()>,
    pending_path_events: &Arc<Mutex<Vec<PathEvent>>>,
    root_path: &SanitizedPath,
    case_insensitive: bool,
    watched_root: &Path,
    event: &notify::Event,
    rescan_roots: &Mutex<HashMap<WatchKey, PathBuf>>,
) {
    let kind = match event.kind {
        EventKind::Create(_) => Some(PathEventKind::Created),
        EventKind::Modify(_) => Some(PathEventKind::Changed),
        EventKind::Remove(_) => Some(PathEventKind::Removed),
        _ => None,
    };
    let mut path_events = event
        .paths
        .iter()
        .filter_map(|event_path| {
            let event_path = SanitizedPath::new(event_path);
            path_is_under(event_path, root_path, case_insensitive).then(|| PathEvent {
                path: event_path.as_path().to_path_buf(),
                kind,
            })
        })
        .collect::<Vec<_>>();

    if event.need_rescan() {
        // A rescan event names the subtrees that lost sync (FSEvents
        // `MustScanSubDirs`); only a pathless one (inotify queue overflow) means
        // the whole watched root is suspect.
        if path_events.is_empty() {
            path_events.push(PathEvent {
                path: watched_root.to_path_buf(),
                kind: Some(PathEventKind::Rescan),
            });
        } else {
            for path_event in &mut path_events {
                path_event.kind = Some(PathEventKind::Rescan);
            }
        }
    }
    log::trace!("path_events: {:?}", path_events);
    enqueue_path_events(tx, pending_path_events, path_events, || {
        rescan_roots.lock().values().cloned().collect()
    });
}

fn coalesce_pending_rescans(pending_paths: &mut Vec<PathEvent>, path_events: &mut Vec<PathEvent>) {
    if !path_events
        .iter()
        .any(|event| event.kind == Some(PathEventKind::Rescan))
    {
        return;
    }

    let mut new_rescan_paths: Vec<std::path::PathBuf> = path_events
        .iter()
        .filter(|e| e.kind == Some(PathEventKind::Rescan))
        .map(|e| e.path.clone())
        .collect();
    new_rescan_paths.sort_unstable();

    let mut deduped_rescans: Vec<std::path::PathBuf> = Vec::with_capacity(new_rescan_paths.len());
    for path in new_rescan_paths {
        if deduped_rescans
            .iter()
            .any(|ancestor| path != *ancestor && path.starts_with(ancestor))
        {
            continue;
        }
        deduped_rescans.push(path);
    }

    deduped_rescans.retain(|new_path| {
        !pending_paths
            .iter()
            .any(|pending| is_covered_rescan(pending.kind, new_path, &pending.path))
    });

    if !deduped_rescans.is_empty() {
        pending_paths.retain(|pending| {
            !deduped_rescans.iter().any(|rescan_path| {
                pending.path == *rescan_path
                    || is_covered_rescan(pending.kind, &pending.path, rescan_path)
            })
        });
    }

    path_events.retain(|event| {
        event.kind != Some(PathEventKind::Rescan) || deduped_rescans.contains(&event.path)
    });
}

fn is_covered_rescan(kind: Option<PathEventKind>, path: &Path, ancestor: &Path) -> bool {
    kind == Some(PathEventKind::Rescan) && path != ancestor && path.starts_with(ancestor)
}

#[derive(Default, Debug, Copy, Clone, PartialEq, Eq, Hash)]
pub struct WatcherRegistrationId(u64);

struct WatcherRegistrationState {
    callback: Arc<dyn Fn(&notify::Event) + Send + Sync>,
    key: WatchKey,
}

struct PathRegistrationState {
    path: Arc<SanitizedPath>,
    watcher_ids: Vec<WatcherRegistrationId>,
    case_insensitive: bool,
}

#[derive(Default)]
struct WatcherState {
    watchers: HashMap<WatcherRegistrationId, WatcherRegistrationState>,
    paths: FxHashMap<WatchKey, PathRegistrationState>,
    last_registration: WatcherRegistrationId,
}

pub(crate) trait WatchBackend: Send {
    fn watch(&mut self, path: &Path, mode: notify::RecursiveMode) -> notify::Result<()>;
    fn unwatch(&mut self, path: &Path) -> notify::Result<()>;
}

impl<T: notify::Watcher + Send> WatchBackend for T {
    fn watch(&mut self, path: &Path, mode: notify::RecursiveMode) -> notify::Result<()> {
        notify::Watcher::watch(self, path, mode)
    }

    fn unwatch(&mut self, path: &Path) -> notify::Result<()> {
        notify::Watcher::unwatch(self, path)
    }
}

enum NativeCommand {
    Start(gix_notify::asynchronous::Watcher),
    Replace,
    Barrier(async_channel::Sender<()>),
}

fn request_native_replacement(
    commands: &async_channel::Sender<NativeCommand>,
    queued: &AtomicBool,
) -> anyhow::Result<()> {
    if !queued.swap(true, Ordering::AcqRel)
        && let Err(error) = commands.try_send(NativeCommand::Replace)
    {
        queued.store(false, Ordering::Release);
        return Err(error.into());
    }
    Ok(())
}

pub struct OsWatcher {
    kind: OsWatcherKind,
    recursive: bool,
    state: Arc<Mutex<WatcherState>>,
    backend: Mutex<Option<Box<dyn WatchBackend>>>,
    native: bool,
    native_started: Mutex<bool>,
    commands: async_channel::Sender<NativeCommand>,
    replacement_queued: Arc<AtomicBool>,
    event_tx: async_channel::Sender<notify::Result<notify::Event>>,
    overflow: Arc<AtomicBool>,
    _native_task: Task<()>,
    _dispatch_task: Task<()>,
    diagnostics: Arc<diagnostics::DiagnosticRecorder>,
    _executor: BackgroundExecutor,
}

impl OsWatcher {
    pub fn new(kind: OsWatcherKind, executor: BackgroundExecutor) -> Arc<Self> {
        Self::with_backend(kind, executor, None)
    }

    pub(crate) fn with_backend(
        kind: OsWatcherKind,
        executor: BackgroundExecutor,
        backend: Option<Box<dyn WatchBackend>>,
    ) -> Arc<Self> {
        let native = kind == OsWatcherKind::Native && backend.is_none();
        let state = Arc::new(Mutex::new(WatcherState::default()));
        let diagnostics = Arc::new(diagnostics::DiagnosticRecorder::default());
        let (event_tx, event_rx) = async_channel::bounded(MAX_PENDING_EVENTS);
        let overflow = Arc::new(AtomicBool::new(false));
        let dispatch_task = executor.spawn({
            let state = state.clone();
            let overflow = overflow.clone();
            let diagnostics = diagnostics.clone();
            async move {
                let mut rescan_history = diagnostics::RescanHistory::default();
                while let Ok(first) = event_rx.recv().await {
                    if overflow.swap(false, Ordering::AcqRel) {
                        let loss = Event::new(EventKind::Other)
                            .set_flag(notify::event::Flag::Rescan)
                            .set_info("filesystem event delivery overflowed");
                        record_event(kind, &diagnostics, &Ok(loss.clone()));
                        dispatch(kind, &state, Ok(loss));
                    }
                    dispatch_batch(kind, &state, &mut rescan_history, first, &event_rx);
                    smol::future::yield_now().await;
                }
            }
        });
        let (commands, command_rx) = async_channel::unbounded();
        let replacement_queued = Arc::new(AtomicBool::new(false));
        let native_task = executor.spawn(native_notifications(
            state.clone(),
            {
                let state = state.clone();
                let diagnostics = diagnostics.clone();
                move |event| {
                    record_event(OsWatcherKind::Native, &diagnostics, &event);
                    dispatch(OsWatcherKind::Native, &state, event);
                }
            },
            command_rx,
            executor.clone(),
            kind.is_recursive(),
            replacement_queued.clone(),
        ));
        Arc::new(Self {
            kind,
            recursive: kind.is_recursive(),
            state,
            backend: Mutex::new(backend),
            native,
            native_started: Mutex::new(false),
            commands,
            replacement_queued,
            event_tx,
            overflow,
            _native_task: native_task,
            _dispatch_task: dispatch_task,
            diagnostics,
            _executor: executor,
        })
    }

    pub(crate) fn event_sink(
        &self,
    ) -> impl Fn(notify::Result<notify::Event>) + Send + Sync + 'static {
        event_sink(
            self.kind,
            self.event_tx.clone(),
            self.diagnostics.clone(),
            self.overflow.clone(),
        )
    }

    pub(crate) fn is_recursive(&self) -> bool {
        self.recursive
    }

    #[must_use]
    fn add(
        &self,
        path: Arc<Path>,
        case_insensitive: bool,
        callback: impl Fn(&notify::Event) + Send + Sync + 'static,
    ) -> anyhow::Result<WatcherRegistrationId> {
        let path = SanitizedPath::from_arc(path);
        let key = WatchKey::for_registration(&path, case_insensitive);
        let mut state = self.state.lock();
        let is_new = !state.paths.contains_key(&key);
        if is_new {
            self.ensure_backend()?;
            if !self.native {
                self.watch(path.as_path())?;
            }
        }
        let id = state.last_registration;
        state.last_registration = WatcherRegistrationId(
            id.0.checked_add(1)
                .ok_or_else(|| anyhow::anyhow!("watch registration IDs exhausted"))?,
        );
        state.watchers.insert(
            id,
            WatcherRegistrationState {
                callback: Arc::new(callback),
                key: key.clone(),
            },
        );
        state
            .paths
            .entry(key)
            .or_insert_with(|| PathRegistrationState {
                path: path.clone(),
                watcher_ids: Vec::new(),
                case_insensitive,
            })
            .watcher_ids
            .push(id);
        if is_new && self.native {
            request_native_replacement(&self.commands, &self.replacement_queued)?;
            self.record_registration("watch", path.as_path(), &Ok(()));
        }
        Ok(id)
    }

    fn remove(&self, id: WatcherRegistrationId) {
        let mut state = self.state.lock();
        let Some(registration) = state.watchers.remove(&id) else {
            return;
        };
        let Some(path) = state.paths.get_mut(&registration.key) else {
            return;
        };
        path.watcher_ids
            .retain(|registered_id| *registered_id != id);
        if path.watcher_ids.is_empty()
            && let Some(path) = state.paths.remove(&registration.key)
        {
            if self.native {
                request_native_replacement(&self.commands, &self.replacement_queued).log_err();
                self.record_registration("unwatch", path.path.as_path(), &Ok(()));
            } else {
                self.unwatch(path.path.as_path()).log_err();
            }
        }
    }

    async fn synchronize_registrations(&self) -> anyhow::Result<()> {
        if !self.native || !*self.native_started.lock() {
            return Ok(());
        }
        let (sender, receiver) = async_channel::bounded(1);
        self.commands.try_send(NativeCommand::Barrier(sender))?;
        receiver.recv().await?;
        Ok(())
    }

    fn watch(&self, path: &Path) -> anyhow::Result<()> {
        let result = self
            .backend
            .lock()
            .as_mut()
            .ok_or_else(|| anyhow::anyhow!("watcher backend was not initialized"))?
            .watch(
                path,
                if self.recursive {
                    notify::RecursiveMode::Recursive
                } else {
                    notify::RecursiveMode::NonRecursive
                },
            );
        self.record_registration("watch", path, &result);
        result?;
        Ok(())
    }

    fn unwatch(&self, path: &Path) -> anyhow::Result<()> {
        let result = self
            .backend
            .lock()
            .as_mut()
            .ok_or_else(|| anyhow::anyhow!("watcher backend was not initialized"))?
            .unwatch(path);
        self.record_registration("unwatch", path, &result);
        match result {
            Err(error) if !matches!(error.kind, notify::ErrorKind::WatchNotFound) => {
                Err(error.into())
            }
            _ => Ok(()),
        }
    }

    fn record_registration(&self, operation: &str, path: &Path, result: &notify::Result<()>) {
        self.diagnostics.record(|| {
            WatchDiagnosticEvent::new(
                self.kind,
                &if result.is_err() {
                    format!("{operation}_error")
                } else {
                    operation.into()
                },
                &[path.to_owned()],
                result
                    .as_ref()
                    .err()
                    .map(|error| format!("{error:?}"))
                    .unwrap_or_default(),
                false,
            )
        });
    }

    pub(crate) fn ensure_backend(&self) -> anyhow::Result<()> {
        let mut backend = self.backend.lock();
        if self.native {
            let mut started = self.native_started.lock();
            if !*started {
                let watcher =
                    gix_notify::asynchronous::Watcher::new(gix_notify::Options::default())
                        .map_err(anyhow::Error::from)?;
                self.commands.try_send(NativeCommand::Start(watcher))?;
                *started = true;
            }
        } else if backend.is_none() {
            // gix-notify has no polling backend for mounts that cannot deliver native events.
            let config = notify::Config::default().with_poll_interval(*POLL_INTERVAL);
            *backend = Some(Box::new(notify::PollWatcher::new(
                self.event_sink(),
                config,
            )?));
        }
        Ok(())
    }
}

fn record_event(
    kind: OsWatcherKind,
    diagnostics: &diagnostics::DiagnosticRecorder,
    event: &notify::Result<notify::Event>,
) {
    diagnostics.record(|| match event {
        Ok(event) => WatchDiagnosticEvent::from_notify_event(kind, event),
        Err(error) => {
            WatchDiagnosticEvent::new(kind, "error", &error.paths, format!("{error:?}"), false)
        }
    });
}

fn event_sink(
    kind: OsWatcherKind,
    event_tx: async_channel::Sender<notify::Result<notify::Event>>,
    diagnostics: Arc<diagnostics::DiagnosticRecorder>,
    overflow: Arc<AtomicBool>,
) -> impl Fn(notify::Result<notify::Event>) + Send + Sync + 'static {
    move |event| {
        record_event(kind, &diagnostics, &event);
        if matches!(
            event,
            Ok(Event {
                kind: EventKind::Access(_),
                ..
            })
        ) {
            return;
        }
        match event_tx.try_send(event) {
            Ok(()) | Err(async_channel::TrySendError::Closed(_)) => {}
            Err(async_channel::TrySendError::Full(_)) => overflow.store(true, Ordering::Release),
        }
    }
}

#[derive(PartialEq, Eq)]
struct NativeAlias {
    physical: PathBuf,
    logical: PathBuf,
    case_insensitive: bool,
    rescan: bool,
}

fn native_watch_set(
    paths: Vec<(PathBuf, bool)>,
    recursive: bool,
) -> (Vec<gix_notify::Watch>, Vec<NativeAlias>, bool) {
    let mut watches = Vec::new();
    let mut aliases = Vec::new();
    let mut needs_recheck = false;
    for (logical, case_insensitive) in paths {
        for ancestor in logical.ancestors() {
            if std::fs::symlink_metadata(ancestor).is_ok_and(|metadata| metadata.is_symlink()) {
                // Retargeting can leave the previous physical target and its native watches healthy.
                needs_recheck = true;
                if let Some(parent) = ancestor.parent()
                    && parent.parent().is_some()
                    && let Ok(physical_parent) = std::fs::canonicalize(parent)
                    && let Some(name) = ancestor.file_name()
                {
                    let physical_parent = SanitizedPath::new(&physical_parent).as_path().to_owned();
                    watches.push(gix_notify::Watch {
                        path: physical_parent.clone(),
                        recursive: false,
                    });
                    aliases.push(NativeAlias {
                        physical: physical_parent.join(name),
                        logical: logical.clone(),
                        case_insensitive,
                        rescan: true,
                    });
                }
            }
        }
        match std::fs::canonicalize(&logical) {
            Ok(physical) => {
                let physical = SanitizedPath::new(&physical).as_path().to_owned();
                if physical.is_dir() {
                    watches.push(gix_notify::Watch {
                        path: physical.clone(),
                        recursive,
                    });
                }
                // Parent coverage observes atomic file replacement and root recreation.
                if let Some(parent) = physical.parent() {
                    watches.push(gix_notify::Watch {
                        path: parent.to_owned(),
                        recursive: false,
                    });
                }
                aliases.push(NativeAlias {
                    physical,
                    logical,
                    case_insensitive,
                    rescan: false,
                });
            }
            Err(error) => {
                needs_recheck = true;
                if error.kind() != std::io::ErrorKind::NotFound {
                    log::warn!(
                        "failed to resolve watched path {}: {error}",
                        logical.display()
                    );
                }
                if let Some((ancestor, physical)) =
                    logical.ancestors().skip(1).find_map(|ancestor| {
                        std::fs::canonicalize(ancestor)
                            .ok()
                            .filter(|physical| physical.is_dir())
                            .map(|physical| (ancestor, physical))
                    })
                {
                    let physical = SanitizedPath::new(&physical).as_path().to_owned();
                    watches.push(gix_notify::Watch {
                        path: physical.clone(),
                        recursive: false,
                    });
                    if let Ok(suffix) = logical.strip_prefix(ancestor) {
                        aliases.push(NativeAlias {
                            physical: physical.join(suffix),
                            logical,
                            case_insensitive,
                            rescan: false,
                        });
                    }
                }
            }
        }
    }
    (watches, aliases, needs_recheck)
}

fn logical_event(event: gix_notify::Event, aliases: &[NativeAlias]) -> notify::Event {
    use notify::event::{CreateKind, ModifyKind, RemoveKind, RenameMode};
    let kind = match event.kind {
        gix_notify::EventKind::Any => EventKind::Modify(ModifyKind::Any),
        gix_notify::EventKind::Create => EventKind::Create(CreateKind::Any),
        gix_notify::EventKind::Modify => EventKind::Modify(ModifyKind::Any),
        gix_notify::EventKind::Remove => EventKind::Remove(RemoveKind::Any),
        gix_notify::EventKind::Rename => EventKind::Modify(ModifyKind::Name(RenameMode::Any)),
    };
    let mut paths = Vec::new();
    let mut rescan = false;
    for physical in event.paths {
        let physical = SanitizedPath::new(&physical);
        for alias in aliases {
            let root = SanitizedPath::new(&alias.physical);
            if path_is_under(physical, root, alias.case_insensitive) {
                if alias.rescan {
                    paths.push(alias.logical.clone());
                    rescan = true;
                    continue;
                }
                let suffix: PathBuf = physical
                    .as_path()
                    .components()
                    .skip(root.as_path().components().count())
                    .collect();
                paths.push(alias.logical.join(suffix));
            }
        }
    }
    paths.sort();
    paths.dedup();
    let event = notify::Event {
        paths,
        ..notify::Event::new(kind)
    };
    if rescan {
        event.set_flag(notify::event::Flag::Rescan)
    } else {
        event
    }
}

enum NativeActivity {
    Command(NativeCommand),
    Ready,
    Retry,
    Stopped,
}

async fn next_native_activity(
    notification: impl Future<Output = ()>,
    commands: &async_channel::Receiver<NativeCommand>,
    retry: &mut Option<Task<()>>,
    recovering: bool,
    replacement_queued: &AtomicBool,
) -> NativeActivity {
    let notification = async {
        if recovering {
            // Repeated backend diagnostics must not defeat the recovery backoff.
            futures::future::pending::<()>().await;
        }
        notification.await;
    }
    .fuse();
    futures::pin_mut!(notification);
    loop {
        let command = commands.recv().fuse();
        let retry = async {
            match retry {
                Some(timer) => timer.await,
                None => futures::future::pending().await,
            }
        }
        .fuse();
        futures::pin_mut!(command, retry);
        let activity = futures::select! {
            () = notification => NativeActivity::Ready,
            command = command => match command {
                Ok(command) => NativeActivity::Command(command),
                Err(_) => NativeActivity::Stopped,
            },
            () = retry => NativeActivity::Retry,
        };
        if matches!(&activity, NativeActivity::Command(NativeCommand::Replace)) {
            replacement_queued.store(false, Ordering::Release);
            if recovering {
                continue;
            }
        }
        return activity;
    }
}

async fn native_notifications(
    state: Arc<Mutex<WatcherState>>,
    emit: impl Fn(notify::Result<notify::Event>) + Send + Sync + 'static,
    commands: async_channel::Receiver<NativeCommand>,
    executor: BackgroundExecutor,
    recursive: bool,
    replacement_queued: Arc<AtomicBool>,
) {
    let mut watcher: Option<gix_notify::asynchronous::Watcher> = None;
    let mut aliases = Vec::new();
    let mut retry: Option<Task<()>> = None;
    let mut restart = false;
    let mut recovering = false;
    loop {
        let activity = if let Some(watcher) = &watcher {
            next_native_activity(
                watcher.ready(),
                &commands,
                &mut retry,
                recovering,
                &replacement_queued,
            )
            .await
        } else {
            match commands.recv().await {
                Ok(command) => NativeActivity::Command(command),
                Err(_) => NativeActivity::Stopped,
            }
        };
        let replace = match activity {
            NativeActivity::Command(NativeCommand::Start(started)) => {
                watcher = Some(started);
                false
            }
            NativeActivity::Command(NativeCommand::Barrier(reply)) => {
                match reply.try_send(()) {
                    Ok(()) | Err(async_channel::TrySendError::Closed(())) => {}
                    Err(error) => {
                        log::error!("failed to acknowledge filesystem registration: {error}")
                    }
                }
                false
            }
            NativeActivity::Command(NativeCommand::Replace) => true,
            NativeActivity::Retry => {
                retry = None;
                true
            }
            NativeActivity::Ready => {
                let Some(watcher) = &mut watcher else {
                    continue;
                };
                let batch = watcher.drain(gix_notify::Budget::default());
                smol::future::yield_now().await;
                let mut refresh = false;
                for event in batch.events {
                    let event = logical_event(event, &aliases);
                    if !event.paths.is_empty() {
                        refresh |= event.need_rescan();
                        emit(Ok(event));
                    }
                }
                if let Some(error) = batch.error {
                    emit(Err(notify::Error::generic(&format!("{error:?}"))));
                    recovering = true;
                    restart = true;
                    retry.get_or_insert_with(|| executor.timer(Duration::from_secs(1)));
                }
                if let Some(loss) = batch.loss {
                    emit(Ok(Event::new(EventKind::Other)
                        .set_flag(notify::event::Flag::Rescan)
                        .set_info(&format!("gix-notify: {loss:?}"))));
                    restart |= loss == gix_notify::Loss::BackendStopped;
                    match loss {
                        gix_notify::Loss::BackendError | gix_notify::Loss::BackendStopped => {
                            recovering = true;
                            restart = true;
                            retry.get_or_insert_with(|| executor.timer(Duration::from_secs(1)));
                            false
                        }
                        gix_notify::Loss::WatchSetChanged => refresh && !recovering,
                        _ => !recovering && (refresh || retry.is_none()),
                    }
                } else {
                    refresh && !recovering
                }
            }
            NativeActivity::Stopped => break,
        };
        if !replace {
            continue;
        }
        if restart {
            match gix_notify::asynchronous::Watcher::new(gix_notify::Options::default()) {
                Ok(started) => {
                    watcher = Some(started);
                    restart = false;
                }
                Err(error) => {
                    emit(Err(notify::Error::generic(&format!("{error:?}"))));
                    retry.get_or_insert_with(|| executor.timer(Duration::from_secs(1)));
                    continue;
                }
            }
        }
        let paths = state
            .lock()
            .paths
            .values()
            .map(|path| (path.path.as_path().to_owned(), path.case_insensitive))
            .collect();
        let (watches, refreshed_aliases, needs_recheck) =
            smol::unblock(move || native_watch_set(paths, recursive)).await;
        let identity_changed = !aliases.is_empty() && aliases != refreshed_aliases;
        aliases = refreshed_aliases;
        let Some(watcher) = &mut watcher else {
            continue;
        };
        let result = match watcher.replace(watches) {
            Ok(operation) => operation.await,
            Err(error) => Err(error),
        };
        if identity_changed {
            emit(Ok(Event::new(EventKind::Other)
                .set_flag(notify::event::Flag::Rescan)
                .set_info("watched path identity changed")));
        }
        recovering = result.is_err();
        if let Err(error) = &result {
            restart = true;
            emit(Err(notify::Error::generic(&format!("{error:?}"))));
            emit(Ok(
                Event::new(EventKind::Other).set_flag(notify::event::Flag::Rescan)
            ));
        }
        if needs_recheck || result.is_err() {
            retry.get_or_insert_with(|| executor.timer(Duration::from_secs(1)));
        } else {
            retry = None;
        }
    }
}

fn dispatch(
    kind: OsWatcherKind,
    state: &Mutex<WatcherState>,
    event: notify::Result<notify::Event>,
) {
    let event = match event {
        Ok(event) => event,
        Err(error) => {
            log::warn!("{kind:?} watcher error: {error}");
            return;
        }
    };
    let callbacks = {
        let state = state.lock();
        if event.need_rescan() {
            state
                .watchers
                .values()
                .map(|watcher| watcher.callback.clone())
                .collect::<Vec<_>>()
        } else {
            let mut ids = Vec::new();
            for path in &event.paths {
                for ancestor in SanitizedPath::new(path).as_path().ancestors() {
                    let ancestor = SanitizedPath::unchecked_new(ancestor);
                    if let Some(registration) = state.paths.get(&WatchKey::exact(ancestor)) {
                        ids.extend_from_slice(&registration.watcher_ids);
                    }
                    #[cfg(not(target_os = "macos"))]
                    if let Some(registration) = state.paths.get(&WatchKey::folded(ancestor)) {
                        ids.extend_from_slice(&registration.watcher_ids);
                    }
                }
                #[cfg(target_os = "macos")]
                {
                    let folded_path = WatchKey::folded_path(SanitizedPath::new(path));
                    for ancestor in Path::new(&folded_path).ancestors() {
                        let key = WatchKey::Folded(Arc::from(ancestor.to_string_lossy().as_ref()));
                        if let Some(registration) = state.paths.get(&key) {
                            ids.extend_from_slice(&registration.watcher_ids);
                        }
                    }
                }
            }
            ids.sort_unstable_by_key(|id| id.0);
            ids.dedup();
            ids.into_iter()
                .filter_map(|id| state.watchers.get(&id))
                .map(|watcher| watcher.callback.clone())
                .collect()
        }
    };
    for callback in callbacks {
        callback(&event);
    }
}

fn dispatch_batch(
    kind: OsWatcherKind,
    state: &Mutex<WatcherState>,
    rescan_history: &mut diagnostics::RescanHistory,
    first: notify::Result<notify::Event>,
    event_rx: &async_channel::Receiver<notify::Result<notify::Event>>,
) {
    let mut rescan_dispatched = false;
    // A busy backend must give other tasks a turn before its queue becomes empty.
    for event in std::iter::once(first)
        .chain(std::iter::from_fn(|| event_rx.try_recv().ok()))
        .take(MAX_DISPATCH_BATCH_SIZE)
    {
        if event
            .as_ref()
            .is_ok_and(|event| event.need_rescan() && event.paths.is_empty())
        {
            if rescan_dispatched {
                continue;
            }
            rescan_dispatched = true;
        }
        if let Ok(event) = &event
            && let Some(report) = rescan_history.record(event)
        {
            log::debug!("{kind:?} filesystem watcher requested rescan: {report}");
        }
        dispatch(kind, state, event);
    }
}

const MAX_PENDING_EVENTS: usize = 4096;
const MAX_PENDING_BYTES: usize = 4 * 1024 * 1024;

static POLL_INTERVAL: LazyLock<Duration> = LazyLock::new(|| {
    let poll_ms: u64 = std::env::var("ZED_FILE_WATCHER_POLL_MS")
        .ok()
        .and_then(|value| value.parse().ok())
        .unwrap_or(2000)
        .clamp(500, 30000);
    Duration::from_millis(poll_ms)
});

pub fn poll_interval() -> Duration {
    *POLL_INTERVAL
}

#[cfg(test)]
mod tests {
    use super::*;
    use futures::StreamExt as _;
    use gpui::TestAppContext;
    use notify::event::{Flag, ModifyKind};

    struct Backend;

    impl WatchBackend for Backend {
        fn watch(&mut self, _: &Path, _: notify::RecursiveMode) -> notify::Result<()> {
            Ok(())
        }

        fn unwatch(&mut self, _: &Path) -> notify::Result<()> {
            Ok(())
        }
    }

    fn watcher(cx: &TestAppContext) -> Arc<OsWatcher> {
        OsWatcher::with_backend(
            OsWatcherKind::Native,
            cx.executor(),
            Some(Box::new(Backend)),
        )
    }

    #[gpui::test]
    fn dispatch_batches_are_bounded_without_losing_or_reordering_events(cx: &TestAppContext) {
        let watcher = watcher(cx);
        let fired = Arc::new(Mutex::new(Vec::new()));
        watcher
            .add(Path::new("/root").into(), false, {
                let fired = fired.clone();
                move |event| fired.lock().push(event.paths.first().cloned())
            })
            .expect("register watch");
        let (sender, receiver) = async_channel::unbounded();
        for _ in 0..MAX_DISPATCH_BATCH_SIZE {
            sender
                .try_send(Ok(Event::new(EventKind::Other).set_flag(Flag::Rescan)))
                .expect("enqueue rescan");
        }
        let expected = (0..MAX_DISPATCH_BATCH_SIZE * 2 + 3)
            .map(|index| Some(PathBuf::from(format!("/root/{index}"))))
            .collect::<Vec<_>>();
        for path in expected.iter().flatten() {
            sender
                .try_send(Ok(
                    Event::new(EventKind::Modify(ModifyKind::Any)).add_path(path.clone())
                ))
                .expect("enqueue event");
        }
        let mut history = diagnostics::RescanHistory::default();
        dispatch_batch(
            OsWatcherKind::Native,
            &watcher.state,
            &mut history,
            receiver.try_recv().expect("first rescan"),
            &receiver,
        );
        assert_eq!(fired.lock().as_slice(), &[None]);
        assert_eq!(receiver.len(), expected.len());
        fired.lock().clear();
        dispatch_batch(
            OsWatcherKind::Native,
            &watcher.state,
            &mut history,
            receiver.try_recv().expect("first event"),
            &receiver,
        );
        assert_eq!(
            fired.lock().as_slice(),
            &expected[..MAX_DISPATCH_BATCH_SIZE]
        );
        assert_eq!(receiver.len(), expected.len() - MAX_DISPATCH_BATCH_SIZE);
        while let Ok(first) = receiver.try_recv() {
            dispatch_batch(
                OsWatcherKind::Native,
                &watcher.state,
                &mut history,
                first,
                &receiver,
            );
        }
        assert_eq!(fired.lock().as_slice(), expected.as_slice());
    }

    #[gpui::test]
    fn overlapping_subscriptions_survive_parent_removal(cx: &TestAppContext) {
        let watcher = watcher(cx);
        let fired = Arc::new(Mutex::new(Vec::new()));
        let mut registrations = Vec::new();
        for path in ["/root", "/root/child", "/other"] {
            let fired = fired.clone();
            registrations.push(
                watcher
                    .add(Path::new(path).into(), false, move |_| {
                        fired.lock().push(path)
                    })
                    .expect("register watch"),
            );
        }
        let event =
            || Event::new(EventKind::Modify(ModifyKind::Any)).add_path("/root/child/file".into());
        dispatch(OsWatcherKind::Native, &watcher.state, Ok(event()));
        fired.lock().sort();
        assert_eq!(*fired.lock(), ["/root", "/root/child"]);
        if let Some(parent) = registrations.first() {
            watcher.remove(*parent);
        }
        fired.lock().clear();
        dispatch(OsWatcherKind::Native, &watcher.state, Ok(event()));
        assert_eq!(*fired.lock(), ["/root/child"]);
    }

    #[gpui::test]
    fn loss_does_not_discard_adjacent_notifications(cx: &TestAppContext) {
        let watcher = watcher(cx);
        let fired = Arc::new(Mutex::new(Vec::new()));
        watcher
            .add(Path::new("/root").into(), false, {
                let fired = fired.clone();
                move |event| fired.lock().push(event.need_rescan())
            })
            .expect("register watch");
        let (sender, receiver) = async_channel::unbounded();
        sender
            .try_send(Ok(Event::new(EventKind::Other).set_flag(Flag::Rescan)))
            .expect("queue duplicate loss");
        sender
            .try_send(Ok(
                Event::new(EventKind::Modify(ModifyKind::Any)).add_path("/root/file".into())
            ))
            .expect("queue file notification");
        dispatch_batch(
            OsWatcherKind::Native,
            &watcher.state,
            &mut diagnostics::RescanHistory::default(),
            Ok(Event::new(EventKind::Other).set_flag(Flag::Rescan)),
            &receiver,
        );
        assert_eq!(*fired.lock(), [true, false]);
    }

    #[test]
    fn physical_paths_are_mapped_to_each_logical_alias() {
        let event = logical_event(
            gix_notify::Event {
                paths: vec!["/physical/child/file".into(), "/outside/file".into()],
                kind: gix_notify::EventKind::Rename,
                path_kind: gix_notify::PathKind::File,
            },
            &[
                NativeAlias {
                    physical: "/physical".into(),
                    logical: "/symlink".into(),
                    case_insensitive: false,
                    rescan: false,
                },
                NativeAlias {
                    physical: "/physical/child".into(),
                    logical: "/another".into(),
                    case_insensitive: false,
                    rescan: false,
                },
            ],
        );
        assert_eq!(
            event.paths,
            [
                PathBuf::from("/another/file"),
                PathBuf::from("/symlink/child/file")
            ]
        );
        assert!(matches!(event.kind, EventKind::Modify(ModifyKind::Name(_))));
    }

    #[test]
    fn native_file_watches_cover_atomic_replacement_through_parent() {
        let directory = tempfile::tempdir().expect("create temporary directory");
        let file = directory.path().join("file");
        std::fs::write(&file, b"contents").expect("create watched file");
        let file = std::fs::canonicalize(&file).expect("resolve watched file");
        let file = SanitizedPath::new(&file).as_path().to_owned();
        let (watches, aliases, missing) = native_watch_set(vec![(file.clone(), false)], false);
        assert!(!missing);
        assert_eq!(watches.len(), 1);
        let parent = std::fs::canonicalize(directory.path()).expect("resolve parent");
        let parent = SanitizedPath::new(&parent).as_path().to_owned();
        assert_eq!(watches.first().map(|watch| &watch.path), Some(&parent));
        assert!(watches.iter().all(|watch| !watch.recursive));
        assert_eq!(aliases.first().map(|alias| &alias.logical), Some(&file));

        std::fs::remove_file(&file).expect("remove watched file");
        let (watches, _, missing) = native_watch_set(vec![(file, false)], false);
        assert!(missing);
        assert_eq!(watches.first().map(|watch| &watch.path), Some(&parent));
    }

    #[test]
    fn native_directory_watches_preserve_requested_recursion() {
        let directory = tempfile::tempdir().expect("create temporary directory");
        let root = std::fs::canonicalize(directory.path()).expect("resolve watched directory");
        let root = SanitizedPath::new(&root).as_path().to_owned();
        std::fs::create_dir_all(root.join("ignored/deep")).expect("create unloaded directories");
        for recursive in [false, true] {
            let (watches, _, _) = native_watch_set(vec![(root.clone(), false)], recursive);
            assert_eq!(watches.len(), 2);
            assert_eq!(
                watches
                    .iter()
                    .find(|watch| watch.path == root)
                    .map(|watch| watch.recursive),
                Some(recursive)
            );
            assert!(
                watches
                    .iter()
                    .filter(|watch| watch.path != root)
                    .all(|watch| !watch.recursive)
            );
        }
    }

    #[gpui::test]
    fn synthetic_backends_use_the_platform_recursion_contract(cx: &TestAppContext) {
        struct Backend {
            modes: Arc<Mutex<Vec<notify::RecursiveMode>>>,
        }
        impl WatchBackend for Backend {
            fn watch(&mut self, _: &Path, mode: notify::RecursiveMode) -> notify::Result<()> {
                self.modes.lock().push(mode);
                Ok(())
            }

            fn unwatch(&mut self, _: &Path) -> notify::Result<()> {
                Ok(())
            }
        }

        for kind in [OsWatcherKind::Native, OsWatcherKind::Poll] {
            let modes: Arc<Mutex<Vec<notify::RecursiveMode>>> = Default::default();
            let watcher = OsWatcher::with_backend(
                kind,
                cx.executor(),
                Some(Box::new(Backend {
                    modes: modes.clone(),
                })),
            );
            watcher
                .add(Path::new(util::path!("/root")).into(), false, |_| {})
                .expect("register synthetic watch");
            let expected = if kind == OsWatcherKind::Poll
                || gix_notify::asynchronous::Watcher::supports_native_recursive_watches()
            {
                notify::RecursiveMode::Recursive
            } else {
                notify::RecursiveMode::NonRecursive
            };
            assert_eq!(modes.lock().last(), Some(&expected));
        }
    }

    #[gpui::test]
    async fn missing_fake_path_is_registered_after_creation(cx: &mut TestAppContext) {
        let fs = crate::FakeFs::new(cx.executor());
        let root = Path::new(util::path!("/root"));
        fs.create_dir(root).await.expect("create parent");
        let file = root.join("file");
        let (mut events, _watcher) = fs.watch(&file, Duration::ZERO).await;
        fs.write(&file, b"contents")
            .await
            .expect("create watched file");
        cx.run_until_parked();
        cx.executor().advance_clock(poll_interval());
        let batch = events.next().await.expect("receive creation notification");
        assert!(batch.iter().any(|event| event.path == file));
        assert_eq!(fs.registered_watch_paths(), [file]);
    }

    #[gpui::test]
    async fn cancelling_next_preserves_pending_collection(cx: &mut TestAppContext) {
        let fs = crate::FakeFs::new(cx.executor());
        let root = Path::new(util::path!("/root"));
        fs.create_dir(root).await.expect("create root");
        let (mut events, _watcher) = watch(
            fs.clone(),
            fs.native_watcher.clone(),
            fs.poll_watcher.clone(),
            cx.executor(),
            root,
            Duration::from_millis(100),
        )
        .await;
        let file = root.join("file");
        fs.write(&file, b"contents")
            .await
            .expect("change watched file");
        cx.run_until_parked();
        assert!(events.next().now_or_never().is_none());
        cx.run_until_parked();
        cx.executor().advance_clock(Duration::from_millis(100));
        let batch = events
            .next()
            .await
            .expect("receive collected notifications");
        assert!(batch.iter().any(|event| event.path == file));
    }

    #[gpui::test]
    async fn replacement_bursts_preserve_updates_and_barrier_order() {
        let (sender, receiver) = async_channel::unbounded();
        let queued = AtomicBool::new(false);
        for _ in 0..1_000 {
            request_native_replacement(&sender, &queued).expect("queue initial replacement");
        }
        let (barrier_sender, barrier_receiver) = async_channel::bounded(1);
        sender
            .try_send(NativeCommand::Barrier(barrier_sender))
            .expect("queue registration barrier");
        assert_eq!(receiver.len(), 2);
        let mut retry = None;
        let activity = next_native_activity(
            futures::future::pending(),
            &receiver,
            &mut retry,
            false,
            &queued,
        )
        .await;
        assert!(matches!(
            activity,
            NativeActivity::Command(NativeCommand::Replace)
        ));
        assert!(!queued.load(Ordering::Acquire));

        for _ in 0..1_000 {
            request_native_replacement(&sender, &queued).expect("queue update during registration");
        }
        assert_eq!(receiver.len(), 2);
        match next_native_activity(
            futures::future::pending(),
            &receiver,
            &mut retry,
            false,
            &queued,
        )
        .await
        {
            NativeActivity::Command(NativeCommand::Barrier(reply)) => reply
                .try_send(())
                .expect("acknowledge completed registration"),
            _ => panic!("later replacements overtook the registration barrier"),
        }
        barrier_receiver
            .recv()
            .await
            .expect("receive registration acknowledgement");
        assert!(queued.load(Ordering::Acquire));
        let activity = next_native_activity(
            futures::future::pending(),
            &receiver,
            &mut retry,
            false,
            &queued,
        )
        .await;
        assert!(matches!(
            activity,
            NativeActivity::Command(NativeCommand::Replace)
        ));
        assert!(!queued.load(Ordering::Acquire));
        assert!(receiver.is_empty());
    }

    #[gpui::test]
    async fn stopped_backend_waits_for_recovery_and_coalesces_replacements(
        cx: &mut TestAppContext,
    ) {
        let (sender, receiver) = async_channel::unbounded();
        let queued = AtomicBool::new(false);
        for _ in 0..3 {
            request_native_replacement(&sender, &queued).expect("queue replacement");
        }
        let mut retry = Some(cx.executor().timer(Duration::from_secs(1)));
        let activity = next_native_activity(
            futures::future::ready(()),
            &receiver,
            &mut retry,
            true,
            &queued,
        );
        futures::pin_mut!(activity);
        assert!(activity.as_mut().now_or_never().is_none());
        assert!(!queued.load(Ordering::Acquire));
        for _ in 0..3 {
            request_native_replacement(&sender, &queued)
                .expect("queue replacement during recovery");
        }
        assert!(activity.as_mut().now_or_never().is_none());
        assert!(!queued.load(Ordering::Acquire));
        cx.run_until_parked();
        cx.executor().advance_clock(Duration::from_millis(999));
        cx.run_until_parked();
        assert!(activity.as_mut().now_or_never().is_none());
        cx.executor().advance_clock(Duration::from_millis(1));
        assert!(matches!(activity.await, NativeActivity::Retry));
        assert!(receiver.try_recv().is_err());
        request_native_replacement(&sender, &queued).expect("queue replacement after recovery");
        assert!(matches!(receiver.try_recv(), Ok(NativeCommand::Replace)));
    }

    #[test]
    fn pending_overflow_collapses_to_registered_roots_with_one_wakeup() {
        let pending: Arc<Mutex<Vec<PathEvent>>> = Default::default();
        let (sender, receiver) = async_channel::bounded(1);
        let events = (0..=MAX_PENDING_EVENTS)
            .map(|index| PathEvent {
                path: PathBuf::from(format!("/root/file-{index}")),
                kind: Some(PathEventKind::Changed),
            })
            .collect();
        enqueue_path_events(&sender, &pending, events, || {
            vec!["/root".into(), "/outside/file".into()]
        });
        assert_eq!(pending.lock().len(), 2);
        assert!(
            pending
                .lock()
                .iter()
                .all(|event| event.kind == Some(PathEventKind::Rescan))
        );
        receiver.try_recv().expect("receive overflow wakeup");
        assert!(receiver.try_recv().is_err());
    }

    #[gpui::test]
    fn synthetic_transport_overflow_requests_rescan(cx: &TestAppContext) {
        let watcher = watcher(cx);
        let rescans = Arc::new(Mutex::new(0));
        watcher
            .add(Path::new("/root").into(), false, {
                let rescans = rescans.clone();
                move |event| {
                    if event.need_rescan() {
                        *rescans.lock() += 1;
                    }
                }
            })
            .expect("register root");
        let sink = watcher.event_sink();
        for index in 0..=MAX_PENDING_EVENTS {
            sink(Ok(Event::new(EventKind::Modify(ModifyKind::Any))
                .add_path(PathBuf::from(format!("/root/file-{index}")))));
        }
        cx.run_until_parked();
        assert_eq!(*rescans.lock(), 1);
    }

    #[test]
    fn case_folding_keeps_component_boundaries_and_exact_paths_distinct() {
        let root = SanitizedPath::new(Path::new("/Root"));
        assert!(path_is_under(
            SanitizedPath::new(Path::new("/root/file")),
            root,
            true
        ));
        assert!(!path_is_under(
            SanitizedPath::new(Path::new("/root-other/file")),
            root,
            true
        ));
        assert_ne!(
            WatchKey::exact(root),
            WatchKey::exact(SanitizedPath::new(Path::new("/root")))
        );
        assert_eq!(
            WatchKey::folded(root),
            WatchKey::folded(SanitizedPath::new(Path::new("/root")))
        );
    }
}
