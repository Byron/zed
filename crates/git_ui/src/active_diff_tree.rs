use collections::{BTreeMap, HashMap, HashSet};
use git::{
    repository::RepoPath,
    status::{DiffStat, FileStatus},
};
use gpui::SharedString;
use util::paths::PathStyle;

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ActiveDiffEntry {
    pub(crate) repo_path: RepoPath,
    pub(crate) status: FileStatus,
    pub(crate) diff_stat: Option<DiffStat>,
}

impl ActiveDiffEntry {
    pub(crate) fn display_name(&self, path_style: PathStyle) -> String {
        self.repo_path
            .file_name()
            .map(|name| name.to_owned())
            .unwrap_or_else(|| self.repo_path.display(path_style).to_string())
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ActiveDiffFileEntry {
    pub(crate) entry: ActiveDiffEntry,
    pub(crate) depth: usize,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) struct ActiveDiffDirectoryEntry {
    pub(crate) path: RepoPath,
    pub(crate) name: SharedString,
    pub(crate) depth: usize,
    pub(crate) expanded: bool,
    pub(crate) diff_stat: Option<DiffStat>,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(crate) enum ActiveDiffListEntry {
    File(ActiveDiffFileEntry),
    Directory(ActiveDiffDirectoryEntry),
}

impl ActiveDiffListEntry {
    pub(crate) fn depth(&self) -> usize {
        match self {
            ActiveDiffListEntry::File(entry) => entry.depth,
            ActiveDiffListEntry::Directory(entry) => entry.depth,
        }
    }
}

#[derive(Clone, Debug, PartialEq, Eq)]
enum SelectedEntryIdentity {
    File(RepoPath),
    Directory(RepoPath),
}

#[derive(Default)]
pub(crate) struct ActiveDiffTree {
    source_entries: Vec<ActiveDiffEntry>,
    entries: Vec<ActiveDiffListEntry>,
    visible_indices: Vec<usize>,
    expanded_dirs: HashMap<RepoPath, bool>,
    directory_descendants: HashMap<RepoPath, Vec<ActiveDiffEntry>>,
    entries_indices: HashMap<RepoPath, usize>,
    selected_entry: Option<usize>,
}

impl ActiveDiffTree {
    pub(crate) fn clear(&mut self) {
        self.source_entries.clear();
        self.entries.clear();
        self.visible_indices.clear();
        self.directory_descendants.clear();
        self.entries_indices.clear();
        self.selected_entry = None;
    }

    pub(crate) fn set_entries(&mut self, entries: Vec<ActiveDiffEntry>) {
        if self.source_entries == entries {
            return;
        }

        let selected_entry = self.selected_entry_identity();
        self.source_entries = entries;
        self.rebuild();
        self.selected_entry = selected_entry
            .as_ref()
            .and_then(|identity| self.index_for_selected_entry_identity(identity));
    }

    pub(crate) fn entries(&self) -> &[ActiveDiffListEntry] {
        &self.entries
    }

    pub(crate) fn visible_indices(&self) -> &[usize] {
        &self.visible_indices
    }

    pub(crate) fn is_empty(&self) -> bool {
        self.source_entries.is_empty()
    }

    pub(crate) fn selected_entry(&self) -> Option<usize> {
        self.selected_entry
    }

    pub(crate) fn select_first(&mut self) {
        self.selected_entry = self.visible_indices.first().copied();
    }

    pub(crate) fn select_last(&mut self) {
        self.selected_entry = self.visible_indices.last().copied();
    }

    pub(crate) fn select_previous(&mut self) {
        let Some(selected_entry) = self.selected_entry else {
            return;
        };
        let Some(visible_index) = self
            .visible_indices
            .iter()
            .position(|&index| index == selected_entry)
        else {
            return;
        };
        self.selected_entry = self
            .visible_indices
            .get(visible_index.saturating_sub(1))
            .copied();
    }

    pub(crate) fn select_next(&mut self) {
        let Some(selected_entry) = self.selected_entry else {
            return;
        };
        let Some(visible_index) = self
            .visible_indices
            .iter()
            .position(|&index| index == selected_entry)
        else {
            return;
        };
        if let Some(index) = self.visible_indices.get(visible_index.saturating_add(1)) {
            self.selected_entry = Some(*index);
        }
    }

    pub(crate) fn select_entry(&mut self, index: usize) {
        if self.visible_indices.contains(&index) {
            self.selected_entry = Some(index);
        }
    }

    pub(crate) fn selected_file(&self) -> Option<&ActiveDiffEntry> {
        self.selected_entry
            .and_then(|index| self.entries.get(index))
            .and_then(|entry| match entry {
                ActiveDiffListEntry::File(entry) => Some(&entry.entry),
                ActiveDiffListEntry::Directory(_) => None,
            })
    }

    pub(crate) fn selected_directory(&self) -> Option<&ActiveDiffDirectoryEntry> {
        self.selected_entry
            .and_then(|index| self.entries.get(index))
            .and_then(|entry| match entry {
                ActiveDiffListEntry::Directory(entry) => Some(entry),
                ActiveDiffListEntry::File(_) => None,
            })
    }

    pub(crate) fn select_path(&mut self, repo_path: &RepoPath) -> bool {
        let mut needs_rebuild = false;
        let mut current_dir = repo_path.parent().map(RepoPath::from_rel_path);
        while let Some(dir) = current_dir {
            let parent = dir.parent().map(RepoPath::from_rel_path);
            if self.expanded_dirs.get(&dir) == Some(&false) {
                self.expanded_dirs.insert(dir, true);
                needs_rebuild = true;
            }
            current_dir = parent;
        }

        if needs_rebuild {
            self.rebuild();
        }

        let Some(index) = self.entries_indices.get(repo_path).copied() else {
            return false;
        };
        self.selected_entry = Some(index);
        true
    }

    pub(crate) fn toggle_directory(&mut self, path: &RepoPath) {
        if let Some(expanded) = self.expanded_dirs.get_mut(path) {
            *expanded = !*expanded;
            let selected_entry = self.selected_entry_identity();
            self.rebuild();
            self.selected_entry = selected_entry
                .as_ref()
                .and_then(|identity| self.index_for_selected_entry_identity(identity))
                .or_else(|| self.entries_indices.get(path).copied());
        }
    }

    fn selected_entry_identity(&self) -> Option<SelectedEntryIdentity> {
        let entry = self
            .selected_entry
            .and_then(|index| self.entries.get(index))?;
        Some(match entry {
            ActiveDiffListEntry::File(entry) => {
                SelectedEntryIdentity::File(entry.entry.repo_path.clone())
            }
            ActiveDiffListEntry::Directory(entry) => {
                SelectedEntryIdentity::Directory(entry.path.clone())
            }
        })
    }

    fn index_for_selected_entry_identity(&self, identity: &SelectedEntryIdentity) -> Option<usize> {
        match identity {
            SelectedEntryIdentity::File(path) => self.entries_indices.get(path).copied(),
            SelectedEntryIdentity::Directory(path) => self.entries.iter().position(|entry| {
                matches!(entry, ActiveDiffListEntry::Directory(entry) if &entry.path == path)
            }),
        }
    }

    fn rebuild(&mut self) {
        self.entries.clear();
        self.visible_indices.clear();
        self.directory_descendants.clear();
        self.entries_indices.clear();

        let mut entries = self.source_entries.clone();
        entries.sort_by(|a, b| a.repo_path.cmp(&b.repo_path));

        let mut root = TreeNode::default();
        for entry in entries {
            let components = entry.repo_path.components().collect::<Vec<_>>();
            if components.is_empty() {
                root.files.push(entry);
                continue;
            }

            let mut current = &mut root;
            let mut current_path = String::new();

            for (index, component) in components.iter().enumerate() {
                if index == components.len() - 1 {
                    current.files.push(entry.clone());
                } else {
                    if !current_path.is_empty() {
                        current_path.push('/');
                    }
                    current_path.push_str(component);
                    let Ok(dir_path) = RepoPath::new(&current_path) else {
                        continue;
                    };
                    let component = SharedString::from(component.to_string());
                    current = current
                        .children
                        .entry(component.clone())
                        .or_insert_with(|| TreeNode {
                            name: component,
                            path: Some(dir_path),
                            ..Default::default()
                        });
                }
            }
        }

        let mut seen_directories = HashSet::default();
        let (entries, _) = self.flatten_tree(&root, 0, &mut seen_directories);
        self.expanded_dirs
            .retain(|path, _| seen_directories.contains(path));
        for (entry, is_visible) in entries {
            let index = self.entries.len();
            if let ActiveDiffListEntry::File(entry) = &entry {
                self.entries_indices
                    .insert(entry.entry.repo_path.clone(), index);
            }
            if is_visible {
                self.visible_indices.push(index);
            }
            self.entries.push(entry);
        }
    }

    fn flatten_tree(
        &mut self,
        node: &TreeNode,
        depth: usize,
        seen_directories: &mut HashSet<RepoPath>,
    ) -> (Vec<(ActiveDiffListEntry, bool)>, Vec<ActiveDiffEntry>) {
        let mut all_statuses = Vec::new();
        let mut flattened = Vec::new();

        for child in node.children.values() {
            let (terminal, name) = Self::compact_directory_chain(child);
            let Some(path) = terminal.path.clone().or_else(|| child.path.clone()) else {
                continue;
            };
            let (child_flattened, mut child_statuses) =
                self.flatten_tree(terminal, depth + 1, seen_directories);
            let expanded = *self.expanded_dirs.get(&path).unwrap_or(&true);
            self.expanded_dirs.entry(path.clone()).or_insert(true);
            seen_directories.insert(path.clone());
            self.directory_descendants
                .insert(path.clone(), child_statuses.clone());

            flattened.push((
                ActiveDiffListEntry::Directory(ActiveDiffDirectoryEntry {
                    path,
                    name,
                    depth,
                    expanded,
                    diff_stat: Self::aggregate_diff_stat(&child_statuses),
                }),
                true,
            ));

            if expanded {
                flattened.extend(child_flattened);
            } else {
                flattened.extend(child_flattened.into_iter().map(|(child, _)| (child, false)));
            }

            all_statuses.append(&mut child_statuses);
        }

        for file in &node.files {
            all_statuses.push(file.clone());
            flattened.push((
                ActiveDiffListEntry::File(ActiveDiffFileEntry {
                    entry: file.clone(),
                    depth,
                }),
                true,
            ));
        }

        (flattened, all_statuses)
    }

    fn aggregate_diff_stat(entries: &[ActiveDiffEntry]) -> Option<DiffStat> {
        let mut aggregate = DiffStat::default();
        let mut has_diff_stat = false;
        for entry in entries {
            let Some(diff_stat) = entry.diff_stat else {
                continue;
            };
            has_diff_stat = true;
            aggregate.added = aggregate.added.saturating_add(diff_stat.added);
            aggregate.deleted = aggregate.deleted.saturating_add(diff_stat.deleted);
        }
        has_diff_stat.then_some(aggregate)
    }

    fn compact_directory_chain(mut node: &TreeNode) -> (&TreeNode, SharedString) {
        let mut parts = vec![node.name.clone()];
        while node.files.is_empty() && node.children.len() == 1 {
            let Some(child) = node.children.values().next() else {
                break;
            };
            if child.path.is_none() {
                break;
            }
            parts.push(child.name.clone());
            node = child;
        }
        (node, SharedString::from(parts.join("/")))
    }
}

#[derive(Default)]
struct TreeNode {
    name: SharedString,
    path: Option<RepoPath>,
    children: BTreeMap<SharedString, TreeNode>,
    files: Vec<ActiveDiffEntry>,
}

#[cfg(test)]
mod tests {
    use super::*;
    use git::{
        repository::repo_path,
        status::{StatusCode, TrackedStatus},
    };

    fn entry(path: &str, added: u32, deleted: u32) -> ActiveDiffEntry {
        ActiveDiffEntry {
            repo_path: repo_path(path),
            status: FileStatus::Tracked(TrackedStatus {
                index_status: StatusCode::Modified,
                worktree_status: StatusCode::Modified,
            }),
            diff_stat: Some(DiffStat { added, deleted }),
        }
    }

    #[test]
    fn builds_tree_and_compacts_single_child_directories() {
        let mut tree = ActiveDiffTree::default();
        tree.set_entries(vec![entry("src/deep/file.rs", 2, 1)]);

        assert_eq!(tree.visible_indices(), &[0, 1]);
        let directory = match &tree.entries()[0] {
            ActiveDiffListEntry::Directory(directory) => directory,
            ActiveDiffListEntry::File(_) => panic!("expected directory"),
        };
        assert_eq!(directory.name.to_string(), "src/deep");
        assert_eq!(directory.depth, 0);
        assert_eq!(
            directory.diff_stat,
            Some(DiffStat {
                added: 2,
                deleted: 1
            })
        );

        let file = match &tree.entries()[1] {
            ActiveDiffListEntry::File(file) => file,
            ActiveDiffListEntry::Directory(_) => panic!("expected file"),
        };
        assert_eq!(file.depth, 1);
        assert_eq!(file.entry.repo_path, repo_path("src/deep/file.rs"));
    }

    #[test]
    fn aggregates_directory_diffstats() {
        let mut tree = ActiveDiffTree::default();
        tree.set_entries(vec![
            entry("src/a.rs", 2, 1),
            entry("src/b.rs", 3, 4),
            entry("README.md", 1, 0),
        ]);

        let src = tree
            .entries()
            .iter()
            .find_map(|entry| match entry {
                ActiveDiffListEntry::Directory(directory) if directory.name.as_ref() == "src" => {
                    Some(directory)
                }
                _ => None,
            })
            .expect("src directory");

        assert_eq!(
            src.diff_stat,
            Some(DiffStat {
                added: 5,
                deleted: 5
            })
        );
    }

    #[test]
    fn selects_path_and_expands_ancestors() {
        let mut tree = ActiveDiffTree::default();
        tree.set_entries(vec![entry("src/deep/file.rs", 1, 1)]);
        let directory_path = repo_path("src/deep");
        tree.toggle_directory(&directory_path);

        assert_eq!(tree.visible_indices(), &[0]);
        assert!(tree.select_path(&repo_path("src/deep/file.rs")));
        assert_eq!(tree.visible_indices(), &[0, 1]);
        assert_eq!(
            tree.selected_file().map(|entry| entry.repo_path.clone()),
            Some(repo_path("src/deep/file.rs"))
        );
    }

    #[test]
    fn preserves_selection_when_entries_are_rebuilt() {
        let mut tree = ActiveDiffTree::default();
        tree.set_entries(vec![entry("a.rs", 1, 0), entry("b.rs", 0, 1)]);
        assert!(tree.select_path(&repo_path("b.rs")));

        tree.set_entries(vec![
            entry("a.rs", 1, 0),
            entry("b.rs", 3, 3),
            entry("c.rs", 1, 1),
        ]);

        assert_eq!(
            tree.selected_file().map(|entry| entry.repo_path.clone()),
            Some(repo_path("b.rs"))
        );
    }
}
