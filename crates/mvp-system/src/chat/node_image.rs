use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File};
use std::io::{BufRead, BufReader, Read};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

const NODE_IMAGE_CONTENT_INPUTS: &[&str] = &[
    "apps/mvp-node/Dockerfile",
    "apps/mvp-node/tinygrad_worker.py",
];

const BASE_IMAGE_SOURCE_INPUTS: &[&str] = &[
    "apps/mvp-node/Dockerfile.base",
    "apps/mvp-node/mvp_entrypoint.sh",
];
const NODE_IMAGE_TAG_LABEL: &str = "org.swactor.mvp.node-image-tag";
const NODE_IMAGE_SOURCE_HASH_LABEL: &str = "org.swactor.mvp.node.source-hash";
const NODE_IMAGE_WORKER_HASH_LABEL: &str = "org.swactor.mvp.node.worker-hash";
const NODE_IMAGE_BASE_HASH_LABEL: &str = "org.swactor.mvp.node.base-hash";
const BASE_IMAGE_SOURCE_HASH_LABEL: &str = "org.swactor.mvp.base.source-hash";
const NODE_IMAGE_PRUNE_ENV: &str = "MVP_NODE_IMAGE_PRUNE";
const NODE_IMAGE_PRUNE_KEEP_ENV: &str = "MVP_NODE_IMAGE_PRUNE_KEEP";
const DEFAULT_DIRTY_IMAGE_KEEP: usize = 3;

#[derive(Clone, Copy, Debug, PartialEq, Eq)]
pub enum NodeImageProvider {
    Docker,
    VastAi,
}

impl NodeImageProvider {
    fn requires_remote_image(self) -> bool {
        matches!(self, Self::VastAi)
    }
}

#[derive(Clone, Debug)]
pub struct NodeImageRequest {
    pub requested_image: String,
    pub base_image: String,
    pub node_bin: PathBuf,
    pub provider: NodeImageProvider,
    pub extra_tag: Option<String>,
    pub push: bool,
    pub force_refresh: bool,
    pub enabled: bool,
}

#[derive(Clone, Debug)]
pub struct PreparedNodeImage {
    pub image_ref: String,
    pub tag: String,
    pub already_available: bool,
    pub built: bool,
    pub pushed: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub enum NodeImageProgressEventKind {
    ImageReference {
        role: String,
        image_ref: String,
    },
    CommandStarted {
        program: String,
        args: Vec<String>,
    },
    CommandStdout {
        line: String,
    },
    CommandStderr {
        line: String,
    },
    CommandExited {
        status: String,
        code: Option<i32>,
        success: bool,
    },
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub struct NodeImageProgressEvent {
    pub command_label: Option<String>,
    pub image_ref: Option<String>,
    pub elapsed_ms: Option<u128>,
    pub kind: NodeImageProgressEventKind,
}

pub trait NodeImageProgressSink {
    fn emit(&mut self, event: NodeImageProgressEvent);
}

impl<F> NodeImageProgressSink for F
where
    F: FnMut(NodeImageProgressEvent),
{
    fn emit(&mut self, event: NodeImageProgressEvent) {
        self(event);
    }
}

trait ImageCommandRunner {
    fn run_status(
        &mut self,
        root: &Path,
        program: &str,
        args: &[String],
        label: &str,
        image_ref: Option<&str>,
        progress: &mut Option<&mut dyn NodeImageProgressSink>,
    ) -> Result<(), String>;

    fn docker_image_exists(&mut self, root: &Path, image_ref: &str) -> bool;

    fn docker_image_labels(
        &mut self,
        root: &Path,
        image_ref: &str,
    ) -> Result<Option<BTreeMap<String, String>>, String>;

    fn docker_manifest_exists(&mut self, root: &Path, image_ref: &str) -> bool;

    fn docker_image_has_container(&mut self, root: &Path, image_ref: &str) -> bool;

    fn docker_image_tags(
        &mut self,
        root: &Path,
        repository: &str,
    ) -> Result<Vec<(String, String)>, String>;

    fn docker_image_remove(&mut self, root: &Path, image_ref: &str) -> Result<(), String>;
}

struct RealImageCommandRunner;

pub fn prepare_node_image(request: NodeImageRequest) -> Result<PreparedNodeImage, String> {
    prepare_node_image_with_progress(request, None)
}

pub fn prepare_node_image_with_progress(
    request: NodeImageRequest,
    progress: Option<&mut dyn NodeImageProgressSink>,
) -> Result<PreparedNodeImage, String> {
    let mut progress = progress;
    let mut runner = RealImageCommandRunner;
    prepare_node_image_inner(request, &mut progress, &mut runner)
}

fn prepare_node_image_inner(
    request: NodeImageRequest,
    progress: &mut Option<&mut dyn NodeImageProgressSink>,
    runner: &mut dyn ImageCommandRunner,
) -> Result<PreparedNodeImage, String> {
    emit_image_reference(progress, "requested", &request.requested_image);
    if !request.enabled {
        return Ok(PreparedNodeImage {
            image_ref: request.requested_image,
            tag: String::new(),
            already_available: false,
            built: false,
            pushed: false,
        });
    }

    emit_image_reference(progress, "base", &request.base_image);
    let root = workspace_root()?;
    let image = ImageName::parse(&request.requested_image)?;
    if request.provider.requires_remote_image() && !looks_registry_reachable(&image.repository) {
        return Err(format!(
            "VastAI node image {:?} must include a registry namespace",
            image.repository
        ));
    }

    run_status(
        runner,
        progress,
        &root,
        "cargo",
        &[
            "build",
            "--quiet",
            "-p",
            "mvp-system",
            "--bin",
            "mvp-worker-node",
        ],
        "build mvp-worker-node",
        None,
    )?;

    let base_hash = content_hash_for_inputs(&root, BASE_IMAGE_SOURCE_INPUTS)?;
    let image_content_hash = node_image_content_hash(&root, &request.node_bin, &base_hash)?;
    let tag = image_version_tag(&root, &image_content_hash)?;
    let image_ref = image.ref_for_tag(&tag);
    emit_image_reference(progress, "resolved", &image_ref);
    let worker_hash = file_content_hash(&root, Path::new("apps/mvp-node/tinygrad_worker.py"))?;
    let expected_node_labels =
        node_image_labels(&tag, &image_content_hash, &worker_hash, &base_hash);
    let expected_base_labels = base_image_labels(&base_hash);
    let alias_tags = alias_tags(&image, request.extra_tag.as_deref(), &tag)?;
    for alias in alias_refs(&image, &alias_tags) {
        emit_image_reference(progress, "alias", &alias);
    }
    let remote_required = request.provider.requires_remote_image() || request.push;

    let local_image_matches =
        docker_image_labels_match(runner, &root, &image_ref, &expected_node_labels)?;
    let remote_available = remote_required && runner.docker_manifest_exists(&root, &image_ref);
    if !request.force_refresh && remote_required && remote_available {
        let pushed =
            ensure_aliases_for_remote(runner, progress, &root, &image_ref, &image, &alias_tags)?;
        prune_old_dirty_images(runner, &root, &image, &tag);
        return Ok(PreparedNodeImage {
            image_ref,
            tag,
            already_available: true,
            built: false,
            pushed,
        });
    }
    if !request.force_refresh && remote_required && local_image_matches {
        ensure_aliases_local(runner, progress, &root, &image_ref, &image, &alias_tags)?;
        push_image(runner, progress, &root, &image_ref)?;
        for alias in alias_refs(&image, &alias_tags) {
            push_image(runner, progress, &root, &alias)?;
        }
        prune_old_dirty_images(runner, &root, &image, &tag);
        return Ok(PreparedNodeImage {
            image_ref,
            tag,
            already_available: true,
            built: false,
            pushed: true,
        });
    }
    if !request.force_refresh && !remote_required && local_image_matches {
        ensure_aliases_local(runner, progress, &root, &image_ref, &image, &alias_tags)?;
        prune_old_dirty_images(runner, &root, &image, &tag);
        return Ok(PreparedNodeImage {
            image_ref,
            tag,
            already_available: true,
            built: false,
            pushed: false,
        });
    }
    let base_image_matches =
        docker_image_labels_match(runner, &root, &request.base_image, &expected_base_labels)?;
    if !base_image_matches {
        run_status_vec(
            runner,
            progress,
            &root,
            "docker",
            vec![
                "build".to_owned(),
                "-f".to_owned(),
                "apps/mvp-node/Dockerfile.base".to_owned(),
                "--label".to_owned(),
                format!("{BASE_IMAGE_SOURCE_HASH_LABEL}={base_hash}"),
                "-t".to_owned(),
                request.base_image.clone(),
                ".".to_owned(),
            ],
            "build mvp node base image",
            Some(&request.base_image),
        )?;
    }

    let node_bin = docker_build_context_path(&root, &request.node_bin)?;
    let mut build_args = vec![
        "build".to_owned(),
        "-f".to_owned(),
        "apps/mvp-node/Dockerfile".to_owned(),
        "--build-arg".to_owned(),
        format!("BASE_IMAGE={}", request.base_image),
        "--build-arg".to_owned(),
        format!("MVP_NODE_BIN={node_bin}"),
    ];
    for (key, value) in &expected_node_labels {
        build_args.push("--label".to_owned());
        build_args.push(format!("{key}={value}"));
    }
    build_args.extend(["-t".to_owned(), image_ref.clone(), ".".to_owned()]);
    run_status_vec(
        runner,
        progress,
        &root,
        "docker",
        build_args,
        "build mvp node image",
        Some(&image_ref),
    )?;
    ensure_aliases_local(runner, progress, &root, &image_ref, &image, &alias_tags)?;

    let mut pushed = false;
    if remote_required {
        push_image(runner, progress, &root, &image_ref)?;
        pushed = true;
        for alias in alias_refs(&image, &alias_tags) {
            push_image(runner, progress, &root, &alias)?;
        }
    }

    prune_old_dirty_images(runner, &root, &image, &tag);
    Ok(PreparedNodeImage {
        image_ref,
        tag,
        already_available: false,
        built: true,
        pushed,
    })
}

fn workspace_root() -> Result<PathBuf, String> {
    let output = Command::new("git")
        .args(["rev-parse", "--show-toplevel"])
        .current_dir(env!("CARGO_MANIFEST_DIR"))
        .stdin(Stdio::null())
        .output()
        .map_err(|e| format!("locate repository root with git: {e}"))?;
    if !output.status.success() {
        return Err(format!(
            "git rev-parse --show-toplevel failed with {}: {}",
            output.status,
            String::from_utf8_lossy(&output.stderr).trim()
        ));
    }
    Ok(PathBuf::from(
        String::from_utf8_lossy(&output.stdout).trim(),
    ))
}

fn image_version_tag(root: &Path, image_content_hash: &str) -> Result<String, String> {
    if git_worktree_clean(root)? {
        let sha = git_capture(root, &["rev-parse", "--short=12", "HEAD"])?;
        Ok(format!("git-{}", sha.trim()))
    } else {
        Ok(format!("dirty-{image_content_hash}"))
    }
}

fn git_worktree_clean(root: &Path) -> Result<bool, String> {
    Ok(git_capture(root, &["status", "--porcelain"])?
        .trim()
        .is_empty())
}

fn git_capture(root: &Path, args: &[&str]) -> Result<String, String> {
    let output = Command::new("git")
        .current_dir(root)
        .args(args)
        .stdin(Stdio::null())
        .output()
        .map_err(|e| format!("run git {}: {e}", args.join(" ")))?;
    if output.status.success() {
        Ok(String::from_utf8_lossy(&output.stdout).to_string())
    } else {
        Err(format!(
            "git {} failed with {}: {}",
            args.join(" "),
            output.status,
            String::from_utf8_lossy(&output.stderr).trim()
        ))
    }
}

fn node_image_content_hash(
    root: &Path,
    node_bin: &Path,
    base_hash: &str,
) -> Result<String, String> {
    let mut files = Vec::new();
    for input in NODE_IMAGE_CONTENT_INPUTS {
        let path = root.join(input);
        collect_hash_inputs(root, &path, &mut files)?;
    }
    let node_bin = if node_bin.is_absolute() {
        node_bin.to_path_buf()
    } else {
        root.join(node_bin)
    };
    files.push(relative_path(root, &node_bin)?);
    files.sort();
    files.dedup();
    hash_relative_files_with_salts(root, files, &[("base", base_hash)])
}

fn content_hash_for_inputs(root: &Path, inputs: &[&str]) -> Result<String, String> {
    let mut files = Vec::new();
    for input in inputs {
        let path = root.join(input);
        collect_hash_inputs(root, &path, &mut files)?;
    }
    files.sort();
    files.dedup();
    hash_relative_files(root, files)
}

fn file_content_hash(root: &Path, path: &Path) -> Result<String, String> {
    hash_relative_files(root, vec![relative_path(root, &root.join(path))?])
}

fn hash_relative_files(root: &Path, files: Vec<PathBuf>) -> Result<String, String> {
    hash_relative_files_with_salts(root, files, &[])
}

fn hash_relative_files_with_salts(
    root: &Path,
    files: Vec<PathBuf>,
    salts: &[(&str, &str)],
) -> Result<String, String> {
    let mut hasher = blake3::Hasher::new();
    for (key, value) in salts {
        hasher.update(key.as_bytes());
        hasher.update(b"\0");
        hasher.update(value.as_bytes());
        hasher.update(b"\0");
    }
    for relative in files {
        let full = root.join(&relative);
        hasher.update(relative.to_string_lossy().as_bytes());
        hasher.update(b"\0");
        hash_file_content(root, &full, &mut hasher)?;
        hasher.update(b"\0");
    }
    let hash = hasher.finalize().to_hex().to_string();
    Ok(hash[..16].to_owned())
}

fn hash_file_content(root: &Path, path: &Path, hasher: &mut blake3::Hasher) -> Result<(), String> {
    let display = display_workspace_path(root, path);
    let mut file = File::open(path).map_err(|e| format!("open {display}: {e}"))?;
    let mut buf = [0_u8; 64 * 1024];
    loop {
        let n = file
            .read(&mut buf)
            .map_err(|e| format!("read {display}: {e}"))?;
        if n == 0 {
            break;
        }
        hasher.update(&buf[..n]);
    }
    Ok(())
}

fn collect_hash_inputs(root: &Path, path: &Path, out: &mut Vec<PathBuf>) -> Result<(), String> {
    if !path.exists() {
        return Ok(());
    }
    let display = display_workspace_path(root, path);
    let metadata = fs::metadata(path).map_err(|e| format!("stat {display}: {e}"))?;
    if metadata.is_file() {
        if !skip_file(path) {
            out.push(relative_path(root, path)?);
        }
        return Ok(());
    }
    if !metadata.is_dir() || skip_dir(path) {
        return Ok(());
    }
    let entries = fs::read_dir(path).map_err(|e| format!("read dir {display}: {e}"))?;
    for entry in entries {
        let entry = entry.map_err(|e| format!("read dir entry {display}: {e}"))?;
        collect_hash_inputs(root, &entry.path(), out)?;
    }
    Ok(())
}

fn relative_path(root: &Path, path: &Path) -> Result<PathBuf, String> {
    path.strip_prefix(root).map(Path::to_path_buf).map_err(|e| {
        format!(
            "make {} relative to {}: {e}",
            display_workspace_path(root, path),
            "."
        )
    })
}

fn docker_build_context_path(root: &Path, path: &Path) -> Result<String, String> {
    let full = if path.is_absolute() {
        path.to_path_buf()
    } else {
        root.join(path)
    };
    relative_path(root, &full).map(|relative| relative.to_string_lossy().to_string())
}

fn display_workspace_path(root: &Path, path: &Path) -> String {
    match path.strip_prefix(root) {
        Ok(relative) if relative.as_os_str().is_empty() => ".".to_owned(),
        Ok(relative) => format!("./{}", relative.display()),
        Err(_) => path.display().to_string(),
    }
}

fn skip_dir(path: &Path) -> bool {
    matches!(
        path.file_name().and_then(|name| name.to_str()),
        Some(".git" | "target" | "__pycache__")
    )
}

fn skip_file(path: &Path) -> bool {
    matches!(path.extension().and_then(|ext| ext.to_str()), Some("pyc"))
}

fn alias_tags(
    image: &ImageName,
    extra_tag: Option<&str>,
    version_tag: &str,
) -> Result<BTreeSet<String>, String> {
    let mut tags = BTreeSet::new();
    if let Some(tag) = image.requested_tag.as_deref() {
        insert_alias_tag(&mut tags, tag, version_tag)?;
    }
    if let Some(tag) = extra_tag {
        insert_alias_tag(&mut tags, tag, version_tag)?;
    }
    Ok(tags)
}

fn insert_alias_tag(
    tags: &mut BTreeSet<String>,
    tag: &str,
    version_tag: &str,
) -> Result<(), String> {
    let tag = tag.trim();
    if tag.is_empty() {
        return Err("node image tag must not be empty".to_owned());
    }
    if tag != version_tag {
        tags.insert(tag.to_owned());
    }
    Ok(())
}

fn node_image_labels<'a>(
    tag: &'a str,
    source_hash: &'a str,
    worker_hash: &'a str,
    base_hash: &'a str,
) -> Vec<(&'static str, &'a str)> {
    vec![
        (NODE_IMAGE_TAG_LABEL, tag),
        (NODE_IMAGE_SOURCE_HASH_LABEL, source_hash),
        (NODE_IMAGE_WORKER_HASH_LABEL, worker_hash),
        (NODE_IMAGE_BASE_HASH_LABEL, base_hash),
    ]
}

fn base_image_labels<'a>(base_hash: &'a str) -> Vec<(&'static str, &'a str)> {
    vec![(BASE_IMAGE_SOURCE_HASH_LABEL, base_hash)]
}

fn ensure_aliases_local(
    runner: &mut dyn ImageCommandRunner,
    progress: &mut Option<&mut dyn NodeImageProgressSink>,
    root: &Path,
    source_ref: &str,
    image: &ImageName,
    alias_tags: &BTreeSet<String>,
) -> Result<(), String> {
    for alias in alias_refs(image, alias_tags) {
        if alias != source_ref {
            run_status(
                runner,
                progress,
                root,
                "docker",
                &["tag", source_ref, &alias],
                "tag mvp node image",
                Some(&alias),
            )?;
        }
    }
    Ok(())
}

fn ensure_aliases_for_remote(
    runner: &mut dyn ImageCommandRunner,
    progress: &mut Option<&mut dyn NodeImageProgressSink>,
    root: &Path,
    source_ref: &str,
    image: &ImageName,
    alias_tags: &BTreeSet<String>,
) -> Result<bool, String> {
    if alias_tags.is_empty() {
        return Ok(false);
    }
    if !runner.docker_image_exists(root, source_ref) {
        run_status(
            runner,
            progress,
            root,
            "docker",
            &["pull", source_ref],
            "pull mvp node image",
            Some(source_ref),
        )?;
    }
    ensure_aliases_local(runner, progress, root, source_ref, image, alias_tags)?;
    for alias in alias_refs(image, alias_tags) {
        push_image(runner, progress, root, &alias)?;
    }
    Ok(true)
}

fn alias_refs(image: &ImageName, alias_tags: &BTreeSet<String>) -> Vec<String> {
    alias_tags
        .iter()
        .map(|tag| image.ref_for_tag(tag))
        .collect()
}

fn push_image(
    runner: &mut dyn ImageCommandRunner,
    progress: &mut Option<&mut dyn NodeImageProgressSink>,
    root: &Path,
    image_ref: &str,
) -> Result<(), String> {
    run_status(
        runner,
        progress,
        root,
        "docker",
        &["push", image_ref],
        "push mvp node image",
        Some(image_ref),
    )
}

fn docker_image_exists(root: &Path, image_ref: &str) -> bool {
    Command::new("docker")
        .current_dir(root)
        .args(["image", "inspect", image_ref])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|status| status.success())
        .unwrap_or(false)
}

fn docker_image_labels_match(
    runner: &mut dyn ImageCommandRunner,
    root: &Path,
    image_ref: &str,
    expected: &[(&str, &str)],
) -> Result<bool, String> {
    let Some(labels) = runner.docker_image_labels(root, image_ref)? else {
        return Ok(false);
    };
    Ok(expected
        .iter()
        .all(|(key, value)| labels.get(*key).map(String::as_str) == Some(*value)))
}

fn docker_image_labels(
    root: &Path,
    image_ref: &str,
) -> Result<Option<BTreeMap<String, String>>, String> {
    let output = Command::new("docker")
        .current_dir(root)
        .args([
            "image",
            "inspect",
            "--format",
            "{{ json .Config.Labels }}",
            image_ref,
        ])
        .stdin(Stdio::null())
        .output()
        .map_err(|e| format!("inspect docker image {image_ref}: {e}"))?;
    if !output.status.success() {
        return Ok(None);
    }
    let stdout = String::from_utf8_lossy(&output.stdout);
    let labels: Option<BTreeMap<String, String>> = serde_json::from_str(stdout.trim())
        .map_err(|e| format!("parse docker labels for {image_ref}: {e}"))?;
    Ok(Some(labels.unwrap_or_default()))
}

fn docker_manifest_exists(root: &Path, image_ref: &str) -> bool {
    Command::new("docker")
        .current_dir(root)
        .args(["manifest", "inspect", image_ref])
        .stdin(Stdio::null())
        .stdout(Stdio::null())
        .stderr(Stdio::null())
        .status()
        .map(|status| status.success())
        .unwrap_or(false)
}

fn prune_old_dirty_images(
    runner: &mut dyn ImageCommandRunner,
    root: &Path,
    image: &ImageName,
    keep_tag: &str,
) {
    if !dirty_image_prune_enabled() {
        return;
    }

    let tags = match runner.docker_image_tags(root, &image.repository) {
        Ok(tags) => tags,
        Err(error) => {
            eprintln!("mvp-node-image: prune old dirty images skipped: {error}");
            return;
        }
    };

    let keep_old = dirty_image_prune_keep();
    let mut retained_old = 0_usize;
    for (repository, tag) in tags {
        if repository != image.repository
            || !tag.starts_with("dirty-")
            || tag == keep_tag
            || tag == "<none>"
        {
            continue;
        }

        let image_ref = image.ref_for_tag(&tag);
        let Ok(Some(labels)) = runner.docker_image_labels(root, &image_ref) else {
            continue;
        };
        if labels.get(NODE_IMAGE_TAG_LABEL).map(String::as_str) != Some(tag.as_str())
            || !labels.contains_key(NODE_IMAGE_SOURCE_HASH_LABEL)
            || !labels.contains_key(NODE_IMAGE_WORKER_HASH_LABEL)
            || !labels.contains_key(NODE_IMAGE_BASE_HASH_LABEL)
        {
            continue;
        }
        if runner.docker_image_has_container(root, &image_ref) {
            eprintln!(
                "mvp-node-image: prune old dirty image {image_ref} skipped: container exists"
            );
            continue;
        }
        if retained_old < keep_old {
            retained_old += 1;
            continue;
        }

        eprintln!("mvp-node-image: prune old dirty image {image_ref}");
        if let Err(error) = runner.docker_image_remove(root, &image_ref) {
            eprintln!("mvp-node-image: prune old dirty image {image_ref} skipped: {error}");
        }
    }
}

fn dirty_image_prune_enabled() -> bool {
    std::env::var(NODE_IMAGE_PRUNE_ENV)
        .map(|value| {
            let value = value.trim().to_ascii_lowercase();
            !matches!(value.as_str(), "0" | "false" | "no" | "off")
        })
        .unwrap_or(true)
}

fn dirty_image_prune_keep() -> usize {
    std::env::var(NODE_IMAGE_PRUNE_KEEP_ENV)
        .ok()
        .and_then(|value| value.trim().parse::<usize>().ok())
        .unwrap_or(DEFAULT_DIRTY_IMAGE_KEEP)
}

fn docker_image_has_container(root: &Path, image_ref: &str) -> bool {
    Command::new("docker")
        .current_dir(root)
        .args([
            "ps",
            "-a",
            "--filter",
            &format!("ancestor={image_ref}"),
            "--format",
            "{{.ID}}",
        ])
        .stdin(Stdio::null())
        .output()
        .map(|output| output.status.success() && !output.stdout.is_empty())
        .unwrap_or(true)
}

fn run_status(
    runner: &mut dyn ImageCommandRunner,
    progress: &mut Option<&mut dyn NodeImageProgressSink>,
    root: &Path,
    program: &str,
    args: &[&str],
    label: &str,
    image_ref: Option<&str>,
) -> Result<(), String> {
    let args = args.iter().map(|arg| (*arg).to_owned()).collect::<Vec<_>>();
    runner.run_status(root, program, &args, label, image_ref, progress)
}

fn run_status_vec(
    runner: &mut dyn ImageCommandRunner,
    progress: &mut Option<&mut dyn NodeImageProgressSink>,
    root: &Path,
    program: &str,
    args: Vec<String>,
    label: &str,
    image_ref: Option<&str>,
) -> Result<(), String> {
    runner.run_status(root, program, &args, label, image_ref, progress)
}

fn emit_image_reference(
    progress: &mut Option<&mut dyn NodeImageProgressSink>,
    role: &str,
    image_ref: &str,
) {
    emit_progress(
        progress,
        NodeImageProgressEvent {
            command_label: None,
            image_ref: Some(image_ref.to_owned()),
            elapsed_ms: None,
            kind: NodeImageProgressEventKind::ImageReference {
                role: role.to_owned(),
                image_ref: image_ref.to_owned(),
            },
        },
    );
}

fn emit_progress(
    progress: &mut Option<&mut dyn NodeImageProgressSink>,
    event: NodeImageProgressEvent,
) {
    if let Some(sink) = progress.as_deref_mut() {
        sink.emit(event);
    }
}

fn emit_command_progress(
    progress: &mut Option<&mut dyn NodeImageProgressSink>,
    label: &str,
    image_ref: Option<&str>,
    elapsed_ms: u128,
    kind: NodeImageProgressEventKind,
) {
    emit_progress(
        progress,
        NodeImageProgressEvent {
            command_label: Some(label.to_owned()),
            image_ref: image_ref.map(str::to_owned),
            elapsed_ms: Some(elapsed_ms),
            kind,
        },
    );
}

enum CommandOutputLine {
    Stdout(String),
    Stderr(String),
}

fn spawn_line_reader<R>(
    reader: R,
    to_line: fn(String) -> CommandOutputLine,
    tx: mpsc::Sender<CommandOutputLine>,
) -> thread::JoinHandle<()>
where
    R: Read + Send + 'static,
{
    thread::spawn(move || {
        for line in BufReader::new(reader).lines().map_while(Result::ok) {
            if tx.send(to_line(line)).is_err() {
                break;
            }
        }
    })
}

fn drain_command_lines(
    rx: &mpsc::Receiver<CommandOutputLine>,
    progress: &mut Option<&mut dyn NodeImageProgressSink>,
    label: &str,
    image_ref: Option<&str>,
    started: Instant,
) {
    while let Ok(line) = rx.try_recv() {
        let kind = match line {
            CommandOutputLine::Stdout(line) => NodeImageProgressEventKind::CommandStdout { line },
            CommandOutputLine::Stderr(line) => NodeImageProgressEventKind::CommandStderr { line },
        };
        emit_command_progress(
            progress,
            label,
            image_ref,
            started.elapsed().as_millis(),
            kind,
        );
    }
}

impl ImageCommandRunner for RealImageCommandRunner {
    fn run_status(
        &mut self,
        root: &Path,
        program: &str,
        args: &[String],
        label: &str,
        image_ref: Option<&str>,
        progress: &mut Option<&mut dyn NodeImageProgressSink>,
    ) -> Result<(), String> {
        eprintln!("mvp-node-image: {label}");
        if progress.is_none() {
            let status = Command::new(program)
                .current_dir(root)
                .args(args)
                .stdin(Stdio::null())
                .stdout(Stdio::inherit())
                .stderr(Stdio::inherit())
                .status()
                .map_err(|e| format!("run {label}: {e}"))?;
            return if status.success() {
                Ok(())
            } else {
                Err(format!("{label} failed with {status}"))
            };
        }

        let started = Instant::now();
        emit_command_progress(
            progress,
            label,
            image_ref,
            0,
            NodeImageProgressEventKind::CommandStarted {
                program: program.to_owned(),
                args: args.to_vec(),
            },
        );
        let mut child = match Command::new(program)
            .current_dir(root)
            .args(args)
            .stdin(Stdio::null())
            .stdout(Stdio::piped())
            .stderr(Stdio::piped())
            .spawn()
        {
            Ok(child) => child,
            Err(error) => {
                emit_command_progress(
                    progress,
                    label,
                    image_ref,
                    started.elapsed().as_millis(),
                    NodeImageProgressEventKind::CommandExited {
                        status: format!("spawn error: {error}"),
                        code: None,
                        success: false,
                    },
                );
                return Err(format!("run {label}: {error}"));
            }
        };

        let (tx, rx) = mpsc::channel();
        let mut readers = Vec::new();
        if let Some(stdout) = child.stdout.take() {
            readers.push(spawn_line_reader(
                stdout,
                CommandOutputLine::Stdout,
                tx.clone(),
            ));
        }
        if let Some(stderr) = child.stderr.take() {
            readers.push(spawn_line_reader(
                stderr,
                CommandOutputLine::Stderr,
                tx.clone(),
            ));
        }
        drop(tx);

        let status = loop {
            match child.try_wait() {
                Ok(Some(status)) => break status,
                Ok(None) => {
                    drain_command_lines(&rx, progress, label, image_ref, started);
                    thread::sleep(Duration::from_millis(10));
                }
                Err(error) => {
                    drain_command_lines(&rx, progress, label, image_ref, started);
                    emit_command_progress(
                        progress,
                        label,
                        image_ref,
                        started.elapsed().as_millis(),
                        NodeImageProgressEventKind::CommandExited {
                            status: format!("wait error: {error}"),
                            code: None,
                            success: false,
                        },
                    );
                    return Err(format!("run {label}: {error}"));
                }
            }
        };
        for reader in readers {
            let _ = reader.join();
        }
        drain_command_lines(&rx, progress, label, image_ref, started);
        let status_text = status.to_string();
        let success = status.success();
        emit_command_progress(
            progress,
            label,
            image_ref,
            started.elapsed().as_millis(),
            NodeImageProgressEventKind::CommandExited {
                status: status_text.clone(),
                code: status.code(),
                success,
            },
        );
        if success {
            Ok(())
        } else {
            Err(format!("{label} failed with {status_text}"))
        }
    }

    fn docker_image_exists(&mut self, root: &Path, image_ref: &str) -> bool {
        docker_image_exists(root, image_ref)
    }

    fn docker_image_labels(
        &mut self,
        root: &Path,
        image_ref: &str,
    ) -> Result<Option<BTreeMap<String, String>>, String> {
        docker_image_labels(root, image_ref)
    }

    fn docker_manifest_exists(&mut self, root: &Path, image_ref: &str) -> bool {
        docker_manifest_exists(root, image_ref)
    }

    fn docker_image_has_container(&mut self, root: &Path, image_ref: &str) -> bool {
        docker_image_has_container(root, image_ref)
    }

    fn docker_image_tags(
        &mut self,
        root: &Path,
        repository: &str,
    ) -> Result<Vec<(String, String)>, String> {
        let output = Command::new("docker")
            .current_dir(root)
            .args([
                "image",
                "ls",
                "--format",
                "{{.Repository}}\t{{.Tag}}",
                repository,
            ])
            .stdin(Stdio::null())
            .output()
            .map_err(|error| format!("docker image ls failed: {error}"))?;
        if !output.status.success() {
            return Err(format!("docker image ls failed with {}", output.status));
        }
        let stdout = String::from_utf8_lossy(&output.stdout);
        Ok(stdout
            .lines()
            .filter_map(|line| {
                let (repository, tag) = line.split_once('\t')?;
                Some((repository.to_owned(), tag.to_owned()))
            })
            .collect())
    }

    fn docker_image_remove(&mut self, root: &Path, image_ref: &str) -> Result<(), String> {
        let status = Command::new("docker")
            .current_dir(root)
            .args(["image", "rm", image_ref])
            .stdin(Stdio::null())
            .stdout(Stdio::null())
            .stderr(Stdio::null())
            .status()
            .map_err(|error| format!("docker image rm failed: {error}"))?;
        if status.success() {
            Ok(())
        } else {
            Err(format!("docker image rm failed with {status}"))
        }
    }
}

fn looks_registry_reachable(repository: &str) -> bool {
    let first = repository.split('/').next().unwrap_or(repository);
    repository.contains('/') || first.contains('.') || first.contains(':') || first == "localhost"
}

#[derive(Clone, Debug)]
struct ImageName {
    repository: String,
    requested_tag: Option<String>,
}

impl ImageName {
    fn parse(raw: &str) -> Result<Self, String> {
        let raw = raw.trim();
        if raw.is_empty() {
            return Err("node image must not be empty".to_owned());
        }
        if raw.contains('@') {
            return Err(format!(
                "node image {raw:?} uses a digest; use a repository/tag base for image preparation"
            ));
        }
        let last_slash = raw.rfind('/');
        let last_colon = raw.rfind(':');
        let has_tag = match (last_slash, last_colon) {
            (_, None) => false,
            (None, Some(_)) => true,
            (Some(slash), Some(colon)) => colon > slash,
        };
        let (repository, requested_tag) = if has_tag {
            let colon = last_colon.expect("has tag colon");
            let repository = raw[..colon].to_owned();
            let tag = raw[colon + 1..].to_owned();
            if tag.is_empty() {
                return Err(format!("node image {raw:?} has an empty tag"));
            }
            (repository, Some(tag))
        } else {
            (raw.to_owned(), None)
        };
        if repository.is_empty() {
            return Err(format!("node image {raw:?} has an empty repository"));
        }
        Ok(Self {
            repository,

            requested_tag,
        })
    }

    fn ref_for_tag(&self, tag: &str) -> String {
        format!("{}:{tag}", self.repository)
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    #[derive(Default)]
    struct CollectProgress {
        events: Vec<NodeImageProgressEvent>,
    }

    impl NodeImageProgressSink for CollectProgress {
        fn emit(&mut self, event: NodeImageProgressEvent) {
            self.events.push(event);
        }
    }

    #[derive(Default)]
    struct DryImageCommandRunner {
        commands: Vec<(String, Vec<String>, String, Option<String>)>,
        labels: BTreeMap<String, BTreeMap<String, String>>,
        existing_images: BTreeSet<String>,
        manifests: BTreeSet<String>,
        image_tags: Vec<(String, String)>,
        containers: BTreeSet<String>,
        removed_images: Vec<String>,
    }

    impl ImageCommandRunner for DryImageCommandRunner {
        fn run_status(
            &mut self,
            _root: &Path,
            program: &str,
            args: &[String],
            label: &str,
            image_ref: Option<&str>,
            progress: &mut Option<&mut dyn NodeImageProgressSink>,
        ) -> Result<(), String> {
            self.commands.push((
                program.to_owned(),
                args.to_vec(),
                label.to_owned(),
                image_ref.map(str::to_owned),
            ));
            emit_command_progress(
                progress,
                label,
                image_ref,
                0,
                NodeImageProgressEventKind::CommandStarted {
                    program: program.to_owned(),
                    args: args.to_vec(),
                },
            );
            emit_command_progress(
                progress,
                label,
                image_ref,
                1,
                NodeImageProgressEventKind::CommandStdout {
                    line: format!("{label} stdout"),
                },
            );
            emit_command_progress(
                progress,
                label,
                image_ref,
                2,
                NodeImageProgressEventKind::CommandStderr {
                    line: format!("{label} stderr"),
                },
            );
            emit_command_progress(
                progress,
                label,
                image_ref,
                3,
                NodeImageProgressEventKind::CommandExited {
                    status: "exit status: 0".to_owned(),
                    code: Some(0),
                    success: true,
                },
            );
            Ok(())
        }

        fn docker_image_exists(&mut self, _root: &Path, image_ref: &str) -> bool {
            self.existing_images.contains(image_ref)
        }

        fn docker_image_labels(
            &mut self,
            _root: &Path,
            image_ref: &str,
        ) -> Result<Option<BTreeMap<String, String>>, String> {
            Ok(self.labels.get(image_ref).cloned())
        }

        fn docker_manifest_exists(&mut self, _root: &Path, image_ref: &str) -> bool {
            self.manifests.contains(image_ref)
        }

        fn docker_image_has_container(&mut self, _root: &Path, image_ref: &str) -> bool {
            self.containers.contains(image_ref)
        }

        fn docker_image_tags(
            &mut self,
            _root: &Path,
            _repository: &str,
        ) -> Result<Vec<(String, String)>, String> {
            Ok(self.image_tags.clone())
        }

        fn docker_image_remove(&mut self, _root: &Path, image_ref: &str) -> Result<(), String> {
            self.removed_images.push(image_ref.to_owned());
            Ok(())
        }
    }

    #[test]
    fn fake_child_command_emits_structured_progress_and_duration() {
        let mut runner = RealImageCommandRunner;
        let mut progress = CollectProgress::default();
        let mut sink: Option<&mut dyn NodeImageProgressSink> = Some(&mut progress);

        runner
            .run_status(
                Path::new("."),
                "sh",
                &[
                    "-c".to_owned(),
                    "printf 'stdout line\\n'; printf 'stderr line\\n' >&2".to_owned(),
                ],
                "fake child progress",
                Some("docker.io/acme/node:test"),
                &mut sink,
            )
            .expect("fake child exits successfully");

        assert!(progress.events.iter().any(|event| matches!(
            &event.kind,
            NodeImageProgressEventKind::CommandStarted { program, .. } if program == "sh"
        )));
        assert!(progress.events.iter().any(|event| matches!(
            &event.kind,
            NodeImageProgressEventKind::CommandStdout { line } if line == "stdout line"
        )));
        assert!(progress.events.iter().any(|event| matches!(
            &event.kind,
            NodeImageProgressEventKind::CommandStderr { line } if line == "stderr line"
        )));
        let exit = progress
            .events
            .iter()
            .find(|event| matches!(event.kind, NodeImageProgressEventKind::CommandExited { .. }))
            .expect("exit progress event");
        assert_eq!(exit.command_label.as_deref(), Some("fake child progress"));
        assert_eq!(exit.image_ref.as_deref(), Some("docker.io/acme/node:test"));
        assert!(exit.elapsed_ms.is_some());
        assert!(matches!(
            exit.kind,
            NodeImageProgressEventKind::CommandExited {
                success: true,
                code: Some(0),
                ..
            }
        ));
    }

    #[test]
    fn fake_child_failure_preserves_command_label_and_status() {
        let mut runner = RealImageCommandRunner;
        let mut progress = CollectProgress::default();
        let mut sink: Option<&mut dyn NodeImageProgressSink> = Some(&mut progress);

        let error = runner
            .run_status(
                Path::new("."),
                "sh",
                &["-c".to_owned(), "exit 7".to_owned()],
                "failing fake child",
                None,
                &mut sink,
            )
            .expect_err("fake child failure propagates");
        assert!(error.contains("failing fake child"), "{error}");

        let exit = progress
            .events
            .iter()
            .find_map(|event| match &event.kind {
                NodeImageProgressEventKind::CommandExited {
                    status,
                    code,
                    success,
                } => Some((event, status, code, success)),
                _ => None,
            })
            .expect("exit progress event");
        assert_eq!(exit.0.command_label.as_deref(), Some("failing fake child"));
        assert!(exit.1.contains("exit status"), "{}", exit.1);
        assert_eq!(*exit.2, Some(7));
        assert!(!*exit.3);
    }

    #[test]
    fn prepare_node_image_with_dry_runner_returns_expected_image_and_progress() {
        let node_bin = std::env::current_exe().expect("test binary path resolves");
        let mut runner = DryImageCommandRunner::default();
        let mut progress = CollectProgress::default();
        let mut sink: Option<&mut dyn NodeImageProgressSink> = Some(&mut progress);

        let prepared = prepare_node_image_inner(
            NodeImageRequest {
                requested_image: "docker.io/acme/mvp-node:latest".to_owned(),
                base_image: "swactor-mvp-node-base:cuda12.6".to_owned(),
                node_bin,
                provider: NodeImageProvider::Docker,
                extra_tag: Some("smoke".to_owned()),
                push: false,
                force_refresh: false,
                enabled: true,
            },
            &mut sink,
            &mut runner,
        )
        .expect("dry image preparation succeeds");

        assert_eq!(
            prepared.image_ref,
            format!("docker.io/acme/mvp-node:{}", prepared.tag)
        );
        assert!(prepared.built);
        assert!(!prepared.pushed);
        assert!(runner.commands.iter().any(|(_, _, label, image_ref)| {
            label == "build mvp node image"
                && image_ref.as_deref() == Some(prepared.image_ref.as_str())
        }));
        assert!(progress.events.iter().any(|event| matches!(
            &event.kind,
            NodeImageProgressEventKind::ImageReference { role, image_ref }
                if role == "resolved" && image_ref == &prepared.image_ref
        )));
        assert!(progress.events.iter().any(|event| matches!(
            &event.kind,
            NodeImageProgressEventKind::CommandStdout { line }
                if line == "build mvp node image stdout"
        )));
    }

    #[test]
    fn node_image_content_hash_tracks_node_payload_not_unrelated_files() {
        let root = workspace_root().expect("workspace root resolves");
        let scratch = root
            .join("target/node-image-hash-test")
            .join(std::process::id().to_string());
        fs::create_dir_all(&scratch).expect("scratch dir is writable");
        let node_bin = scratch.join("mvp-worker-node");
        fs::write(&node_bin, b"worker binary v1").expect("node bin fixture is writable");

        let initial =
            node_image_content_hash(&root, &node_bin, "base-v1").expect("initial hash succeeds");
        fs::write(scratch.join("unrelated.txt"), b"not part of the image")
            .expect("unrelated fixture is writable");
        let after_unrelated =
            node_image_content_hash(&root, &node_bin, "base-v1").expect("unrelated hash succeeds");
        fs::write(&node_bin, b"worker binary v2").expect("node bin fixture update is writable");
        let after_node_bin =
            node_image_content_hash(&root, &node_bin, "base-v1").expect("node bin hash succeeds");
        let after_base =
            node_image_content_hash(&root, &node_bin, "base-v2").expect("base hash succeeds");

        let _ = fs::remove_dir_all(&scratch);

        assert_eq!(initial, after_unrelated);
        assert_ne!(initial, after_node_bin);
        assert_ne!(after_node_bin, after_base);
    }

    #[test]
    fn image_name_splits_tag_after_last_slash() {
        let image = ImageName::parse("localhost:5000/team/mvp-node:trial").unwrap();

        assert_eq!(image.repository, "localhost:5000/team/mvp-node");
        assert_eq!(image.requested_tag.as_deref(), Some("trial"));
        assert_eq!(
            image.ref_for_tag("git-abcdef"),
            "localhost:5000/team/mvp-node:git-abcdef"
        );
    }

    #[test]
    fn image_name_keeps_registry_port_without_tag() {
        let image = ImageName::parse("localhost:5000/team/mvp-node").unwrap();

        assert_eq!(image.repository, "localhost:5000/team/mvp-node");
        assert_eq!(image.requested_tag, None);
    }

    #[test]
    fn alias_tags_include_requested_and_extra_without_version_duplicate() {
        let image = ImageName::parse("ghcr.io/team/mvp-node:latest").unwrap();

        let aliases = alias_tags(&image, Some("smoke"), "dirty-1234").unwrap();

        assert_eq!(
            aliases.into_iter().collect::<Vec<_>>(),
            vec!["latest".to_owned(), "smoke".to_owned()]
        );
    }

    #[test]
    fn docker_build_context_path_makes_worker_binary_relative_to_workspace() {
        let root = Path::new("/workspace/swactor");

        assert_eq!(
            docker_build_context_path(
                root,
                Path::new("/workspace/swactor/target/debug/mvp-worker-node")
            )
            .expect("absolute workspace path is valid"),
            "target/debug/mvp-worker-node"
        );
        assert_eq!(
            docker_build_context_path(root, Path::new("target/debug/mvp-worker-node"))
                .expect("relative workspace path is valid"),
            "target/debug/mvp-worker-node"
        );
        assert!(
            docker_build_context_path(root, Path::new("/tmp/mvp-worker-node")).is_err(),
            "Docker COPY inputs must stay inside the build context"
        );
    }
}
