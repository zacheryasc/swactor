use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File};
use std::io::{BufRead, BufReader, Read};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::mpsc;
use std::thread;
use std::time::{Duration, Instant};

const NODE_IMAGE_CONTENT_INPUTS: &[&str] = &[
    "apps/myelin/node-image/Dockerfile",
    "apps/myelin/node-image/tinygrad_worker.py",
];

const BASE_IMAGE_SOURCE_INPUTS: &[&str] = &[
    "apps/myelin/node-image/Dockerfile.base",
    "apps/myelin/node-image/myelin_entrypoint.sh",
];
const NODE_IMAGE_TAG_LABEL: &str = "org.swactor.myelin.node-image-tag";
const NODE_IMAGE_SOURCE_HASH_LABEL: &str = "org.swactor.myelin.node.source-hash";
const NODE_IMAGE_WORKER_HASH_LABEL: &str = "org.swactor.myelin.node.worker-hash";
const NODE_IMAGE_BASE_HASH_LABEL: &str = "org.swactor.myelin.node.base-hash";
const BASE_IMAGE_SOURCE_HASH_LABEL: &str = "org.swactor.myelin.base.source-hash";

pub(super) struct NodeImageRequest {
    pub(super) requested_image: String,
    pub(super) base_image: String,
    pub(super) node_bin: PathBuf,
    pub(super) requires_registry_image: bool,
    pub(super) extra_tag: Option<String>,
    pub(super) force_refresh: bool,
    pub(super) enabled: bool,
}

#[derive(Clone, Debug, PartialEq, Eq)]
pub(super) enum NodeImageProgressEventKind {
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
pub(super) struct NodeImageProgressEvent {
    pub(super) command_label: Option<String>,
    pub(super) image_ref: Option<String>,
    pub(super) elapsed_ms: Option<u128>,
    pub(super) kind: NodeImageProgressEventKind,
}

pub(super) trait NodeImageProgressSink {
    fn emit(&mut self, event: NodeImageProgressEvent);
}

pub(super) fn prepare_node_image_with_progress(
    request: NodeImageRequest,
    progress: Option<&mut dyn NodeImageProgressSink>,
) -> Result<String, String> {
    let mut progress = progress;
    prepare_node_image_inner(request, &mut progress)
}

fn prepare_node_image_inner(
    request: NodeImageRequest,
    progress: &mut Option<&mut dyn NodeImageProgressSink>,
) -> Result<String, String> {
    emit_image_reference(progress, "requested", &request.requested_image);
    if !request.enabled {
        return Ok(request.requested_image);
    }

    emit_image_reference(progress, "base", &request.base_image);
    let root = workspace_root()?;
    let image = ImageName::parse(&request.requested_image)?;
    let first_repository_component = image
        .repository
        .split('/')
        .next()
        .unwrap_or(&image.repository);
    let registry_reachable = image.repository.contains('/')
        || first_repository_component.contains('.')
        || first_repository_component.contains(':')
        || first_repository_component == "localhost";
    if request.requires_registry_image && !registry_reachable {
        return Err(format!(
            "VastAI node image {:?} must include a registry namespace",
            image.repository
        ));
    }

    run_status_command(
        &root,
        "cargo",
        &["build", "--quiet", "-p", "myelin", "--bin", "myelin-worker"],
        "build myelin-worker",
        None,
        progress,
    )?;

    let base_hash = content_hash_for_inputs(&root, BASE_IMAGE_SOURCE_INPUTS)?;
    let image_content_hash = node_image_content_hash(&root, &request.node_bin, &base_hash)?;
    let tag = image_version_tag(&root, &image_content_hash)?;
    let image_ref = image.ref_for_tag(&tag);
    emit_image_reference(progress, "resolved", &image_ref);
    let worker_hash = hash_relative_files_with_salts(
        &root,
        vec![relative_path(
            &root,
            &root.join("apps/myelin/node-image/tinygrad_worker.py"),
        )?],
        &[],
    )?;
    let expected_node_labels = vec![
        (NODE_IMAGE_TAG_LABEL, tag.as_str()),
        (NODE_IMAGE_SOURCE_HASH_LABEL, image_content_hash.as_str()),
        (NODE_IMAGE_WORKER_HASH_LABEL, worker_hash.as_str()),
        (NODE_IMAGE_BASE_HASH_LABEL, base_hash.as_str()),
    ];
    let expected_base_labels = vec![(BASE_IMAGE_SOURCE_HASH_LABEL, base_hash.as_str())];
    let alias_tags = alias_tags(&image, request.extra_tag.as_deref(), &tag)?;
    for alias in alias_refs(&image, &alias_tags) {
        emit_image_reference(progress, "alias", &alias);
    }
    let remote_required = request.requires_registry_image;

    let local_image_matches = docker_image_labels_match(&root, &image_ref, &expected_node_labels)?;
    let remote_available = remote_required && docker_manifest_exists(&root, &image_ref);
    if !request.force_refresh && remote_required && remote_available {
        ensure_aliases_for_remote(progress, &root, &image_ref, &image, &alias_tags)?;
        prune_old_dirty_images(&root, &image, &tag);
        return Ok(image_ref);
    }
    if !request.force_refresh && remote_required && local_image_matches {
        ensure_aliases_local(progress, &root, &image_ref, &image, &alias_tags)?;
        push_image(progress, &root, &image_ref)?;
        for alias in alias_refs(&image, &alias_tags) {
            push_image(progress, &root, &alias)?;
        }
        prune_old_dirty_images(&root, &image, &tag);
        return Ok(image_ref);
    }
    if !request.force_refresh && !remote_required && local_image_matches {
        ensure_aliases_local(progress, &root, &image_ref, &image, &alias_tags)?;
        prune_old_dirty_images(&root, &image, &tag);
        return Ok(image_ref);
    }
    let base_image_matches =
        docker_image_labels_match(&root, &request.base_image, &expected_base_labels)?;
    if !base_image_matches {
        let base_source_hash_label = format!("{BASE_IMAGE_SOURCE_HASH_LABEL}={base_hash}");
        run_status_command(
            &root,
            "docker",
            &[
                "build",
                "-f",
                "apps/myelin/node-image/Dockerfile.base",
                "--label",
                base_source_hash_label.as_str(),
                "-t",
                request.base_image.as_str(),
                ".",
            ],
            "build myelin node base image",
            Some(&request.base_image),
            progress,
        )?;
    }

    let node_bin = {
        let full = if request.node_bin.is_absolute() {
            request.node_bin.to_path_buf()
        } else {
            root.join(&request.node_bin)
        };
        relative_path(&root, &full).map(|relative| relative.to_string_lossy().to_string())
    }?;
    let mut build_args = vec![
        "build".to_owned(),
        "-f".to_owned(),
        "apps/myelin/node-image/Dockerfile".to_owned(),
        "--build-arg".to_owned(),
        format!("BASE_IMAGE={}", request.base_image),
        "--build-arg".to_owned(),
        format!("MYELIN_NODE_BIN={node_bin}"),
    ];
    for (key, value) in &expected_node_labels {
        build_args.push("--label".to_owned());
        build_args.push(format!("{key}={value}"));
    }
    build_args.extend(["-t".to_owned(), image_ref.clone(), ".".to_owned()]);
    run_status_command(
        &root,
        "docker",
        &build_args.iter().map(String::as_str).collect::<Vec<_>>(),
        "build myelin node image",
        Some(&image_ref),
        progress,
    )?;
    ensure_aliases_local(progress, &root, &image_ref, &image, &alias_tags)?;

    if remote_required {
        push_image(progress, &root, &image_ref)?;
        for alias in alias_refs(&image, &alias_tags) {
            push_image(progress, &root, &alias)?;
        }
    }

    prune_old_dirty_images(&root, &image, &tag);
    Ok(image_ref)
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
    if git_capture(root, &["status", "--porcelain"])?
        .trim()
        .is_empty()
    {
        let sha = git_capture(root, &["rev-parse", "--short=12", "HEAD"])?;
        Ok(format!("git-{}", sha.trim()))
    } else {
        Ok(format!("dirty-{image_content_hash}"))
    }
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
        if !matches!(path.extension().and_then(|ext| ext.to_str()), Some("pyc")) {
            out.push(relative_path(root, path)?);
        }
        return Ok(());
    }
    if !metadata.is_dir()
        || matches!(
            path.file_name().and_then(|name| name.to_str()),
            Some(".git" | "target" | "__pycache__")
        )
    {
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

fn display_workspace_path(root: &Path, path: &Path) -> String {
    match path.strip_prefix(root) {
        Ok(relative) if relative.as_os_str().is_empty() => ".".to_owned(),
        Ok(relative) => format!("./{}", relative.display()),
        Err(_) => path.display().to_string(),
    }
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

fn ensure_aliases_local(
    progress: &mut Option<&mut dyn NodeImageProgressSink>,
    root: &Path,
    source_ref: &str,
    image: &ImageName,
    alias_tags: &BTreeSet<String>,
) -> Result<(), String> {
    for alias in alias_refs(image, alias_tags) {
        if alias != source_ref {
            run_status_command(
                root,
                "docker",
                &["tag", source_ref, &alias],
                "tag myelin node image",
                Some(&alias),
                progress,
            )?;
        }
    }
    Ok(())
}

fn ensure_aliases_for_remote(
    progress: &mut Option<&mut dyn NodeImageProgressSink>,
    root: &Path,
    source_ref: &str,
    image: &ImageName,
    alias_tags: &BTreeSet<String>,
) -> Result<bool, String> {
    if alias_tags.is_empty() {
        return Ok(false);
    }
    if !docker_image_exists(root, source_ref) {
        run_status_command(
            root,
            "docker",
            &["pull", source_ref],
            "pull myelin node image",
            Some(source_ref),
            progress,
        )?;
    }
    ensure_aliases_local(progress, root, source_ref, image, alias_tags)?;
    for alias in alias_refs(image, alias_tags) {
        push_image(progress, root, &alias)?;
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
    progress: &mut Option<&mut dyn NodeImageProgressSink>,
    root: &Path,
    image_ref: &str,
) -> Result<(), String> {
    run_status_command(
        root,
        "docker",
        &["push", image_ref],
        "push myelin node image",
        Some(image_ref),
        progress,
    )
}

fn docker_image_labels_match(
    root: &Path,
    image_ref: &str,
    expected: &[(&str, &str)],
) -> Result<bool, String> {
    let Some(labels) = docker_image_labels(root, image_ref)? else {
        return Ok(false);
    };
    Ok(expected
        .iter()
        .all(|(key, value)| labels.get(*key).map(String::as_str) == Some(*value)))
}

fn prune_old_dirty_images(root: &Path, image: &ImageName, keep_tag: &str) {
    let prune_enabled = std::env::var("MYELIN_NODE_IMAGE_PRUNE")
        .map(|value| {
            let value = value.trim().to_ascii_lowercase();
            !matches!(value.as_str(), "0" | "false" | "no" | "off")
        })
        .unwrap_or(true);
    if !prune_enabled {
        return;
    }

    let tags = match docker_image_tags(root, &image.repository) {
        Ok(tags) => tags,
        Err(error) => {
            eprintln!("myelin-node-image: prune old dirty images skipped: {error}");
            return;
        }
    };

    let keep_old = std::env::var("MYELIN_NODE_IMAGE_PRUNE_KEEP")
        .ok()
        .and_then(|value| value.trim().parse::<usize>().ok())
        .unwrap_or(3);
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
        let Ok(Some(labels)) = docker_image_labels(root, &image_ref) else {
            continue;
        };
        if labels.get(NODE_IMAGE_TAG_LABEL).map(String::as_str) != Some(tag.as_str())
            || !labels.contains_key(NODE_IMAGE_SOURCE_HASH_LABEL)
            || !labels.contains_key(NODE_IMAGE_WORKER_HASH_LABEL)
            || !labels.contains_key(NODE_IMAGE_BASE_HASH_LABEL)
        {
            continue;
        }
        if docker_image_has_container(root, &image_ref) {
            eprintln!(
                "myelin-node-image: prune old dirty image {image_ref} skipped: container exists"
            );
            continue;
        }
        if retained_old < keep_old {
            retained_old += 1;
            continue;
        }

        eprintln!("myelin-node-image: prune old dirty image {image_ref}");
        if let Err(error) = docker_image_remove(root, &image_ref) {
            eprintln!("myelin-node-image: prune old dirty image {image_ref} skipped: {error}");
        }
    }
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

// container image build is provisioning infrastructure, out of scope (ENGINE_SPEC.md §2)
#[allow(clippy::disallowed_methods)]
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

// container image build is provisioning infrastructure, out of scope (ENGINE_SPEC.md §2)
#[allow(clippy::disallowed_methods)]
fn run_status_command(
    root: &Path,
    program: &str,
    args: &[&str],
    label: &str,
    image_ref: Option<&str>,
    progress: &mut Option<&mut dyn NodeImageProgressSink>,
) -> Result<(), String> {
    let args: Vec<String> = args.iter().map(|arg| (*arg).to_owned()).collect();
    eprintln!("myelin-node-image: {label}");
    if progress.is_none() {
        let status = Command::new(program)
            .current_dir(root)
            .args(&args)
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
        .args(&args)
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

fn docker_image_tags(root: &Path, repository: &str) -> Result<Vec<(String, String)>, String> {
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

fn docker_image_remove(root: &Path, image_ref: &str) -> Result<(), String> {
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
