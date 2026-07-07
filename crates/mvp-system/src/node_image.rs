use std::collections::{BTreeMap, BTreeSet};
use std::fs::{self, File};
use std::io::Read;
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};

const IMAGE_SOURCE_INPUTS: &[&str] = &[
    "Cargo.lock",
    "Cargo.toml",
    "src",
    "crates/datastream/Cargo.toml",
    "crates/datastream/src",
    "crates/distribution/Cargo.toml",
    "crates/distribution/src",
    "crates/iroh-driver/Cargo.toml",
    "crates/iroh-driver/src",
    "crates/mvp-system/Cargo.toml",
    "crates/mvp-system/src",
    "crates/transport/Cargo.toml",
    "crates/transport/src",
    "tools/vastai/Cargo.toml",
    "tools/vastai/src",
    "apps/mvp-node/Dockerfile",
    "apps/mvp-node/Dockerfile.base",
    "apps/mvp-node/mvp_entrypoint.sh",
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

pub fn prepare_node_image(request: NodeImageRequest) -> Result<PreparedNodeImage, String> {
    if !request.enabled {
        return Ok(PreparedNodeImage {
            image_ref: request.requested_image,
            tag: String::new(),
            already_available: false,
            built: false,
            pushed: false,
        });
    }

    let root = workspace_root()?;
    let image = ImageName::parse(&request.requested_image)?;
    if request.provider.requires_remote_image() && !looks_registry_reachable(&image.repository) {
        return Err(format!(
            "VastAI node image {:?} must include a registry namespace",
            image.repository
        ));
    }

    let tag = image_version_tag(&root)?;
    let image_ref = image.ref_for_tag(&tag);
    let source_hash = source_content_hash(&root)?;
    let worker_hash = file_content_hash(&root, Path::new("apps/mvp-node/tinygrad_worker.py"))?;
    let base_hash = content_hash_for_inputs(&root, BASE_IMAGE_SOURCE_INPUTS)?;
    let expected_node_labels = node_image_labels(&tag, &source_hash, &worker_hash, &base_hash);
    let expected_base_labels = base_image_labels(&base_hash);
    let alias_tags = alias_tags(&image, request.extra_tag.as_deref(), &tag)?;
    let remote_required = request.provider.requires_remote_image() || request.push;

    let local_image_matches = docker_image_labels_match(&root, &image_ref, &expected_node_labels)?;
    let remote_available = remote_required && docker_manifest_exists(&root, &image_ref);
    if !request.force_refresh && remote_required && remote_available {
        let pushed = ensure_aliases_for_remote(&root, &image_ref, &image, &alias_tags)?;
        return Ok(PreparedNodeImage {
            image_ref,
            tag,
            already_available: true,
            built: false,
            pushed,
        });
    }
    if !request.force_refresh && remote_required && local_image_matches {
        ensure_aliases_local(&root, &image_ref, &image, &alias_tags)?;
        push_image(&root, &image_ref)?;
        for alias in alias_refs(&image, &alias_tags) {
            push_image(&root, &alias)?;
        }
        return Ok(PreparedNodeImage {
            image_ref,
            tag,
            already_available: true,
            built: false,
            pushed: true,
        });
    }
    if !request.force_refresh && !remote_required && local_image_matches {
        ensure_aliases_local(&root, &image_ref, &image, &alias_tags)?;
        return Ok(PreparedNodeImage {
            image_ref,
            tag,
            already_available: true,
            built: false,
            pushed: false,
        });
    }

    run_status(
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
    )?;
    let base_image_matches =
        docker_image_labels_match(&root, &request.base_image, &expected_base_labels)?;
    if !base_image_matches {
        run_status_vec(
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
        )?;
    }

    let node_bin = request.node_bin.to_string_lossy().to_string();
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
    run_status_vec(&root, "docker", build_args, "build mvp node image")?;
    ensure_aliases_local(&root, &image_ref, &image, &alias_tags)?;

    let mut pushed = false;
    if remote_required {
        push_image(&root, &image_ref)?;
        pushed = true;
        for alias in alias_refs(&image, &alias_tags) {
            push_image(&root, &alias)?;
        }
    }

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

fn image_version_tag(root: &Path) -> Result<String, String> {
    if git_worktree_clean(root)? {
        let sha = git_capture(root, &["rev-parse", "--short=12", "HEAD"])?;
        Ok(format!("git-{}", sha.trim()))
    } else {
        Ok(format!("dirty-{}", dirty_content_hash(root)?))
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

fn dirty_content_hash(root: &Path) -> Result<String, String> {
    source_content_hash(root)
}

fn source_content_hash(root: &Path) -> Result<String, String> {
    content_hash_for_inputs(root, IMAGE_SOURCE_INPUTS)
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
    let mut hasher = blake3::Hasher::new();
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
    root: &Path,
    source_ref: &str,
    image: &ImageName,
    alias_tags: &BTreeSet<String>,
) -> Result<(), String> {
    for alias in alias_refs(image, alias_tags) {
        if alias != source_ref {
            run_status(
                root,
                "docker",
                &["tag", source_ref, &alias],
                "tag mvp node image",
            )?;
        }
    }
    Ok(())
}

fn ensure_aliases_for_remote(
    root: &Path,
    source_ref: &str,
    image: &ImageName,
    alias_tags: &BTreeSet<String>,
) -> Result<bool, String> {
    if alias_tags.is_empty() {
        return Ok(false);
    }
    if !docker_image_exists(root, source_ref) {
        run_status(root, "docker", &["pull", source_ref], "pull mvp node image")?;
    }
    ensure_aliases_local(root, source_ref, image, alias_tags)?;
    for alias in alias_refs(image, alias_tags) {
        push_image(root, &alias)?;
    }
    Ok(true)
}

fn alias_refs(image: &ImageName, alias_tags: &BTreeSet<String>) -> Vec<String> {
    alias_tags
        .iter()
        .map(|tag| image.ref_for_tag(tag))
        .collect()
}

fn push_image(root: &Path, image_ref: &str) -> Result<(), String> {
    run_status(root, "docker", &["push", image_ref], "push mvp node image")
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
    root: &Path,
    image_ref: &str,
    expected: &[(&str, &str)],
) -> Result<bool, String> {
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
        return Ok(false);
    }
    let stdout = String::from_utf8_lossy(&output.stdout);
    let labels: Option<BTreeMap<String, String>> = serde_json::from_str(stdout.trim())
        .map_err(|e| format!("parse docker labels for {image_ref}: {e}"))?;
    let labels = labels.unwrap_or_default();
    Ok(expected
        .iter()
        .all(|(key, value)| labels.get(*key).map(String::as_str) == Some(*value)))
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

fn run_status(root: &Path, program: &str, args: &[&str], label: &str) -> Result<(), String> {
    eprintln!("mvp-node-image: {label}");
    let status = Command::new(program)
        .current_dir(root)
        .args(args)
        .stdin(Stdio::null())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .status()
        .map_err(|e| format!("run {label}: {e}"))?;
    if status.success() {
        Ok(())
    } else {
        Err(format!("{label} failed with {status}"))
    }
}

fn run_status_vec(
    root: &Path,
    program: &str,
    args: Vec<String>,
    label: &str,
) -> Result<(), String> {
    eprintln!("mvp-node-image: {label}");
    let status = Command::new(program)
        .current_dir(root)
        .args(&args)
        .stdin(Stdio::null())
        .stdout(Stdio::inherit())
        .stderr(Stdio::inherit())
        .status()
        .map_err(|e| format!("run {label}: {e}"))?;
    if status.success() {
        Ok(())
    } else {
        Err(format!("{label} failed with {status}"))
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
}
