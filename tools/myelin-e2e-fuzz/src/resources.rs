//! Build/image helpers, Docker resource utilities, and telemetry census.

use std::collections::{BTreeMap, BTreeSet, VecDeque};
use std::fs;
use std::io::{BufRead, BufReader, Read as IoRead, Seek, SeekFrom, Write as IoWrite};
use std::net::{SocketAddr, TcpListener, TcpStream};
use std::path::{Path, PathBuf};
use std::process::{Command, Stdio};
use std::sync::{Arc, LazyLock, Mutex};
use std::thread;
use std::time::{Duration, Instant};

use serde_json::{Value, json};

use crate::budget::{Budget, record_execution_identity, record_execution_stage};

const TRANSIENT_ACTOR_TYPES: [&str; 5] = [
    "swactor_process::",
    "swactor_process_context::",
    "myelin::contextual_process::ContextualOutputRelay",
    "data_plane::host::",
    "data_plane::source::FileBlobSourceActor",
];

const LIVE_TRANSPORT_FIELDS: [&str; 6] = [
    "pending_sinks",
    "pending_inbound",
    "active_controls",
    "source_probes",
    "sink_probes",
    "local_incarnations",
];

/// Durable private fixture state is separate from publishable evidence.
pub fn private_fixture_dir(artifacts: &Path) -> Result<std::path::PathBuf, String> {
    use sha2::{Digest, Sha256};
    use std::os::unix::ffi::OsStrExt;
    use std::os::unix::fs::{DirBuilderExt, MetadataExt};

    let artifacts = artifacts
        .canonicalize()
        .map_err(|error| format!("resolve artifact directory: {error}"))?;
    let base =
        match std::env::var_os("XDG_STATE_HOME") {
            Some(path) => std::path::PathBuf::from(path),
            None => std::path::PathBuf::from(std::env::var_os("HOME").ok_or_else(|| {
                "private fixture state requires HOME or XDG_STATE_HOME".to_owned()
            })?)
            .join(".local/state"),
        };
    if !base.is_absolute() {
        return Err("private fixture state root must be absolute".to_owned());
    }
    let key = format!("{:x}", Sha256::digest(artifacts.as_os_str().as_bytes()));
    let directory = base.join("myelin-e2e").join(key);
    fs::DirBuilder::new()
        .recursive(true)
        .mode(0o700)
        .create(&directory)
        .map_err(|error| format!("create private fixture state: {error}"))?;
    let metadata = fs::symlink_metadata(&directory)
        .map_err(|error| format!("inspect private fixture state: {error}"))?;
    if !metadata.is_dir()
        || metadata.uid() != unsafe { libc::geteuid() }
        || metadata.mode() & 0o077 != 0
    {
        return Err(
            "private fixture state must be an owned, non-symlink directory with mode 0700"
                .to_owned(),
        );
    }
    let directory = directory
        .canonicalize()
        .map_err(|error| format!("resolve private fixture state: {error}"))?;
    if directory.starts_with(&artifacts) {
        return Err("private fixture state cannot be inside publishable artifacts".to_owned());
    }
    Ok(directory)
}

/// Invocation-stable executable references. Never launch a mutable Cargo output
/// after accepting this identity.
#[derive(Clone, Debug)]
pub struct BuiltBinaries {
    pub orchestrator: PathBuf,
    pub worker: PathBuf,
    pub input_digest: String,
    pub executable_digests: BTreeMap<String, String>,
    verified_files: BTreeMap<String, FileIdentity>,
}

/// The exact resolver-selected inputs and bytes qualified by ordered local gates.
#[derive(Clone, Debug, PartialEq, Eq, serde::Serialize, serde::Deserialize)]
#[serde(deny_unknown_fields)]
pub struct PreparedArtifactIdentity {
    pub schema_version: u32,
    pub source_build_input_digest: String,
    pub executables: BTreeMap<String, String>,
    pub wheel_input_digest: String,
    pub wheels: BTreeMap<String, String>,
    pub payload: BTreeMap<String, String>,
}

static REQUIRED_ARTIFACTS: LazyLock<Mutex<Option<PreparedArtifactIdentity>>> =
    LazyLock::new(Mutex::default);

fn required_artifacts(budget: &Budget) -> Result<Option<PreparedArtifactIdentity>, String> {
    Ok(budget_lock(&REQUIRED_ARTIFACTS, budget, "qualified artifact identity")?.clone())
}

/// Fail closed without building or installing anything, then pin every later resolver call.
pub fn require_prepared_artifacts(
    workspace: &Path,
    expected: &PreparedArtifactIdentity,
    budget: &Budget,
) -> Result<(), String> {
    if expected.schema_version != 2 || expected.wheels.is_empty() {
        return Err("qualified deployment artifacts are incomplete or incompatible".to_owned());
    }
    {
        let mut required = budget_lock(&REQUIRED_ARTIFACTS, budget, "pin qualified artifacts")?;
        if required.as_ref().is_some_and(|current| current != expected) {
            return Err("attempted to change qualified deployment artifacts".to_owned());
        }
        *required = Some(expected.clone());
    }
    let binaries = resolve_myelin_binaries(workspace, budget)?;
    let cache_root = workspace.join("target/myelin-e2e-cache");
    let key = wheel_input_digest(
        &binaries,
        &cache_root.join("maturin-venv/bin/maturin"),
        budget,
    )?;
    if key != expected.wheel_input_digest {
        return Err(
            "wheel compiler, interpreter or build inputs changed since qualification".to_owned(),
        );
    }
    let wheel_dir = cache_root.join("wheels").join(&key);
    for (name, digest) in &expected.wheels {
        validate_artifact_filename(name, ".whl")?;
        if sha256_hex_budget(&wheel_dir.join(name), budget)? != *digest {
            return Err(format!("qualified wheel changed: {name}"));
        }
    }
    let actual =
        prepared_artifact_identity(workspace, &binaries, key, expected.wheels.clone(), budget)?;
    if &actual != expected {
        return Err("qualified deployment payload changed".to_owned());
    }
    Ok(())
}

fn validate_artifact_filename(name: &str, suffix: &str) -> Result<(), String> {
    if !matches!(
        Path::new(name).components().next(),
        Some(std::path::Component::Normal(_))
    ) || Path::new(name).components().count() != 1
        || !name.ends_with(suffix)
    {
        return Err(format!("invalid artifact filename: {name}"));
    }
    Ok(())
}

fn prepared_artifact_identity(
    workspace: &Path,
    binaries: &BuiltBinaries,
    wheel_input_digest: String,
    wheels: BTreeMap<String, String>,
    budget: &Budget,
) -> Result<PreparedArtifactIdentity, String> {
    let mut payload = BTreeMap::from([
        (
            "bin/myelin-worker".to_owned(),
            binaries.executable_digests["myelin-worker"].clone(),
        ),
        (
            "bin/myelin-e2e-python".to_owned(),
            sha256_hex_budget(
                &workspace.join("apps/myelin/node-image/e2e_python_launcher.py"),
                budget,
            )?,
        ),
    ]);
    payload.extend(
        wheels
            .iter()
            .map(|(name, digest)| (format!("python/{name}"), digest.clone())),
    );
    Ok(PreparedArtifactIdentity {
        schema_version: 2,
        source_build_input_digest: binaries.input_digest.clone(),
        executables: binaries.executable_digests.clone(),
        wheel_input_digest,
        wheels,
        payload,
    })
}

#[derive(Clone, Debug, PartialEq, Eq)]
struct FileIdentity {
    device: u64,
    inode: u64,
    length: u64,
    modified: (i64, i64),
    changed: (i64, i64),
}

fn file_identity(path: &Path) -> Result<FileIdentity, String> {
    use std::os::unix::fs::MetadataExt;
    let metadata = fs::symlink_metadata(path).map_err(|error| error.to_string())?;
    if !metadata.is_file() {
        return Err(format!(
            "immutable executable is not a regular file: {}",
            path.display()
        ));
    }
    Ok(FileIdentity {
        device: metadata.dev(),
        inode: metadata.ino(),
        length: metadata.len(),
        modified: (metadata.mtime(), metadata.mtime_nsec()),
        changed: (metadata.ctime(), metadata.ctime_nsec()),
    })
}

static BINARIES: LazyLock<Mutex<BTreeMap<PathBuf, BuiltBinaries>>> = LazyLock::new(Mutex::default);

pub fn resolve_myelin_binaries(workspace: &Path, budget: &Budget) -> Result<BuiltBinaries, String> {
    let started = Instant::now();
    let workspace = workspace
        .canonicalize()
        .map_err(|error| error.to_string())?;
    let mut cache = budget_lock(&BINARIES, budget, "immutable binary cache")?;
    let required = required_artifacts(budget)?;
    if let Some(binaries) = cache.get_mut(&workspace) {
        if required.as_ref().is_some_and(|expected| {
            expected.source_build_input_digest != binaries.input_digest
                || expected.executables != binaries.executable_digests
        }) {
            return Err("cached executable identity differs from qualified artifacts".to_owned());
        }
        verify_binaries(binaries, budget)?;
        record_execution_stage("build.binary.cache_hit", started.elapsed(), 0, 1);
        return Ok(binaries.clone());
    }
    // Freeze source inputs once for this invocation; later fixture rounds use
    // these accepted executable references, never a mutable Cargo output.
    let input_digest = build_input_digest(&workspace, budget)?;
    if required
        .as_ref()
        .is_some_and(|expected| expected.source_build_input_digest != input_digest)
    {
        return Err("compiler, toolchain or build inputs changed since qualification".to_owned());
    }
    let directory = workspace
        .join("target/myelin-e2e-cache/binaries")
        .join(&input_digest);
    fs::create_dir_all(&directory).map_err(|error| format!("create binary cache: {error}"))?;
    let manifest = directory.join("digests.json");
    let mut binaries = BuiltBinaries {
        orchestrator: directory.join("myelin-orchestrator"),
        worker: directory.join("myelin-worker"),
        input_digest: input_digest.clone(),
        executable_digests: BTreeMap::new(),
        verified_files: BTreeMap::new(),
    };
    if let Ok(file) = fs::File::open(&manifest) {
        if let Ok(digests) = serde_json::from_reader(BufReader::new(file)) {
            binaries.executable_digests = digests;
            if required
                .as_ref()
                .is_some_and(|expected| expected.executables != binaries.executable_digests)
            {
                return Err(
                    "resolver-selected executables differ from qualified artifacts".to_owned(),
                );
            }
            if verify_binaries(&mut binaries, budget).is_ok() {
                record_binary_identity(&binaries, "disk_hit");
                cache.insert(workspace, binaries.clone());
                record_execution_stage("build.binary.disk_cache_hit", started.elapsed(), 0, 1);
                return Ok(binaries);
            }
        }
    }
    if required.is_some() {
        return Err(
            "qualified executable cache is missing or corrupt; rebuilding is prohibited".to_owned(),
        );
    }
    run_checked(
        Command::new("cargo")
            .current_dir(&workspace)
            .env("CARGO_TARGET_DIR", workspace.join("target"))
            .args(["build", "--release", "--locked", "-p", "myelin", "--bins"]),
        "build Myelin binaries",
        budget,
    )?;
    for (name, destination) in [
        ("myelin-orchestrator", &binaries.orchestrator),
        ("myelin-worker", &binaries.worker),
    ] {
        budget.check("stage immutable executable")?;
        copy_atomic(&workspace.join("target/release").join(name), destination)?;
        set_executable(destination)?;
        let mut permissions = fs::metadata(destination)
            .map_err(|error| error.to_string())?
            .permissions();
        permissions.set_readonly(true);
        fs::set_permissions(destination, permissions).map_err(|error| error.to_string())?;
        binaries
            .executable_digests
            .insert(name.to_owned(), sha256_hex_budget(destination, budget)?);
    }
    write_json_atomic(&manifest, &binaries.executable_digests)?;
    verify_binaries(&mut binaries, budget)?;
    record_binary_identity(&binaries, "built");
    cache.insert(workspace, binaries.clone());
    record_execution_stage("build.binary.miss", started.elapsed(), 0, 1);
    Ok(binaries)
}

fn record_binary_identity(binaries: &BuiltBinaries, cache_state: &str) {
    record_execution_identity(
        &format!("binaries:{}", binaries.input_digest),
        json!({
            "schema_version": 1, "source_build_input_digest": binaries.input_digest,
            "executables": binaries.executable_digests, "cache_state": cache_state,
        }),
    );
}

fn verify_binaries(binaries: &mut BuiltBinaries, budget: &Budget) -> Result<(), String> {
    for (name, path) in [
        ("myelin-orchestrator", &binaries.orchestrator),
        ("myelin-worker", &binaries.worker),
    ] {
        budget.check("verify immutable executable identity")?;
        let identity = file_identity(path)?;
        if binaries.verified_files.get(name) == Some(&identity) {
            continue;
        }
        let actual = sha256_hex_budget(path, budget)?;
        if binaries.executable_digests.get(name) != Some(&actual) {
            return Err(format!("immutable executable digest mismatch: {name}"));
        }
        binaries.verified_files.insert(name.to_owned(), identity);
    }
    Ok(())
}

fn budget_lock<'a, T>(
    mutex: &'a Mutex<T>,
    budget: &Budget,
    predicate: &str,
) -> Result<std::sync::MutexGuard<'a, T>, String> {
    loop {
        budget.check(predicate)?;
        match mutex.try_lock() {
            Ok(guard) => return Ok(guard),
            Err(std::sync::TryLockError::Poisoned(error)) => return Ok(error.into_inner()),
            Err(std::sync::TryLockError::WouldBlock) => {
                budget.wait(Duration::from_millis(10), predicate)?
            }
        }
    }
}

fn build_input_digest(workspace: &Path, budget: &Budget) -> Result<String, String> {
    use sha2::{Digest, Sha256};
    let started = Instant::now();
    let mut digest = Sha256::new();
    digest.update(b"myelin-e2e-build-v2\0release\0locked\0myelin-bins\0");
    for name in [
        "Cargo.toml",
        "Cargo.lock",
        "rust-toolchain.toml",
        "rust-toolchain",
        ".dockerignore",
        "clippy.toml",
        ".cargo",
        "src",
        "tests",
        "apps",
        "crates",
        "tools",
        "xtask",
    ] {
        let path = workspace.join(name);
        if path.exists() {
            digest.update(name.as_bytes());
            digest.update([0]);
            hash_tree(&path, &path, &mut digest, budget)?;
        }
    }
    let compiler = run_output_budget(
        Command::new("rustc").arg("-Vv").current_dir(workspace),
        "identify Rust compiler",
        budget,
    )?;
    digest.update(&compiler.stdout);
    let sysroot = run_output_budget(
        Command::new("rustc")
            .args(["--print", "sysroot"])
            .current_dir(workspace),
        "identify Rust toolchain",
        budget,
    )?;
    let sysroot = PathBuf::from(
        String::from_utf8(sysroot.stdout)
            .map_err(|error| error.to_string())?
            .trim(),
    );
    for name in ["rustc", "cargo"] {
        digest.update(name.as_bytes());
        digest.update(sha256_hex_budget(&sysroot.join("bin").join(name), budget)?.as_bytes());
    }
    let cargo_home = std::env::var_os("CARGO_HOME")
        .map(PathBuf::from)
        .or_else(|| std::env::var_os("HOME").map(|home| PathBuf::from(home).join(".cargo")));
    if let Some(home) = cargo_home {
        for name in ["config", "config.toml"] {
            let path = home.join(name);
            if path.exists() {
                digest.update(name.as_bytes());
                digest.update(sha256_hex_budget(&path, budget)?.as_bytes());
            }
        }
    }
    for name in ["cc", "c++", "ar", "ld"] {
        if let Some(path) = resolve_tool(name) {
            digest.update(name.as_bytes());
            digest.update(sha256_hex_budget(&path, budget)?.as_bytes());
        }
    }
    let build_env = std::env::vars_os()
        .filter(|(name, _)| {
            let name = name.to_string_lossy();
            name.starts_with("CARGO_")
                || name.starts_with("RUST")
                || name.starts_with("CC")
                || name.starts_with("CXX")
                || name.starts_with("AR")
                || name.starts_with("LD")
                || name.starts_with("PYO3_")
                || name.starts_with("PYTHON")
                || name.starts_with("MATURIN_")
                || name.starts_with("PKG_CONFIG")
                || matches!(
                    name.as_ref(),
                    "PATH"
                        | "CFLAGS"
                        | "CPPFLAGS"
                        | "CXXFLAGS"
                        | "LDFLAGS"
                        | "LIBRARY_PATH"
                        | "CPATH"
                        | "C_INCLUDE_PATH"
                        | "CPLUS_INCLUDE_PATH"
                        | "VIRTUAL_ENV"
                        | "CONDA_PREFIX"
                )
        })
        .collect::<BTreeMap<_, _>>();
    for (name, value) in build_env {
        digest.update(name.as_encoded_bytes());
        digest.update([0]);
        digest.update(value.as_encoded_bytes());
        digest.update([0]);
        // Tool overrides may keep the same command string while the selected compiler
        // or wrapper changes on disk. Bind those executable bytes, not just the flag.
        let name = name.to_string_lossy();
        if name.starts_with("CC")
            || name.starts_with("CXX")
            || name.starts_with("AR")
            || name.starts_with("LD")
            || name.starts_with("RUSTC")
            || name.ends_with("_LINKER")
            || name == "PYO3_PYTHON"
        {
            for argument in value.to_string_lossy().split_whitespace() {
                let path = Path::new(argument);
                let path = if path.is_file() {
                    Some(path.to_path_buf())
                } else {
                    resolve_tool(argument)
                };
                if let Some(path) = path {
                    digest.update(sha256_hex_budget(&path, budget)?.as_bytes());
                }
            }
        }
    }
    let value = format!("{:x}", digest.finalize());
    record_execution_stage("build.input_digest", started.elapsed(), 0, 1);
    Ok(value)
}

fn resolve_tool(name: &str) -> Option<PathBuf> {
    std::env::split_paths(&std::env::var_os("PATH")?)
        .map(|directory| directory.join(name))
        .find(|path| path.is_file())
}

fn wheel_input_digest(
    binaries: &BuiltBinaries,
    maturin: &Path,
    budget: &Budget,
) -> Result<String, String> {
    use sha2::{Digest, Sha256};
    let python = run_output_budget(
        Command::new("python3").args([
            "-c",
            "import sys; print(sys.version); print(sys.executable)",
        ]),
        "identify Python interpreter",
        budget,
    )?;
    let python_output = String::from_utf8(python.stdout).map_err(|error| error.to_string())?;
    let interpreter = python_output
        .lines()
        .last()
        .ok_or("Python omitted interpreter path")?;
    let builder = run_output_budget(
        Command::new(maturin).arg("--version"),
        "identify wheel builder",
        budget,
    )?;
    let mut digest = Sha256::new();
    digest.update(b"myelin-e2e-wheel-v2\0");
    digest.update(&binaries.input_digest);
    digest.update(python_output.as_bytes());
    digest.update(sha256_hex_budget(Path::new(interpreter), budget)?.as_bytes());
    digest.update(builder.stdout);
    digest.update(sha256_hex_budget(maturin, budget)?.as_bytes());
    Ok(format!("{:x}", digest.finalize()))
}

fn hash_tree(
    root: &Path,
    path: &Path,
    digest: &mut sha2::Sha256,
    budget: &Budget,
) -> Result<(), String> {
    use sha2::Digest;
    use std::os::unix::fs::MetadataExt;
    budget.check("hash immutable artifact inputs")?;
    let mut metadata = fs::symlink_metadata(path)
        .map_err(|error| format!("inspect {}: {error}", path.display()))?;
    digest.update(
        path.strip_prefix(root)
            .unwrap_or(path)
            .as_os_str()
            .as_encoded_bytes(),
    );
    digest.update([0]);
    if metadata.is_symlink() {
        let target = path
            .canonicalize()
            .map_err(|error| format!("resolve {}: {error}", path.display()))?;
        for ancestor in path.ancestors().skip(1) {
            if ancestor.canonicalize().ok().as_ref() == Some(&target) {
                return Err(format!(
                    "cyclic immutable artifact input: {}",
                    path.display()
                ));
            }
        }
        digest.update(b"symlink\0");
        digest.update(
            fs::read_link(path)
                .map_err(|error| error.to_string())?
                .as_os_str()
                .as_encoded_bytes(),
        );
        digest.update([0]);
        metadata = fs::metadata(path).map_err(|error| error.to_string())?;
    }
    if metadata.is_dir() {
        digest.update(b"directory\0");
        let mut children = fs::read_dir(path)
            .map_err(|error| error.to_string())?
            .map(|entry| entry.map(|entry| entry.path()))
            .collect::<Result<Vec<_>, _>>()
            .map_err(|error| error.to_string())?;
        children.sort();
        for child in children {
            if matches!(
                child.file_name().and_then(|name| name.to_str()),
                Some("target" | ".git" | ".venv" | "__pycache__" | "node_modules")
            ) {
                continue;
            }
            hash_tree(root, &child, digest, budget)?;
        }
    } else if metadata.is_file() {
        digest.update(b"file\0");
        digest.update((metadata.mode() & 0o777).to_le_bytes());
        digest.update(metadata.len().to_le_bytes());
        let mut file = fs::File::open(path).map_err(|error| error.to_string())?;
        let mut buffer = [0_u8; 65536];
        loop {
            budget.check("hash immutable artifact inputs")?;
            let count = file.read(&mut buffer).map_err(|error| error.to_string())?;
            if count == 0 {
                break;
            }
            digest.update(&buffer[..count]);
        }
    } else {
        return Err(format!(
            "unsupported immutable artifact input: {}",
            path.display()
        ));
    }
    Ok(())
}

fn copy_atomic(source: &Path, destination: &Path) -> Result<(), String> {
    let parent = destination
        .parent()
        .ok_or_else(|| "cache destination has no parent".to_owned())?;
    let mut temporary =
        tempfile::NamedTempFile::new_in(parent).map_err(|error| error.to_string())?;
    let mut source = fs::File::open(source).map_err(|error| error.to_string())?;
    std::io::copy(&mut source, &mut temporary).map_err(|error| error.to_string())?;
    temporary
        .persist(destination)
        .map_err(|error| error.to_string())?;
    Ok(())
}

fn write_json_atomic(path: &Path, value: &impl serde::Serialize) -> Result<(), String> {
    let mut temporary = tempfile::NamedTempFile::new_in(path.parent().unwrap_or(Path::new(".")))
        .map_err(|error| error.to_string())?;
    serde_json::to_writer(temporary.as_file_mut(), value).map_err(|error| error.to_string())?;
    temporary.persist(path).map_err(|error| error.to_string())?;
    Ok(())
}

static WORKLOAD_IMAGES: LazyLock<Mutex<BTreeMap<String, String>>> = LazyLock::new(Mutex::default);

pub(crate) fn docker_image_identity(image: &str, budget: &Budget) -> Result<String, String> {
    let output = run_output_budget(
        Command::new("docker").args(["image", "inspect", "--format", "{{.Id}}", image]),
        "identify Docker image",
        budget,
    )?;
    let identity = String::from_utf8(output.stdout).map_err(|error| error.to_string())?;
    let identity = identity.trim();
    if !identity.starts_with("sha256:") {
        return Err("Docker image inspection omitted immutable identity".to_owned());
    }
    record_execution_identity(
        &format!("image-reference:{image}"),
        json!({
            "schema_version": 1, "image_reference": image, "image_digest": identity,
        }),
    );
    Ok(identity.to_owned())
}

pub(crate) fn build_workload_image(
    workspace: &Path,
    image: &str,
    budget: &Budget,
) -> Result<(), String> {
    let started = Instant::now();
    let source = resolve_myelin_binaries(workspace, budget)?.input_digest;
    let mut cache = budget_lock(&WORKLOAD_IMAGES, budget, "workload image cache")?;
    let base = docker_image_identity("nvidia/cuda:12.6.3-runtime-ubuntu24.04", budget).ok();
    let key = format!("{source}/{}", base.as_deref().unwrap_or("unresolved"));
    if let Some(identity) = cache.get(&key) {
        if docker_image_identity(identity, budget).as_ref() == Ok(identity) {
            run_checked(
                Command::new("docker").args(["tag", identity, image]),
                "tag cached workload image",
                budget,
            )?;
            record_execution_identity(
                &format!("image:{identity}"),
                json!({
                    "schema_version": 1, "source_build_input_digest": source,
                    "image_digest": identity, "base_image_digest": base, "cache_state": "invocation_hit",
                }),
            );
            record_execution_stage("build.image.cache_hit", started.elapsed(), 0, 1);
            return Ok(());
        }
    }
    let mut parent: Option<String> = None;
    for (role, dockerfile, name) in [
        (
            "base",
            "apps/myelin/node-image/Dockerfile.base",
            "myelin-node-base:cuda12.6",
        ),
        (
            "node",
            "apps/myelin/node-image/Dockerfile",
            "myelin-node:latest",
        ),
        ("e2e", "apps/myelin/node-image/Dockerfile.e2e", image),
    ] {
        let parent_tag = parent.as_ref().map(|identity| {
            format!(
                "myelin-e2e-parent:{}",
                identity.trim_start_matches("sha256:")
            )
        });
        if let (Some(parent), Some(tag)) = (&parent, &parent_tag) {
            run_checked(
                Command::new("docker").args(["tag", parent, tag]),
                "pin Myelin parent image",
                budget,
            )?;
        }
        let mut command = Command::new("docker");
        command
            .current_dir(workspace)
            .args(["build", "-f", dockerfile, "-t", name]);
        // These labels are part of the real build's immutable config, never a
        // qualification-time assertion attached to an existing runtime image.
        command.args(["--label", "org.swactor.myelin.e2e.provenance-version=1"]);
        command.args([
            "--label",
            &format!("org.swactor.myelin.e2e.source-build-input-digest={source}"),
        ]);
        command.args([
            "--label",
            &format!("org.swactor.myelin.e2e.image-role={role}"),
        ]);
        if let (Some(parent), Some(tag)) = (&parent, &parent_tag) {
            command.args(["--build-arg", &format!("BASE_IMAGE={tag}")]);
            command.args([
                "--label",
                &format!("org.swactor.myelin.e2e.parent-image-id={parent}"),
            ]);
        }
        run_checked(
            command.arg("."),
            &format!("build Myelin {role} image"),
            budget,
        )?;
        if let (Some(parent), Some(tag)) = (&parent, &parent_tag) {
            if docker_image_identity(tag, budget)? != *parent {
                return Err("runtime image parent changed during build".to_owned());
            }
        }
        parent = Some(docker_image_identity(name, budget)?);
    }
    if build_input_digest(workspace, budget)? != source {
        return Err("source/build inputs changed while constructing runtime images".to_owned());
    }
    let identity = docker_image_identity(image, budget)?;
    let base = docker_image_identity("nvidia/cuda:12.6.3-runtime-ubuntu24.04", budget)?;
    record_execution_identity(
        &format!("image:{identity}"),
        json!({
            "schema_version": 1, "source_build_input_digest": source,
            "image_digest": identity, "base_image_digest": base, "cache_state": "built",
        }),
    );
    cache.insert(format!("{source}/{base}"), identity);
    record_execution_stage("build.image.miss", started.elapsed(), 0, 1);
    Ok(())
}

pub(crate) fn run_checked(
    command: &mut Command,
    label: &str,
    budget: &Budget,
) -> Result<(), String> {
    run_output_budget(command, label, budget).map(|_| ())
}

#[derive(serde::Serialize, serde::Deserialize, PartialEq, Eq)]
pub struct GateImageIdentity {
    pub id: String,
    pub os: String,
    pub architecture: String,
    pub variant: String,
    pub repo_digests: BTreeSet<String>,
    pub provenance_version: u32,
    pub source_build_input_digest: String,
    pub image_role: String,
    pub parent_image_id: Option<String>,
}

pub fn runtime_image_identity(name: &str, budget: &Budget) -> Result<GateImageIdentity, String> {
    let image = run_output_budget(
        Command::new("docker").args(["image", "inspect", name]),
        "inspect qualified local runtime image",
        budget,
    )?;
    let values: Vec<serde_json::Value> = serde_json::from_slice(&image.stdout)
        .map_err(|error| format!("decode runtime image identity: {error}"))?;
    let value = values
        .first()
        .ok_or("Docker omitted runtime image identity")?;
    if immutable_registry_reference(name) {
        let manifest = run_output_budget(
            Command::new("docker").args(["manifest", "inspect", name]),
            "inspect qualified registry manifest",
            budget,
        )?;
        let manifest: serde_json::Value = serde_json::from_slice(&manifest.stdout)
            .map_err(|error| format!("decode qualified registry manifest: {error}"))?;
        if manifest["schemaVersion"].as_u64() != Some(2)
            || manifest.get("manifests").is_some()
            || manifest["config"]["digest"].as_str() != value["Id"].as_str()
        {
            return Err("paid runtime requires a platform-specific registry manifest whose config digest matches the tested local image".to_owned());
        }
    }
    let field = |name: &str| value[name].as_str().unwrap_or_default().to_owned();
    let labels = &value["Config"]["Labels"];
    if labels["org.swactor.myelin.e2e.provenance-version"].as_str() != Some("1") {
        return Err(format!(
            "runtime image {name} lacks build-time source provenance; rebuild it"
        ));
    }
    Ok(GateImageIdentity {
        id: field("Id"),
        os: field("Os"),
        architecture: field("Architecture"),
        variant: field("Variant"),
        repo_digests: value["RepoDigests"]
            .as_array()
            .into_iter()
            .flatten()
            .filter_map(|value| value.as_str().map(str::to_owned))
            .collect(),
        provenance_version: 1,
        source_build_input_digest: labels["org.swactor.myelin.e2e.source-build-input-digest"]
            .as_str()
            .unwrap_or_default()
            .to_owned(),
        image_role: labels["org.swactor.myelin.e2e.image-role"]
            .as_str()
            .unwrap_or_default()
            .to_owned(),
        parent_image_id: labels["org.swactor.myelin.e2e.parent-image-id"]
            .as_str()
            .map(str::to_owned),
    })
}

pub fn immutable_registry_reference(reference: &str) -> bool {
    reference
        .split_once("@sha256:")
        .is_some_and(|(repository, digest)| {
            !repository.is_empty()
                && !repository.contains('@')
                && !repository.bytes().any(|byte| byte.is_ascii_whitespace())
                && digest.len() == 64
                && digest
                    .bytes()
                    .all(|byte| byte.is_ascii_digit() || (b'a'..=b'f').contains(&byte))
        })
}

/// Capture into files, not pipes: no reader thread can outlive the owner, and a
/// grandchild holding stdout open cannot prevent process reaping on expiry.
fn run_output_budget(
    command: &mut Command,
    label: &str,
    budget: &Budget,
) -> Result<std::process::Output, String> {
    use std::os::fd::{AsRawFd, FromRawFd, OwnedFd};
    use std::os::unix::process::CommandExt;
    let started = Instant::now();
    budget.check(label)?;
    let mut stdout = tempfile::tempfile().map_err(|error| format!("{label}: {error}"))?;
    let mut stderr = tempfile::tempfile().map_err(|error| format!("{label}: {error}"))?;
    command
        .process_group(0)
        .stdin(Stdio::null())
        .stdout(Stdio::from(
            stdout.try_clone().map_err(|error| error.to_string())?,
        ))
        .stderr(Stdio::from(
            stderr.try_clone().map_err(|error| error.to_string())?,
        ));
    let mut child = command
        .spawn()
        .map_err(|error| format!("{label}: {error}"))?;
    let pid = child.id() as libc::pid_t;
    let descriptor = unsafe { libc::syscall(libc::SYS_pidfd_open, pid, 0) } as libc::c_int;
    let pidfd = (descriptor >= 0).then(|| unsafe { OwnedFd::from_raw_fd(descriptor) });
    let outcome = loop {
        match child.try_wait() {
            Ok(Some(status)) => break Ok(status),
            Ok(None) => {}
            Err(error) => break Err(format!("{label}: observe child: {error}")),
        }
        let remaining = match budget.remaining(label) {
            Ok(remaining) => remaining,
            Err(error) => break Err(error),
        };
        if let Some(pidfd) = &pidfd {
            let mut poll = libc::pollfd {
                fd: pidfd.as_raw_fd(),
                events: libc::POLLIN,
                revents: 0,
            };
            let timeout =
                remaining.min(Duration::from_millis(50)).as_millis().max(1) as libc::c_int;
            if unsafe { libc::poll(&mut poll, 1, timeout) } < 0 {
                let error = std::io::Error::last_os_error();
                if error.kind() != std::io::ErrorKind::Interrupted {
                    break Err(format!("{label}: await child exit: {error}"));
                }
            }
        } else if let Err(error) = budget.wait(Duration::from_millis(10), label) {
            // Compatibility for kernels without pidfd_open; ownership and
            // cancellation remain bounded, unlike a blocking child.wait().
            break Err(error);
        }
    };
    if outcome.is_err() {
        // SIGKILL the whole owned group, then reap the actual child. There are
        // no detached helper threads or inherited-output pipe joins.
        unsafe {
            libc::kill(-pid, libc::SIGKILL);
        }
        let _ = child.kill();
        child
            .wait()
            .map_err(|error| format!("{label}: reap cancelled child: {error}"))?;
    }
    record_execution_stage(&format!("subprocess:{label}"), started.elapsed(), 0, 1);
    let status = outcome?;
    stdout.rewind().map_err(|error| error.to_string())?;
    stderr.rewind().map_err(|error| error.to_string())?;
    let mut out = Vec::new();
    let mut err = Vec::new();
    stdout
        .read_to_end(&mut out)
        .map_err(|error| error.to_string())?;
    stderr
        .read_to_end(&mut err)
        .map_err(|error| error.to_string())?;
    if !status.success() {
        return Err(format!(
            "{label} exited {status}: {}",
            String::from_utf8_lossy(&err)
        ));
    }
    Ok(std::process::Output {
        status,
        stdout: out,
        stderr: err,
    })
}

pub(crate) fn reserve_port() -> Result<u16, String> {
    let listener = TcpListener::bind(("127.0.0.1", 0))
        .map_err(|error| format!("reserve dashboard port: {error}"))?;
    listener
        .local_addr()
        .map(|address| address.port())
        .map_err(|error| format!("read reserved dashboard port: {error}"))
}

pub(crate) fn capture_lines(
    reader: impl std::io::Read + Send + 'static,
    lines: Arc<Mutex<VecDeque<String>>>,
) {
    thread::spawn(move || {
        for line in BufReader::new(reader).lines().map_while(Result::ok) {
            let mut lines = lines.lock().unwrap_or_else(|error| error.into_inner());
            if lines.len() == 512 {
                lines.pop_front();
            }
            lines.push_back(line);
        }
    });
}

/// One bounded control transaction. Each attempt uses a fresh socket so a
/// restarted orchestrator cannot inherit a pooled connection's stale stream.
/// GET, HEAD, and the explicitly read-only retained-resource POST are retried.
/// Mutating POSTs are never duplicated by the transport.
pub(crate) fn http_json_budget(
    method: &str,
    url: &str,
    body: Option<Value>,
    budget: &Budget,
) -> Result<Value, String> {
    let started = Instant::now();
    let rest = url
        .strip_prefix("http://")
        .ok_or_else(|| "unsupported harness HTTP scheme".to_owned())?;
    let (authority, path) = rest
        .split_once('/')
        .map(|(authority, path)| (authority, format!("/{path}")))
        .unwrap_or((rest, "/".to_owned()));
    if method.bytes().any(|byte| !byte.is_ascii_uppercase())
        || path.contains(['\r', '\n'])
        || authority.contains(['\r', '\n', '@'])
    {
        return Err("invalid harness HTTP request target".to_owned());
    }
    // Harness control URLs are numeric loopback addresses. Do not introduce an
    // uninterruptible DNS resolver into a deadline-bounded request.
    let address: SocketAddr = authority.parse().map_err(|_| {
        "harness control HTTP requires an explicit numeric address and port".to_owned()
    })?;
    if !address.ip().is_loopback() {
        return Err("harness control HTTP requires a loopback address".to_owned());
    }
    let payload = body.map(|body| body.to_string()).unwrap_or_default();
    let request = format!(
        "{method} {path} HTTP/1.1\r\nHost: {authority}\r\nConnection: close\r\n\
         Content-Length: {}\r\nContent-Type: application/json\r\n\r\n{payload}",
        payload.len()
    );
    let retryable_request = matches!(method, "GET" | "HEAD")
        || (method == "POST" && path == "/api/control/contextual/retained-blobs");
    // Exclude query/request identities and bodies from metric keys.
    let endpoint = path.split('?').next().unwrap_or("/");
    let endpoint = if endpoint.starts_with("/api/control/contextual/") {
        "/api/control/contextual"
    } else {
        endpoint
    };
    let predicate = format!("HTTP {method} {endpoint}");
    let mut attempts = 0_u64;
    let result = (|| loop {
        budget.check(&predicate)?;
        attempts += 1;
        match http_json_once(address, request.as_bytes(), budget, &predicate) {
            Ok(value) => break Ok(value),
            Err((error, retryable)) => {
                if !retryable || !retryable_request || attempts == 3 {
                    break Err(error);
                }
                record_execution_stage("http.retry.transport", Duration::ZERO, 0, 1);
                budget.wait(Duration::from_millis(20 * attempts), &predicate)?;
            }
        }
    })();
    record_execution_stage(
        &format!("http.total:{method}:{endpoint}"),
        started.elapsed(),
        0,
        attempts,
    );
    result
}

type HttpResult<T> = Result<T, (String, bool)>;

struct HttpAttemptSpan(Instant);

impl Drop for HttpAttemptSpan {
    fn drop(&mut self) {
        record_execution_stage("http.attempt", self.0.elapsed(), 0, 1);
    }
}

fn http_remaining(budget: &Budget, predicate: &str) -> HttpResult<Duration> {
    budget.remaining(predicate).map_err(|error| (error, false))
}

fn http_json_once(
    address: SocketAddr,
    request: &[u8],
    budget: &Budget,
    predicate: &str,
) -> HttpResult<Value> {
    let started = Instant::now();
    let _span = HttpAttemptSpan(started);
    let connect_budget = budget.child(Duration::from_secs(2));
    let mut stream = loop {
        let timeout = http_remaining(&connect_budget, predicate)?.min(Duration::from_millis(100));
        match TcpStream::connect_timeout(&address, timeout) {
            Ok(stream) => break stream,
            Err(error)
                if matches!(
                    error.kind(),
                    std::io::ErrorKind::TimedOut | std::io::ErrorKind::Interrupted
                ) => {}
            Err(error) => return Err((format!("{predicate}: Connect error: {error}"), true)),
        }
    };
    record_execution_stage("http.connect", started.elapsed(), 0, 1);
    let write_started = Instant::now();
    let write_budget = budget.child(Duration::from_secs(10));
    let mut written = 0;
    while written < request.len() {
        let timeout = http_remaining(&write_budget, predicate)?.min(Duration::from_millis(50));
        stream
            .set_write_timeout(Some(timeout))
            .map_err(|error| (error.to_string(), true))?;
        match stream.write(&request[written..]) {
            Ok(0) => return Err((format!("{predicate}: write returned zero"), true)),
            Ok(count) => written += count,
            Err(error) if transient_io(&error) => {}
            Err(error) => return Err((format!("{predicate}: write request: {error}"), true)),
        }
    }
    record_execution_stage("http.write", write_started.elapsed(), written as u64, 1);
    let read_started = Instant::now();
    let mut response = Vec::with_capacity(4096);
    let header_end = loop {
        if let Some(position) = response.windows(4).position(|window| window == b"\r\n\r\n") {
            break position + 4;
        }
        if response.len() >= 65536 {
            return Err((
                format!("{predicate}: response headers exceed 64 KiB"),
                false,
            ));
        }
        if !http_read(&mut stream, &mut response, budget, predicate)? {
            return Err((format!("{predicate}: response ended before headers"), true));
        }
    };
    let header = std::str::from_utf8(&response[..header_end]).map_err(|error| {
        (
            format!("{predicate}: invalid response header: {error}"),
            false,
        )
    })?;
    let mut lines = header.split("\r\n");
    let status = lines
        .next()
        .and_then(|line| line.split_ascii_whitespace().nth(1))
        .and_then(|code| code.parse::<u16>().ok())
        .ok_or_else(|| (format!("{predicate}: invalid response status"), false))?;
    let mut length = None;
    let mut chunked = false;
    for line in lines.filter(|line| !line.is_empty()) {
        let (name, value) = line
            .split_once(':')
            .ok_or_else(|| (format!("{predicate}: malformed HTTP header"), false))?;
        if name.eq_ignore_ascii_case("content-length") {
            let parsed = value.trim().parse::<usize>().map_err(|error| {
                (
                    format!("{predicate}: invalid content length: {error}"),
                    false,
                )
            })?;
            if length
                .replace(parsed)
                .is_some_and(|previous| previous != parsed)
            {
                return Err((format!("{predicate}: conflicting content lengths"), false));
            }
        } else if name.eq_ignore_ascii_case("transfer-encoding") {
            if !value.trim().eq_ignore_ascii_case("chunked") {
                return Err((format!("{predicate}: unsupported transfer encoding"), false));
            }
            chunked = true;
        }
    }
    if chunked && length.is_some() {
        return Err((
            format!("{predicate}: ambiguous HTTP response framing"),
            false,
        ));
    }
    let body = if chunked {
        read_chunked(&mut stream, &mut response, header_end, budget, predicate)?
    } else if let Some(length) = length {
        let end = header_end
            .checked_add(length)
            .ok_or_else(|| (format!("{predicate}: content length overflow"), false))?;
        while response.len() < end {
            if !http_read(&mut stream, &mut response, budget, predicate)? {
                return Err((format!("{predicate}: truncated response body"), true));
            }
        }
        response[header_end..end].to_vec()
    } else if matches!(status, 204 | 304) {
        Vec::new()
    } else {
        // HTTP/1.0/close-delimited compatibility remains bounded by the same
        // absolute budget, including a peer sending one byte per timeout.
        while http_read(&mut stream, &mut response, budget, predicate)? {}
        response.split_off(header_end)
    };
    record_execution_stage(
        "http.read",
        read_started.elapsed(),
        response.len() as u64,
        1,
    );
    http_remaining(budget, predicate)?;
    if !(200..300).contains(&status) {
        return Err((
            format!(
                "{predicate}: status code {status}: {}",
                String::from_utf8_lossy(&body)
            ),
            matches!(status, 502 | 503 | 504),
        ));
    }
    if body.is_empty() {
        return Ok(Value::Null);
    }
    let decode_started = Instant::now();
    let result = serde_json::from_slice(&body)
        .map_err(|error| (format!("{predicate}: decode response: {error}"), false));
    record_execution_stage(
        "http.decode",
        decode_started.elapsed(),
        body.len() as u64,
        1,
    );
    http_remaining(budget, predicate)?;
    result
}

fn transient_io(error: &std::io::Error) -> bool {
    matches!(
        error.kind(),
        std::io::ErrorKind::TimedOut
            | std::io::ErrorKind::WouldBlock
            | std::io::ErrorKind::Interrupted
    )
}

fn http_read(
    stream: &mut TcpStream,
    bytes: &mut Vec<u8>,
    budget: &Budget,
    predicate: &str,
) -> HttpResult<bool> {
    let mut buffer = [0_u8; 8192];
    let started = Instant::now();
    loop {
        let timeout = http_remaining(budget, predicate)?.min(Duration::from_millis(50));
        stream
            .set_read_timeout(Some(timeout))
            .map_err(|error| (error.to_string(), true))?;
        match stream.read(&mut buffer) {
            Ok(0) => return Ok(false),
            Ok(count) => {
                bytes.extend_from_slice(&buffer[..count]);
                record_execution_stage("http.received", started.elapsed(), count as u64, 1);
                return Ok(true);
            }
            Err(error) if transient_io(&error) => {}
            Err(error) => return Err((format!("{predicate}: read response: {error}"), true)),
        }
    }
}

fn read_chunked(
    stream: &mut TcpStream,
    bytes: &mut Vec<u8>,
    mut cursor: usize,
    budget: &Budget,
    predicate: &str,
) -> HttpResult<Vec<u8>> {
    let mut body = Vec::new();
    loop {
        http_remaining(budget, predicate)?;
        let line_end = loop {
            if let Some(position) = bytes[cursor..]
                .windows(2)
                .position(|window| window == b"\r\n")
            {
                break cursor + position;
            }
            if bytes.len() - cursor > 65536 {
                return Err((format!("{predicate}: oversized chunk header"), false));
            }
            if !http_read(stream, bytes, budget, predicate)? {
                return Err((format!("{predicate}: truncated chunk header"), true));
            }
        };
        let size = std::str::from_utf8(&bytes[cursor..line_end])
            .ok()
            .and_then(|line| usize::from_str_radix(line.split(';').next()?.trim(), 16).ok())
            .ok_or_else(|| (format!("{predicate}: invalid chunk size"), false))?;
        cursor = line_end + 2;
        if size == 0 {
            // Consume the terminating trailer section, not the connection EOF.
            loop {
                if bytes[cursor..].starts_with(b"\r\n")
                    || bytes[cursor..]
                        .windows(4)
                        .any(|window| window == b"\r\n\r\n")
                {
                    return Ok(body);
                }
                if bytes.len() - cursor > 65536 || !http_read(stream, bytes, budget, predicate)? {
                    return Err((format!("{predicate}: invalid chunk trailer"), false));
                }
            }
        }
        let end = cursor
            .checked_add(size)
            .and_then(|end| end.checked_add(2))
            .ok_or_else(|| (format!("{predicate}: chunk size overflow"), false))?;
        while bytes.len() < end {
            if !http_read(stream, bytes, budget, predicate)? {
                return Err((format!("{predicate}: truncated chunk body"), true));
            }
        }
        if &bytes[end - 2..end] != b"\r\n" {
            return Err((format!("{predicate}: missing chunk terminator"), false));
        }
        body.extend_from_slice(&bytes[cursor..end - 2]);
        cursor = end;
    }
}
#[derive(Default)]
pub(crate) struct TelemetryResourceCensus {
    offset: u64,
    carry: Vec<u8>,
    next_line: usize,
    active: BTreeMap<String, String>,
    arenas: BTreeMap<String, Value>,
    poisoned: Vec<Value>,
    file_identity: Option<(u64, u64)>,
    source_positions: BTreeMap<String, Value>,
    lifecycle_sequences: BTreeMap<String, (u64, u64)>,
    telemetry_gaps: Vec<String>,
    resource_snapshots: BTreeMap<u64, Value>,
    orchestrator_snapshot: Option<(String, Value)>,
}

impl TelemetryResourceCensus {
    pub(crate) fn update(&mut self, path: &Path, budget: &Budget) -> Result<(), String> {
        use std::os::unix::fs::MetadataExt;
        let started = Instant::now();
        budget.check("ingest telemetry resource census")?;
        let mut file = fs::File::open(path).map_err(|error| {
            format!("open telemetry resource census {}: {error}", path.display())
        })?;
        let metadata = file.metadata().map_err(|error| error.to_string())?;
        let identity = (metadata.dev(), metadata.ino());
        let length = metadata.len();
        if length < self.offset
            || self
                .file_identity
                .is_some_and(|previous| previous != identity)
        {
            return Err("telemetry archive identity changed or was truncated; lifecycle proof is incomplete".to_owned());
        }
        self.file_identity = Some(identity);
        file.seek(SeekFrom::Start(self.offset))
            .map_err(|error| error.to_string())?;
        let mut reader = BufReader::new(file.take(length - self.offset));
        let mut line = std::mem::take(&mut self.carry);
        let initial_offset = self.offset;
        let initial_records = self.next_line;
        loop {
            budget.check("ingest telemetry resource census")?;
            let count = reader
                .read_until(b'\n', &mut line)
                .map_err(|error| error.to_string())?;
            self.offset = self
                .offset
                .checked_add(count as u64)
                .ok_or_else(|| "telemetry cursor overflow".to_owned())?;
            if count == 0 || !line.ends_with(b"\n") {
                self.carry = line;
                break;
            }
            let text = std::str::from_utf8(&line).map_err(|error| {
                let message = format!("telemetry archive is not UTF-8: {error}");
                self.telemetry_gaps.push(message.clone());
                message
            })?;
            self.ingest(text).map_err(|error| {
                self.telemetry_gaps.push(error.clone());
                error
            })?;
            line.clear();
        }
        record_execution_stage(
            "telemetry.ingest",
            started.elapsed(),
            self.offset - initial_offset,
            self.next_line.saturating_sub(initial_records) as u64,
        );
        Ok(())
    }

    /// Only a request-tagged producer snapshot may establish fresh cleanup.
    /// Archive arrival order is deliberately not a freshness predicate.
    pub(crate) fn record_resource_snapshot(
        &mut self,
        request_id: &str,
        expected_node: u64,
        resources: &Value,
    ) -> Result<(), String> {
        if resources.get("schema_version").and_then(Value::as_u64) != Some(1)
            || resources.get("request_id").and_then(Value::as_str) != Some(request_id)
            || resources.get("logical_node_id").and_then(Value::as_u64) != Some(expected_node)
        {
            return Err(format!(
                "fresh resource snapshot identity mismatch for node {expected_node}"
            ));
        }
        let generation = resources
            .get("generation")
            .and_then(Value::as_u64)
            .ok_or_else(|| "resource snapshot omitted generation".to_owned())?;
        let sequence = resources
            .get("sample_sequence")
            .and_then(Value::as_u64)
            .ok_or_else(|| "resource snapshot omitted sample sequence".to_owned())?;
        if resources
            .get("iroh_node_id")
            .and_then(Value::as_str)
            .is_none_or(str::is_empty)
        {
            return Err("resource snapshot omitted runtime identity".to_owned());
        }
        if let Some(previous) = self.resource_snapshots.get(&expected_node) {
            let previous_generation = previous["generation"].as_u64().unwrap_or(u64::MAX);
            if generation < previous_generation
                || (generation == previous_generation
                    && sequence <= previous["sample_sequence"].as_u64().unwrap_or(u64::MAX))
            {
                return Err(format!(
                    "stale resource snapshot for node {expected_node}: generation {generation}, sequence {sequence}"
                ));
            }
            if generation == previous_generation
                && previous["iroh_node_id"] != resources["iroh_node_id"]
            {
                return Err(format!(
                    "resource snapshot changed runtime identity within generation for node {expected_node}"
                ));
            }
        }
        let actors = resources
            .get("actors")
            .and_then(Value::as_array)
            .ok_or_else(|| "resource snapshot omitted actor census".to_owned())?;
        for actor in actors {
            if actor.get("address").and_then(Value::as_str).is_none()
                || actor.get("actor_type").and_then(Value::as_str).is_none()
                || actor.get("poisoned").and_then(Value::as_bool).is_none()
                || actor.get("stopping").and_then(Value::as_bool).is_none()
                || actor.get("mailbox_depth").and_then(Value::as_u64).is_none()
                || actor.get("worker_id").and_then(Value::as_u64).is_none()
            {
                return Err("resource snapshot has an unidentified actor".to_owned());
            }
        }
        for field in ["live_bytes", "active_leases", "pending_leases"] {
            if resources
                .get("arena")
                .and_then(|arena| arena.get(field))
                .and_then(Value::as_u64)
                .is_none()
            {
                return Err(format!("resource snapshot omitted arena {field}"));
            }
        }
        for field in LIVE_TRANSPORT_FIELDS {
            if resources
                .get("transport")
                .and_then(|transport| transport.get(field))
                .and_then(Value::as_u64)
                .is_none()
            {
                return Err(format!("resource snapshot omitted transport {field}"));
            }
        }
        if contains_poison(resources) {
            self.poisoned.push(
                json!({"stream": format!("{expected_node}#{generation}"), "payload": resources}),
            );
        }
        self.resource_snapshots
            .insert(expected_node, resources.clone());
        Ok(())
    }

    pub(crate) fn record_orchestrator_snapshot(
        &mut self,
        stream: &str,
        request_id: &str,
        reply: &Value,
    ) -> Result<(), String> {
        if reply.get("schema_version").and_then(Value::as_u64) != Some(1)
            || reply.get("request_id").and_then(Value::as_str) != Some(request_id)
        {
            return Err("fresh orchestrator actor snapshot identity mismatch".to_owned());
        }
        let actors = reply
            .get("actors")
            .and_then(Value::as_array)
            .ok_or_else(|| "orchestrator snapshot omitted actors".to_owned())?;
        for actor in actors {
            if actor.get("address").and_then(Value::as_str).is_none()
                || actor.get("actor_type").and_then(Value::as_str).is_none()
                || actor.get("poisoned").and_then(Value::as_bool).is_none()
                || actor.get("stopping").and_then(Value::as_bool).is_none()
                || actor.get("mailbox_depth").and_then(Value::as_u64).is_none()
                || actor.get("worker_id").and_then(Value::as_u64).is_none()
            {
                return Err("orchestrator snapshot contains an unidentified actor".to_owned());
            }
        }
        if self
            .orchestrator_snapshot
            .as_ref()
            .is_some_and(|(_, previous)| {
                previous.get("request_id").and_then(Value::as_str) == Some(request_id)
            })
        {
            return Err("orchestrator snapshot request identity was reused".to_owned());
        }
        if contains_poison(reply) {
            self.poisoned
                .push(json!({"stream": stream, "payload": reply}));
        }
        self.orchestrator_snapshot = Some((stream.to_owned(), reply.clone()));
        Ok(())
    }

    pub(crate) fn ingest(&mut self, text: &str) -> Result<(), String> {
        for line in text.lines() {
            self.next_line = self.next_line.saturating_add(1);
            let frame: Value = serde_json::from_str(line)
                .map_err(|error| format!("decode telemetry frame {}: {error}", self.next_line))?;
            let channel = frame
                .get("channel")
                .and_then(Value::as_str)
                .unwrap_or_default();
            if channel != "runtime.actors" && channel != "mvp.arena" {
                continue;
            }
            let stream = frame
                .get("stream")
                .and_then(Value::as_str)
                .unwrap_or("unknown");
            let archived_payload = frame.pointer("/payload/value").ok_or_else(|| {
                format!("telemetry frame {} has no decoded payload", self.next_line)
            })?;
            let payload = match archived_payload {
                Value::String(payload) => {
                    serde_json::from_str::<Value>(payload).map_err(|error| {
                        format!("decode telemetry payload {}: {error}", self.next_line)
                    })?
                }
                payload => payload.clone(),
            };
            if channel == "runtime.actors" {
                let generation = payload
                    .get("generation")
                    .and_then(Value::as_u64)
                    .ok_or_else(|| format!("actor telemetry {stream} omitted generation"))?;
                let sequence = payload
                    .get("sequence")
                    .and_then(Value::as_u64)
                    .ok_or_else(|| format!("actor telemetry {stream} omitted sequence"))?;
                if let Some(&(previous_generation, previous_sequence)) =
                    self.lifecycle_sequences.get(stream)
                {
                    if generation != previous_generation
                        || previous_sequence.checked_add(1) != Some(sequence)
                    {
                        self.telemetry_gaps.push(format!("actor telemetry {stream}: ({previous_generation}, {previous_sequence}) -> ({generation}, {sequence})"));
                    }
                } else if sequence != 1 {
                    self.telemetry_gaps.push(format!("actor telemetry {stream} begins after missing sequence 1 (observed {sequence})"));
                }
                self.lifecycle_sequences
                    .insert(stream.to_owned(), (generation, sequence));
            }
            self.source_positions.insert(
                format!("{stream}/{channel}"),
                json!({
                    "stream": stream,
                    "generation": payload.get("generation"),
                    "sequence": payload.get("sequence"),
                    "archive_position": frame.get("position"),
                    "archive_line": self.next_line,
                }),
            );
            if contains_poison(&payload) {
                self.poisoned
                    .push(json!({"stream": stream, "payload": payload}));
            }
            if channel == "mvp.arena" {
                self.arenas.insert(stream.to_owned(), payload);
                continue;
            }
            let Some(event) = payload.get("event").and_then(Value::as_str) else {
                continue;
            };
            let Some(address) = payload.pointer("/actor/address").and_then(Value::as_str) else {
                continue;
            };
            let key = format!("{stream}/{address}");
            match event {
                "started" => {
                    if let Some(actor_type) =
                        payload.pointer("/actor/actor_type").and_then(Value::as_str)
                    {
                        self.active.insert(key, actor_type.to_owned());
                    }
                }
                "stopped" => {
                    self.active.remove(&key);
                }
                _ => {}
            }
        }
        Ok(())
    }

    pub(crate) fn forget_stream(&mut self, stream: &str) {
        let prefix = format!("{stream}/");
        self.active
            .retain(|identity, _| !identity.starts_with(&prefix));
        self.arenas.remove(stream);
        self.source_positions
            .retain(|identity, _| !identity.starts_with(&prefix));
        self.lifecycle_sequences.remove(stream);
        if self
            .orchestrator_snapshot
            .as_ref()
            .is_some_and(|(current, _)| current == stream)
        {
            self.orchestrator_snapshot = None;
        }
    }

    pub(crate) fn snapshot(&self) -> Value {
        self.snapshot_filtered(None)
    }

    pub(crate) fn snapshot_for_nodes(&self, live_nodes: &BTreeSet<u64>) -> Value {
        self.snapshot_filtered(Some(live_nodes))
    }

    fn snapshot_filtered(&self, live_nodes: Option<&BTreeSet<u64>>) -> Value {
        let started = Instant::now();
        let include_stream = |stream: &str| {
            live_nodes.is_none_or(|live| {
                stream
                    .split_once('#')
                    .and_then(|(node, _)| node.parse::<u64>().ok())
                    .is_none_or(|node| live.contains(&node))
            })
        };
        let superseded_node = |stream: &str| {
            self.orchestrator_snapshot
                .as_ref()
                .is_some_and(|(current, _)| current == stream)
                || stream
                    .split_once('#')
                    .and_then(|(node, _)| node.parse::<u64>().ok())
                    .is_some_and(|node| self.resource_snapshots.contains_key(&node))
        };
        let transient = |actor_type: &str| {
            TRANSIENT_ACTOR_TYPES
                .iter()
                .any(|prefix| actor_type.contains(prefix))
        };
        let mut active_actors = self
            .active
            .iter()
            .filter(|(identity, actor_type)| {
                let stream = identity.split('/').next().unwrap_or(identity);
                include_stream(stream) && !superseded_node(stream) && transient(actor_type)
            })
            .map(|(identity, actor_type)| json!({"identity": identity, "type": actor_type}))
            .collect::<Vec<_>>();
        let mut arenas = self
            .arenas
            .iter()
            .filter(|(stream, _)| include_stream(stream) && !superseded_node(stream))
            .map(|(stream, arena)| (stream.clone(), arena.clone()))
            .collect::<BTreeMap<_, _>>();
        let mut resource_snapshots = BTreeMap::new();
        for (node, snapshot) in &self.resource_snapshots {
            if live_nodes.is_some_and(|live| !live.contains(node)) {
                continue;
            }
            resource_snapshots.insert(node, snapshot);
            let stream = format!("{node}#{}", snapshot["generation"]);
            arenas.insert(stream.clone(), snapshot["arena"].clone());
            for actor in snapshot["actors"].as_array().into_iter().flatten() {
                if let (Some(address), Some(actor_type)) =
                    (actor["address"].as_str(), actor["actor_type"].as_str())
                {
                    if transient(actor_type) {
                        active_actors.push(
                            json!({"identity": format!("{stream}/{address}"), "type": actor_type}),
                        );
                    }
                }
            }
        }
        if let Some((stream, snapshot)) = &self.orchestrator_snapshot {
            for actor in snapshot["actors"].as_array().into_iter().flatten() {
                if let (Some(address), Some(actor_type)) =
                    (actor["address"].as_str(), actor["actor_type"].as_str())
                {
                    if transient(actor_type) {
                        active_actors.push(
                            json!({"identity": format!("{stream}/{address}"), "type": actor_type}),
                        );
                    }
                }
            }
        }
        let value = json!({
            "schema_version": 2,
            "active_actors": active_actors,
            "arenas": arenas,
            "poisoned": &self.poisoned,
            "source_positions": &self.source_positions,
            "resource_snapshots": resource_snapshots,
            "orchestrator_stream": self.orchestrator_snapshot.as_ref().map(|(stream, _)| stream),
            "orchestrator_snapshot": self.orchestrator_snapshot.as_ref().map(|(_, snapshot)| snapshot),
            "telemetry_gaps": &self.telemetry_gaps,
            "archive_offset": self.offset,
            "archive_records": self.next_line,
        });
        record_execution_stage("telemetry.snapshot", started.elapsed(), 0, 1);
        value
    }
}

pub(crate) fn pending_resource_cleanup(
    health: &Value,
    resource_baseline: &BTreeSet<String>,
) -> Option<String> {
    use myelin_control_contract::{ContextualHealthEvent, ContextualHealthReply};
    let inspect = || -> Result<(), String> {
        if health.get("schema_version").and_then(Value::as_u64) != Some(2) {
            return Err("versioned health snapshot".to_owned());
        }
        let generation = health["generation"]
            .as_u64()
            .ok_or("health observation generation")?;
        let boundary = health["health_boundary"]
            .as_u64()
            .ok_or("terminal health boundary")?;
        if generation <= boundary {
            return Err("resource observation newer than terminal health boundary".to_owned());
        }
        let running: Vec<u64> = serde_json::from_value(health["running_nodes"].clone())
            .map_err(|error| format!("exact running node identities: {error}"))?;
        let running_nodes = running.iter().copied().collect::<BTreeSet<_>>();
        if running_nodes.is_empty() || running_nodes.len() != running.len() {
            return Err("nonempty unique running node identities".to_owned());
        }
        let nodes: Vec<ContextualHealthReply> = serde_json::from_value(health["nodes"].clone())
            .map_err(|error| format!("typed successful contextual replies: {error}"))?;
        let mut observed = BTreeSet::new();
        for reply in &nodes {
            let observation = &reply.observation;
            let node = observation.logical_node_id;
            reply.validate(node, &observation.request_id)?;
            if !running_nodes.contains(&node) || !observed.insert(node) {
                return Err(format!("unexpected or duplicate contextual node {node}"));
            }
            let ContextualHealthEvent::LiveExecutions {
                executions,
                resources,
            } = &observation.event;
            if !executions.is_empty() {
                return Err(format!(
                    "{} contextual processes on node {node} to exit",
                    executions.len()
                ));
            }
            let recorded = health
                .pointer(&format!("/resources/resource_snapshots/{node}"))
                .ok_or_else(|| format!("fresh producer resource snapshot for node {node}"))?;
            if serde_json::to_value(resources).map_err(|error| error.to_string())? != *recorded {
                return Err(format!(
                    "resource snapshot does not match contextual barrier for node {node}"
                ));
            }
            for (field, count) in [
                ("live_bytes", resources.arena.live_bytes),
                ("active_leases", resources.arena.active_leases),
                ("pending_leases", resources.arena.pending_leases),
                ("pending_sinks", resources.transport.pending_sinks),
                ("pending_inbound", resources.transport.pending_inbound),
                ("active_controls", resources.transport.active_controls),
                ("source_probes", resources.transport.source_probes),
                ("sink_probes", resources.transport.sink_probes),
                ("local_incarnations", resources.transport.local_incarnations),
            ] {
                if count != 0 {
                    return Err(format!(
                        "node {node} {field} to reach zero (observed {count})"
                    ));
                }
            }
        }
        if observed != running_nodes {
            return Err(format!(
                "contextual replies for every exact running node: expected {running_nodes:?}, observed {observed:?}"
            ));
        }
        let gaps = health
            .pointer("/resources/telemetry_gaps")
            .and_then(Value::as_array)
            .ok_or("telemetry continuity proof")?;
        if !gaps.is_empty() {
            return Err(format!("telemetry lifecycle gaps: {gaps:?}"));
        }
        let actors = health
            .pointer("/resources/active_actors")
            .and_then(Value::as_array)
            .ok_or("fresh transient actor census")?;
        for actor in actors {
            let identity = actor["identity"].as_str().ok_or("actor identity")?;
            actor["type"]
                .as_str()
                .filter(|value| !value.is_empty())
                .ok_or("actor type")?;
            if resource_baseline.contains(identity) {
                continue;
            }
            return Err(format!("transient actor {identity} to stop"));
        }
        if contains_poison(health) {
            return Err("poisoned actor or worker panic".to_owned());
        }
        Ok(())
    };
    inspect().err()
}

pub(crate) fn contains_poison(value: &Value) -> bool {
    match value {
        Value::Object(fields) => fields.iter().any(|(key, value)| {
            (key == "poisoned" && value == &Value::Bool(true))
                || (key == "panics" && value.as_u64().is_some_and(|count| count != 0))
                || contains_poison(value)
        }),
        Value::Array(values) => values.iter().any(contains_poison),
        _ => false,
    }
}

pub(crate) fn list_containers(prefix: &str, budget: &Budget) -> Result<Vec<String>, String> {
    Ok(container_census(prefix, budget)?.into_values().collect())
}

pub(crate) fn container_census(
    prefix: &str,
    budget: &Budget,
) -> Result<BTreeMap<String, String>, String> {
    let output = run_output_budget(
        Command::new("docker").args([
            "ps",
            "-a",
            "--no-trunc",
            "--filter",
            &format!("name=^{prefix}"),
            "--format",
            "{{.Names}}\t{{.ID}}",
        ]),
        "list harness containers",
        budget,
    )?;
    let mut census = BTreeMap::new();
    let mut identities = BTreeSet::new();
    for line in String::from_utf8_lossy(&output.stdout).lines() {
        let (name, id) = line.split_once('\t').ok_or("invalid Docker census row")?;
        if name.is_empty()
            || id.len() != 64
            || !id.bytes().all(|byte| byte.is_ascii_hexdigit())
            || !identities.insert(id.to_owned())
            || census.insert(name.to_owned(), id.to_owned()).is_some()
        {
            return Err(
                "Docker census omitted or repeated a complete container identity".to_owned(),
            );
        }
    }
    Ok(census)
}

pub(crate) fn remove_container(container: &str, budget: &Budget) -> Result<(), String> {
    run_checked(
        Command::new("docker").args(["rm", "-f", container]),
        "remove harness container",
        budget,
    )
}

pub(crate) fn remove_containers(prefix: &str, budget: &Budget) -> Result<(), String> {
    let ids = list_containers(prefix, budget)?;
    if ids.is_empty() {
        return Ok(());
    }
    run_checked(
        Command::new("docker").arg("rm").arg("-f").args(ids),
        "remove harness containers",
        budget,
    )
}

/// A versioned deployment bundle: outer tar path plus the deployment
/// identity it declares.
#[derive(Clone, Debug)]
pub struct DeploymentBundle {
    pub tar_path: std::path::PathBuf,
    pub artifact_digest: String,
    pub executable_digest: String,
    pub deployment_generation: String,
}

fn sha256_hex_budget(path: &Path, budget: &Budget) -> Result<String, String> {
    use sha2::{Digest, Sha256};
    let started = Instant::now();
    let mut file =
        fs::File::open(path).map_err(|error| format!("hash {}: {error}", path.display()))?;
    let mut digest = Sha256::new();
    let mut buffer = [0_u8; 65536];
    let mut bytes = 0_u64;
    loop {
        budget.check("verify immutable artifact digest")?;
        let count = file.read(&mut buffer).map_err(|error| error.to_string())?;
        if count == 0 {
            break;
        }
        digest.update(&buffer[..count]);
        bytes += count as u64;
    }
    record_execution_stage("artifact.digest", started.elapsed(), bytes, 1);
    Ok(format!("{:x}", digest.finalize()))
}

/// One-time payload staging: release binaries plus the Swactor wheel. The
/// same payload backs multiple bundle generations; only the descriptor
/// changes between them.
pub fn stage_deployment_payload(
    workspace: &Path,
    artifacts: &Path,
    budget: &Budget,
) -> Result<PathBuf, String> {
    let required = required_artifacts(budget)?;
    let started = Instant::now();
    let binaries = resolve_myelin_binaries(workspace, budget)?;
    let cache_root = workspace
        .canonicalize()
        .map_err(|error| error.to_string())?
        .join("target/myelin-e2e-cache");
    let venv = cache_root.join("maturin-venv");
    let maturin = venv.join("bin/maturin");
    if !maturin.is_file() {
        if required.is_some() {
            return Err(
                "qualified wheel builder is missing; installation is prohibited".to_owned(),
            );
        }
        run_checked(
            Command::new("python3").args(["-m", "venv"]).arg(&venv),
            "create maturin virtualenv",
            budget,
        )?;
        run_checked(
            Command::new(venv.join("bin/pip"))
                .args(["install", "--quiet", "maturin>=1.7,<2"])
                .current_dir(workspace),
            "install maturin",
            budget,
        )?;
    }
    let key = wheel_input_digest(&binaries, &maturin, budget)?;
    if required
        .as_ref()
        .is_some_and(|expected| expected.wheel_input_digest != key)
    {
        return Err("qualified wheel build inputs changed; rebuilding is prohibited".to_owned());
    }
    let wheel_dir = cache_root.join("wheels").join(&key);
    fs::create_dir_all(&wheel_dir).map_err(|error| format!("create wheel cache: {error}"))?;
    let manifest = wheel_dir.join("digests.json");
    let cached: Option<BTreeMap<String, String>> = fs::File::open(&manifest)
        .ok()
        .and_then(|file| serde_json::from_reader(BufReader::new(file)).ok());
    let mut wheels = BTreeMap::new();
    if let Some(cached) = cached {
        for (name, digest) in cached {
            validate_artifact_filename(&name, ".whl")?;
            if sha256_hex_budget(&wheel_dir.join(&name), budget)
                .ok()
                .as_ref()
                == Some(&digest)
            {
                wheels.insert(name, digest);
            } else {
                wheels.clear();
                break;
            }
        }
    }
    if wheels.is_empty() {
        if required.is_some() {
            return Err(
                "qualified wheel cache is missing or corrupt; rebuilding is prohibited".to_owned(),
            );
        }
        let temporary = tempfile::tempdir_in(&wheel_dir).map_err(|error| error.to_string())?;
        run_checked(
            Command::new(&maturin)
                .args([
                    "build",
                    "--release",
                    "--locked",
                    "--interpreter",
                    "python3",
                    "--manifest-path",
                    "crates/bindings/python/Cargo.toml",
                    "--out",
                ])
                .arg(temporary.path())
                .current_dir(workspace),
            "build Swactor wheel",
            budget,
        )?;
        for entry in fs::read_dir(temporary.path()).map_err(|error| error.to_string())? {
            let entry = entry.map_err(|error| error.to_string())?;
            let name = entry.file_name().to_string_lossy().into_owned();
            if name.ends_with(".whl") {
                let digest = sha256_hex_budget(&entry.path(), budget)?;
                copy_atomic(&entry.path(), &wheel_dir.join(&name))?;
                wheels.insert(name, digest);
            }
        }
        if wheels.is_empty() {
            return Err("maturin produced no wheel".to_owned());
        }
        write_json_atomic(&manifest, &wheels)?;
        record_execution_stage(
            "build.wheel.miss",
            started.elapsed(),
            0,
            wheels.len() as u64,
        );
    } else {
        record_execution_stage(
            "build.wheel.cache_hit",
            started.elapsed(),
            0,
            wheels.len() as u64,
        );
    }
    let identity =
        prepared_artifact_identity(workspace, &binaries, key.clone(), wheels.clone(), budget)?;
    if required
        .as_ref()
        .is_some_and(|expected| expected != &identity)
    {
        return Err("staged payload differs from qualified artifacts".to_owned());
    }
    record_execution_identity(
        &format!("wheel:{key}"),
        json!({
            "schema_version": 1, "source_build_input_digest": binaries.input_digest,
            "wheel_input_digest": key, "wheels": wheels,
        }),
    );
    // Each caller receives an independent mutable staging tree: generation
    // marker appends must never mutate the immutable cache or another round.
    fs::create_dir_all(artifacts).map_err(|error| error.to_string())?;
    let payload = tempfile::Builder::new()
        .prefix("payload-")
        .tempdir_in(artifacts)
        .map_err(|error| error.to_string())?;
    let payload_bin = payload.path().join("bin");
    let payload_python = payload.path().join("python");
    fs::create_dir_all(&payload_bin).map_err(|error| error.to_string())?;
    fs::create_dir_all(&payload_python).map_err(|error| error.to_string())?;
    copy_atomic(&binaries.worker, &payload_bin.join("myelin-worker"))?;
    set_executable(&payload_bin.join("myelin-worker"))?;
    copy_atomic(
        &workspace.join("apps/myelin/node-image/e2e_python_launcher.py"),
        &payload_bin.join("myelin-e2e-python"),
    )?;
    set_executable(&payload_bin.join("myelin-e2e-python"))?;
    for name in wheels.keys() {
        budget.check("stage Swactor wheel")?;
        copy_atomic(&wheel_dir.join(name), &payload_python.join(name))?;
    }
    // Hash the copied bytes as well: cache/source changes during staging cannot pass.
    for (name, expected) in &identity.payload {
        if sha256_hex_budget(&payload.path().join(name), budget)? != *expected {
            return Err(format!("deployment payload changed during staging: {name}"));
        }
    }
    write_json_atomic(&artifacts.join("build-identity.json"), &identity)?;
    record_execution_stage("deployment.stage", started.elapsed(), 0, 1);
    Ok(payload.keep())
}

/// Produces a byte-distinct but executable-compatible worker artifact. ELF
/// loaders ignore trailing bytes; the marker is therefore part of the tested
/// executable digest without introducing a second build configuration.
pub fn distinguish_deployment_payload(
    payload: &Path,
    deployment_generation: &str,
) -> Result<(), String> {
    let worker = payload.join("bin").join("myelin-worker");
    let mut file = std::fs::OpenOptions::new()
        .append(true)
        .open(&worker)
        .map_err(|error| format!("open {} for deployment marker: {error}", worker.display()))?;
    file.write_all(format!("\nMYELIN_DEPLOYMENT_ARTIFACT={deployment_generation}\n").as_bytes())
        .map_err(|error| format!("mark {} as distinct deployment: {error}", worker.display()))?;
    file.sync_all()
        .map_err(|error| format!("sync deployment marker in {}: {error}", worker.display()))
}

#[cfg(unix)]
fn set_executable(path: &Path) -> Result<(), String> {
    use std::os::unix::fs::PermissionsExt;
    let metadata =
        std::fs::metadata(path).map_err(|error| format!("inspect {}: {error}", path.display()))?;
    let mut permissions = metadata.permissions();
    permissions.set_mode(0o755);
    std::fs::set_permissions(path, permissions)
        .map_err(|error| format!("mark {} executable: {error}", path.display()))
}

#[cfg(not(unix))]
fn set_executable(_path: &Path) -> Result<(), String> {
    Ok(())
}

/// Deterministic payload tar: identical payload bytes hash identically
/// across invocations.
fn write_payload_tar(
    artifacts: &Path,
    payload: &Path,
    budget: &Budget,
) -> Result<(PathBuf, String), String> {
    use sha2::{Digest, Sha256};
    let started = Instant::now();
    let mut key = Sha256::new();
    key.update(b"myelin-e2e-payload-ustar-v1\0");
    for name in ["bin", "python"] {
        hash_tree(payload, &payload.join(name), &mut key, budget)?;
    }
    let key = format!("{:x}", key.finalize());
    let cache = artifacts
        .canonicalize()
        .map_err(|error| error.to_string())?
        .join("deployment-cache")
        .join(key);
    fs::create_dir_all(&cache).map_err(|error| error.to_string())?;
    let payload_tar = cache.join("payload.tar");
    let manifest = cache.join("digest.json");
    if let Ok(file) = fs::File::open(&manifest) {
        if let Ok(expected) = serde_json::from_reader::<_, String>(BufReader::new(file)) {
            if sha256_hex_budget(&payload_tar, budget).ok().as_ref() == Some(&expected) {
                record_execution_stage("deployment.payload.cache_hit", started.elapsed(), 0, 1);
                return Ok((payload_tar, expected));
            }
        }
    }
    let temporary = tempfile::NamedTempFile::new_in(&cache).map_err(|error| error.to_string())?;
    run_checked(
        Command::new("tar")
            .args([
                "--format=ustar",
                "--sort=name",
                "--mtime=@0",
                "--owner=0",
                "--group=0",
                "--numeric-owner",
                "-cf",
            ])
            .arg(temporary.path())
            .args(["bin", "python"])
            .current_dir(payload),
        "pack deployment payload",
        budget,
    )?;
    let digest = sha256_hex_budget(temporary.path(), budget)?;
    temporary
        .persist(&payload_tar)
        .map_err(|error| error.to_string())?;
    write_json_atomic(&manifest, &digest)?;
    record_execution_stage(
        "deployment.payload.miss",
        started.elapsed(),
        fs::metadata(&payload_tar)
            .map_err(|error| error.to_string())?
            .len(),
        1,
    );
    Ok((payload_tar, digest))
}

/// Assembles `deployment.tar` (descriptor + payload) for one generation.
/// The digest covers the payload only, so a new generation with identical
/// binaries keeps the digest and still forces a node refresh.
pub fn assemble_deployment_bundle(
    artifacts: &Path,
    payload: &Path,
    deployment_generation: &str,
    budget: &Budget,
) -> Result<DeploymentBundle, String> {
    use sha2::{Digest, Sha256};
    let started = Instant::now();
    if deployment_generation.is_empty()
        || !deployment_generation
            .bytes()
            .all(|byte| byte.is_ascii_alphanumeric() || matches!(byte, b'-' | b'_' | b'.'))
    {
        return Err("deployment generation must be a nonempty filename-safe identity".to_owned());
    }
    let (payload_tar, digest) = write_payload_tar(artifacts, payload, budget)?;
    let executable_digest = format!(
        "sha256:{}",
        sha256_hex_budget(&payload.join("bin/myelin-worker"), budget)?
    );
    let descriptor = json!({
        "artifact_digest": format!("sha256:{digest}"),
        "deployment_generation": deployment_generation,
        "executable_digest": executable_digest,
    });
    let descriptor_bytes = serde_json::to_vec(&descriptor).map_err(|error| error.to_string())?;
    let descriptor_digest = format!("{:x}", Sha256::digest(&descriptor_bytes));
    let artifacts = artifacts
        .canonicalize()
        .map_err(|error| error.to_string())?;
    let bundle_tar = artifacts.join(format!(
        "deployment-{deployment_generation}-{descriptor_digest}.tar"
    ));
    let manifest = bundle_tar.with_extension("digest.json");
    let valid_cached = fs::File::open(&manifest)
        .ok()
        .and_then(|file| serde_json::from_reader::<_, String>(BufReader::new(file)).ok())
        .is_some_and(|expected| {
            sha256_hex_budget(&bundle_tar, budget).ok().as_ref() == Some(&expected)
        });
    if !valid_cached {
        let descriptor_dir = tempfile::tempdir_in(&artifacts).map_err(|error| error.to_string())?;
        fs::write(
            descriptor_dir.path().join("deployment.json"),
            descriptor_bytes,
        )
        .map_err(|error| error.to_string())?;
        let temporary =
            tempfile::NamedTempFile::new_in(&artifacts).map_err(|error| error.to_string())?;
        run_checked(
            Command::new("tar")
                .args([
                    "--format=ustar",
                    "--sort=name",
                    "--mtime=@0",
                    "--owner=0",
                    "--group=0",
                    "--numeric-owner",
                    "-cf",
                ])
                .arg(temporary.path())
                .arg("-C")
                .arg(descriptor_dir.path())
                .arg("deployment.json")
                .arg("-C")
                .arg(
                    payload_tar
                        .parent()
                        .ok_or_else(|| "payload cache has no parent".to_owned())?,
                )
                .arg("payload.tar"),
            "pack deployment bundle",
            budget,
        )?;
        let bundle_digest = sha256_hex_budget(temporary.path(), budget)?;
        temporary
            .persist(&bundle_tar)
            .map_err(|error| error.to_string())?;
        write_json_atomic(&manifest, &bundle_digest)?;
    }
    record_execution_stage(
        if valid_cached {
            "deployment.bundle.cache_hit"
        } else {
            "deployment.bundle.miss"
        },
        started.elapsed(),
        fs::metadata(&bundle_tar)
            .map_err(|error| error.to_string())?
            .len(),
        1,
    );
    Ok(DeploymentBundle {
        tar_path: bundle_tar,
        artifact_digest: format!("sha256:{digest}"),
        executable_digest,
        deployment_generation: deployment_generation.to_owned(),
    })
}

#[cfg(test)]
pub(crate) mod resource_deadline_tests {
    use super::*;
    use std::sync::mpsc;

    pub(crate) fn unused_binaries_for_control_peer() -> BuiltBinaries {
        // Collector tests own a real loopback peer, but never build or relaunch
        // a fixture. These paths must remain unusable if that contract changes.
        BuiltBinaries {
            orchestrator: PathBuf::new(),
            worker: PathBuf::new(),
            input_digest: String::new(),
            executable_digests: BTreeMap::new(),
            verified_files: BTreeMap::new(),
        }
    }

    fn accept_with_deadline(listener: &TcpListener) -> (TcpStream, SocketAddr) {
        listener.set_nonblocking(true).unwrap();
        let budget = Budget::new(Duration::from_secs(2));
        loop {
            match listener.accept() {
                Ok((stream, address)) => {
                    stream.set_nonblocking(false).unwrap();
                    return (stream, address);
                }
                Err(error) if error.kind() == std::io::ErrorKind::WouldBlock => {
                    budget
                        .wait(Duration::from_millis(1), "test client connects")
                        .unwrap();
                }
                Err(error) => panic!("accept test connection: {error}"),
            }
        }
    }

    fn read_request(stream: &mut TcpStream) {
        stream
            .set_read_timeout(Some(Duration::from_secs(2)))
            .unwrap();
        let mut request = Vec::new();
        let mut byte = [0_u8; 1];
        while !request.ends_with(b"\r\n\r\n") {
            stream.read_exact(&mut byte).unwrap();
            request.push(byte[0]);
        }
    }

    #[test]
    fn withheld_dashboard_response_expires_and_releases_connection() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
        let address = listener.local_addr().unwrap();
        let (stop, stopped) = mpsc::channel();
        let server = thread::spawn(move || {
            let (mut stream, _) = accept_with_deadline(&listener);
            read_request(&mut stream);
            let _ = stopped.recv_timeout(Duration::from_secs(3));
        });
        let started = Instant::now();
        let error = http_json_budget(
            "GET",
            &format!("http://{address}/withheld"),
            None,
            &Budget::new(Duration::from_millis(150)),
        )
        .unwrap_err();
        let elapsed = started.elapsed();
        let _ = stop.send(());
        server.join().unwrap();
        assert!(
            error.contains("pending predicate: HTTP GET /withheld"),
            "{error}"
        );
        assert!(elapsed < Duration::from_secs(1), "{elapsed:?}");
    }

    #[test]
    fn trickle_response_cannot_renew_total_deadline() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
        let address = listener.local_addr().unwrap();
        let (stop, stopped) = mpsc::channel();
        let server = thread::spawn(move || {
            let (mut stream, _) = accept_with_deadline(&listener);
            read_request(&mut stream);
            stream
                .write_all(b"HTTP/1.1 200 OK\r\nContent-Length: 10000\r\n\r\n")
                .unwrap();
            for _ in 0..200 {
                if stream.write_all(b" ").is_err() {
                    break;
                }
                if stopped.recv_timeout(Duration::from_millis(10)).is_ok() {
                    break;
                }
            }
        });
        let started = Instant::now();
        let error = http_json_budget(
            "GET",
            &format!("http://{address}/trickle"),
            None,
            &Budget::new(Duration::from_millis(150)),
        )
        .unwrap_err();
        let elapsed = started.elapsed();
        let _ = stop.send(());
        server.join().unwrap();
        assert!(
            error.contains("pending predicate: HTTP GET /trickle"),
            "{error}"
        );
        assert!(elapsed < Duration::from_secs(1), "{elapsed:?}");
    }

    #[test]
    fn framed_response_finishes_without_waiting_for_peer_close() {
        for response in [
            b"HTTP/1.1 200 OK\r\nContent-Length: 11\r\n\r\n{\"ok\":true}".as_slice(),
            b"HTTP/1.1 200 OK\r\nTransfer-Encoding: chunked\r\n\r\nb\r\n{\"ok\":true}\r\n0\r\n\r\n"
                .as_slice(),
        ] {
            let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
            let address = listener.local_addr().unwrap();
            let (stop, stopped) = mpsc::channel();
            let server = thread::spawn(move || {
                let (mut stream, _) = accept_with_deadline(&listener);
                read_request(&mut stream);
                stream.write_all(response).unwrap();
                let _ = stopped.recv_timeout(Duration::from_secs(3));
            });
            let reply = http_json_budget(
                "GET",
                &format!("http://{address}/framed"),
                None,
                &Budget::new(Duration::from_millis(500)),
            );
            let _ = stop.send(());
            server.join().unwrap();
            assert_eq!(reply.unwrap(), json!({"ok": true}));
        }
    }

    #[test]
    fn control_restart_reconciles_with_new_connection() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
        let address = listener.local_addr().unwrap();
        let server = thread::spawn(move || {
            let (mut disconnected, _) = accept_with_deadline(&listener);
            read_request(&mut disconnected);
            drop(disconnected);
            for generation in [2, 3] {
                let (mut stream, _) = accept_with_deadline(&listener);
                read_request(&mut stream);
                let body = json!({"generation": generation}).to_string();
                write!(
                    stream,
                    "HTTP/1.1 200 OK\r\nContent-Length: {}\r\n\r\n{body}",
                    body.len()
                )
                .unwrap();
            }
        });
        let budget = Budget::new(Duration::from_secs(2));
        assert_eq!(
            http_json_budget("GET", &format!("http://{address}/status"), None, &budget).unwrap(),
            json!({"generation": 2})
        );
        assert_eq!(
            http_json_budget("GET", &format!("http://{address}/status"), None, &budget).unwrap(),
            json!({"generation": 3})
        );
        server.join().unwrap();
    }

    #[test]
    fn ambiguous_post_is_not_duplicated_by_transport() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
        let address = listener.local_addr().unwrap();
        let (stop, stopped) = mpsc::channel();
        let server = thread::spawn(move || {
            let (mut stream, _) = accept_with_deadline(&listener);
            read_request(&mut stream);
            drop(stream);
            let _ = stopped.recv_timeout(Duration::from_secs(2));
            listener.set_nonblocking(true).unwrap();
            assert!(listener.accept().is_err(), "ambiguous mutation was retried");
        });
        let result = http_json_budget(
            "POST",
            &format!("http://{address}/spawn"),
            Some(json!({})),
            &Budget::new(Duration::from_millis(500)),
        );
        let _ = stop.send(());
        server.join().unwrap();
        assert!(result.is_err());
    }

    #[test]
    fn retained_resource_post_retries_gateway_timeout() {
        let listener = TcpListener::bind(("127.0.0.1", 0)).unwrap();
        let address = listener.local_addr().unwrap();
        let server = thread::spawn(move || {
            for (status, body) in [
                ("504 Gateway Timeout", json!({"error": "deadline"})),
                ("200 OK", json!({"ok": true})),
            ] {
                let (mut stream, _) = accept_with_deadline(&listener);
                read_request(&mut stream);
                let body = body.to_string();
                write!(
                    stream,
                    "HTTP/1.1 {status}\r\nContent-Length: {}\r\n\r\n{body}",
                    body.len()
                )
                .unwrap();
            }
        });
        let result = http_json_budget(
            "POST",
            &format!("http://{address}/api/control/contextual/retained-blobs"),
            None,
            &Budget::new(Duration::from_secs(2)),
        );
        server.join().unwrap();
        assert_eq!(result.unwrap(), json!({"ok": true}));
    }

    fn snapshot(request: &str, sequence: u64, live_bytes: u64) -> Value {
        json!({
            "schema_version": 1, "request_id": request, "logical_node_id": 1,
            "generation": 2, "sample_sequence": sequence, "iroh_node_id": "node-1",
            "artifact_digest": null, "deployment_generation": null, "collection_elapsed_us": 1,
            "actors": [], "arena": {
                "live_bytes": live_bytes, "active_leases": 0, "pending_leases": 0,
                "seq": sequence, "sample_unix_ms": 1, "capacity_bytes": 1024, "free_bytes": 1024-live_bytes,
                "largest_free_range_bytes": 1024-live_bytes, "allocation_failures_total": 0, "release_failures_total": 0,
            },
            "transport": {
                "pending_sinks": 0, "pending_inbound": 0, "active_controls": 0,
                "source_probes": 0, "sink_probes": 0, "local_incarnations": 0,
                "retired_incarnations": 7,
            },
        })
    }

    pub(crate) fn health_reply(request: &str, node: u64, sequence: u64, live_bytes: u64) -> Value {
        let mut resources = snapshot(request, sequence, live_bytes);
        resources["logical_node_id"] = json!(node);
        json!({"schema_version": 1, "type": "event", "observation": {
            "logical_node_id": node, "request_id": request,
            "event": {"type": "live_executions", "executions": [], "resources": resources},
        }})
    }

    fn health(census: &TelemetryResourceCensus) -> Value {
        let resources = census.snapshot();
        let snapshot = &resources["resource_snapshots"]["1"];
        json!({
            "schema_version": 2, "generation": 3, "health_boundary": 2,
            "nodes": [{"schema_version": 1, "type": "event", "observation": {
                "logical_node_id": 1, "request_id": snapshot["request_id"],
                "event": {"type": "live_executions", "executions": [], "resources": snapshot},
            }}],
            "running_nodes": [1], "resources": resources,
        })
    }

    #[test]
    fn stale_archive_zero_cannot_replace_live_producer_snapshot() {
        let mut census = TelemetryResourceCensus::default();
        census
            .record_resource_snapshot("cleanup-1", 1, &snapshot("cleanup-1", 1, 64))
            .unwrap();
        census.ingest(&json!({
            "channel": "mvp.arena", "stream": "1#2",
            "payload": {"value": {"live_bytes": 0, "active_leases": 0, "pending_leases": 0}},
        }).to_string()).unwrap();
        let health = health(&census);
        assert!(pending_resource_cleanup(&health, &BTreeSet::new()).is_some());
        assert!(
            census
                .record_resource_snapshot("cleanup-2", 1, &snapshot("cleanup-1", 2, 0))
                .is_err()
        );
        assert!(
            census
                .record_resource_snapshot("cleanup-2", 1, &snapshot("cleanup-2", 1, 0))
                .is_err()
        );
        census
            .record_resource_snapshot("cleanup-2", 1, &snapshot("cleanup-2", 2, 0))
            .unwrap();
        let health = self::health(&census);
        assert_eq!(pending_resource_cleanup(&health, &BTreeSet::new()), None);
    }

    #[test]
    fn pending_transport_cannot_pass_an_empty_arena_cleanup_gate() {
        let mut census = TelemetryResourceCensus::default();
        let mut pending = snapshot("cleanup-transport", 1, 0);
        pending["transport"]["pending_inbound"] = json!(1);
        census
            .record_resource_snapshot("cleanup-transport", 1, &pending)
            .unwrap();
        let health = health(&census);
        assert!(pending_resource_cleanup(&health, &BTreeSet::new()).is_some());
        census
            .record_resource_snapshot("cleanup-retracted", 1, &snapshot("cleanup-retracted", 2, 0))
            .unwrap();
        let health = self::health(&census);
        assert_eq!(pending_resource_cleanup(&health, &BTreeSet::new()), None);
    }

    #[test]
    fn malformed_missing_wrong_node_and_boundary_stale_health_fail_closed() {
        let mut census = TelemetryResourceCensus::default();
        census
            .record_resource_snapshot("fresh", 1, &snapshot("fresh", 1, 0))
            .unwrap();
        let clean = health(&census);
        assert_eq!(pending_resource_cleanup(&clean, &BTreeSet::new()), None);
        let mut malformed = clean.clone();
        malformed["nodes"] = json!([{"error": "control failed"}]);
        assert!(pending_resource_cleanup(&malformed, &BTreeSet::new()).is_some());
        let mut missing = clean.clone();
        missing["nodes"] = json!([]);
        assert!(pending_resource_cleanup(&missing, &BTreeSet::new()).is_some());
        let mut wrong_node = clean.clone();
        wrong_node["nodes"][0]["observation"]["logical_node_id"] = json!(2);
        assert!(pending_resource_cleanup(&wrong_node, &BTreeSet::new()).is_some());
        let mut stale = clean;
        stale["health_boundary"] = stale["generation"].clone();
        assert!(pending_resource_cleanup(&stale, &BTreeSet::new()).is_some());
    }

    #[test]
    fn dropped_lifecycle_event_cannot_be_hidden_by_fresh_zero_snapshot() {
        let mut census = TelemetryResourceCensus::default();
        for sequence in [1, 3] {
            census.ingest(&json!({
                "stream": "1#2", "channel": "runtime.actors",
                "payload": {"value": {"generation": 2, "sequence": sequence, "event": "census", "actors": []}},
            }).to_string()).unwrap();
        }
        census
            .record_resource_snapshot("fresh", 1, &snapshot("fresh", 1, 0))
            .unwrap();
        assert!(pending_resource_cleanup(&health(&census), &BTreeSet::new()).is_some());
    }

    #[test]
    fn orchestrator_snapshot_and_restart_use_the_exact_nondefault_stream() {
        let mut census = TelemetryResourceCensus::default();
        let started = |stream: &str, address: &str| {
            json!({
                "stream": stream, "channel": "runtime.actors",
                "payload": {"value": {
                    "generation": 7, "sequence": 1, "event": "started",
                    "actor": {"address": address, "actor_type": "data_plane::host::DataPlaneHost"},
                }},
            })
            .to_string()
        };
        census
            .ingest(&started("myelin-orchestrator#7", "old"))
            .unwrap();
        census.ingest(&started("1#7", "worker")).unwrap();
        census
            .record_orchestrator_snapshot(
                "myelin-orchestrator#7",
                "fresh",
                &json!({"schema_version": 1, "request_id": "fresh", "actors": []}),
            )
            .unwrap();
        assert_eq!(
            census.snapshot()["active_actors"],
            json!([{
                "identity": "1#7/worker", "type": "data_plane::host::DataPlaneHost",
            }])
        );

        census.forget_stream("myelin-orchestrator#7");
        census
            .ingest(&started("myelin-orchestrator#7", "replacement"))
            .unwrap();
        let restored = census.snapshot();
        let identities = restored["active_actors"]
            .as_array()
            .unwrap()
            .iter()
            .map(|actor| actor["identity"].as_str().unwrap())
            .collect::<BTreeSet<_>>();
        assert_eq!(
            identities,
            BTreeSet::from(["1#7/worker", "myelin-orchestrator#7/replacement"])
        );
        assert_eq!(restored["telemetry_gaps"], json!([]));
    }

    #[test]
    fn payload_cache_preserves_generation_and_executable_identity() {
        let artifacts = tempfile::tempdir().unwrap();
        let payload = artifacts.path().join("payload");
        fs::create_dir_all(payload.join("bin")).unwrap();
        fs::create_dir_all(payload.join("python")).unwrap();
        fs::write(payload.join("bin/myelin-worker"), b"executable-a").unwrap();
        let budget = Budget::new(Duration::from_secs(10));
        let first = assemble_deployment_bundle(artifacts.path(), &payload, "generation-a", &budget)
            .unwrap();
        let second =
            assemble_deployment_bundle(artifacts.path(), &payload, "generation-b", &budget)
                .unwrap();
        assert_eq!(first.artifact_digest, second.artifact_digest);
        assert_ne!(first.tar_path, second.tar_path);
        distinguish_deployment_payload(&payload, "generation-c").unwrap();
        let third = assemble_deployment_bundle(artifacts.path(), &payload, "generation-c", &budget)
            .unwrap();
        assert_ne!(first.artifact_digest, third.artifact_digest);
        assert_ne!(first.executable_digest, third.executable_digest);
        let original = run_output_budget(
            Command::new("tar")
                .arg("-xOf")
                .arg(&first.tar_path)
                .arg("deployment.json"),
            "inspect first immutable descriptor",
            &budget,
        )
        .unwrap();
        let descriptor: Value = serde_json::from_slice(&original.stdout).unwrap();
        assert_eq!(descriptor["deployment_generation"], "generation-a");
        assert_eq!(descriptor["artifact_digest"], first.artifact_digest);
    }
}
