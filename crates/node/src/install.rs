//! Self-install / uninstall swactor as a system service.
//!
//! Binary is installed to `~/.swactor/bin/swactor`. The service strategy
//! is chosen automatically:
//!
//! - **systemd** → user-level service (`systemctl --user`), no root
//! - **OpenRC / sysvinit + root** → system service in `/etc/init.d/`
//! - **OpenRC / sysvinit + no root** → `@reboot` crontab entry

use std::fs;
use std::path::{Path, PathBuf};
use std::process::Command;

// ── Init system detection ───────────────────────────────────────────────

enum InitSystem {
    Systemd,
    OpenRc,
    SysVinit,
}

fn detect_init() -> InitSystem {
    if Path::new("/run/systemd/system").exists() {
        InitSystem::Systemd
    } else if Path::new("/sbin/openrc").exists() {
        InitSystem::OpenRc
    } else {
        InitSystem::SysVinit
    }
}

fn is_root() -> bool {
    #[cfg(unix)]
    {
        // SAFETY: getuid is always safe to call
        unsafe { libc::getuid() == 0 }
    }
    #[cfg(not(unix))]
    {
        false
    }
}

// ── Paths ───────────────────────────────────────────────────────────────

fn swactor_dir() -> PathBuf {
    let home = std::env::var("HOME").unwrap_or_else(|_| {
        eprintln!("$HOME is not set");
        std::process::exit(1);
    });
    PathBuf::from(home).join(".swactor")
}

fn bin_path() -> PathBuf {
    swactor_dir().join("bin").join("swactor")
}

// ── Service file templates ──────────────────────────────────────────────

fn systemd_unit(bin: &Path) -> String {
    format!(
        "\
[Unit]
Description=Swactor distributed node
After=network-online.target
Wants=network-online.target

[Service]
Type=simple
ExecStart={bin}
Restart=on-failure
RestartSec=5
KillSignal=SIGINT
TimeoutStopSec=30

[Install]
WantedBy=default.target
",
        bin = bin.display(),
    )
}

fn openrc_init(bin: &Path) -> String {
    format!(
        "\
#!/sbin/openrc-run
name=\"swactor\"
description=\"Swactor distributed node\"
command=\"{bin}\"
command_background=true
pidfile=\"/run/${{RC_SVCNAME}}.pid\"
",
        bin = bin.display(),
    )
}

fn sysvinit_script(bin: &Path) -> String {
    format!(
        r#"#!/bin/sh
### BEGIN INIT INFO
# Provides:          swactor
# Required-Start:    $network $remote_fs
# Required-Stop:     $network $remote_fs
# Default-Start:     2 3 4 5
# Default-Stop:      0 1 6
# Short-Description: Swactor distributed node
### END INIT INFO

DAEMON="{bin}"
NAME="swactor"
PIDFILE="/var/run/$NAME.pid"

case "$1" in
  start)
    echo "Starting $NAME..."
    start-stop-daemon --start --background --make-pidfile \
        --pidfile "$PIDFILE" --exec "$DAEMON"
    ;;
  stop)
    echo "Stopping $NAME..."
    start-stop-daemon --stop --pidfile "$PIDFILE" --signal INT --retry 30
    rm -f "$PIDFILE"
    ;;
  restart)
    $0 stop
    $0 start
    ;;
  status)
    if [ -f "$PIDFILE" ] && kill -0 "$(cat "$PIDFILE")" 2>/dev/null; then
      echo "$NAME is running (pid $(cat "$PIDFILE"))"
    else
      echo "$NAME is not running"
      exit 1
    fi
    ;;
  *)
    echo "Usage: $0 {{start|stop|restart|status}}"
    exit 1
    ;;
esac
"#,
        bin = bin.display(),
    )
}

// ── Helpers ─────────────────────────────────────────────────────────────

fn run(program: &str, args: &[&str]) -> bool {
    match Command::new(program).args(args).status() {
        Ok(s) => s.success(),
        Err(e) => {
            eprintln!("  failed to run {program}: {e}");
            false
        }
    }
}

fn copy_self_to_bin(dest: &Path) {
    let exe = std::env::current_exe().expect("cannot determine own executable path");
    let canonical_exe = fs::canonicalize(&exe).unwrap_or(exe);
    if canonical_exe == dest {
        eprintln!("  binary already at {}, skipping copy", dest.display());
        return;
    }
    if let Some(parent) = dest.parent() {
        fs::create_dir_all(parent).unwrap_or_else(|e| {
            eprintln!("  failed to create {}: {e}", parent.display());
            std::process::exit(1);
        });
    }
    // Unlink the existing binary first to avoid ETXTBSY when upgrading
    // a running process. On Unix the kernel keeps the old inode alive
    // until the process exits; the new copy gets a fresh inode.
    if dest.exists() {
        fs::remove_file(dest).unwrap_or_else(|e| {
            eprintln!("  failed to remove old binary: {e}");
            std::process::exit(1);
        });
    }

    eprintln!("  copying {} -> {}", canonical_exe.display(), dest.display());
    fs::copy(&canonical_exe, dest).unwrap_or_else(|e| {
        eprintln!("  failed to copy binary: {e}");
        std::process::exit(1);
    });

    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(dest, fs::Permissions::from_mode(0o755)).ok();
    }
}

fn systemd_user_dir() -> PathBuf {
    let home = std::env::var("HOME").unwrap_or_else(|_| {
        eprintln!("$HOME is not set");
        std::process::exit(1);
    });
    PathBuf::from(home)
        .join(".config")
        .join("systemd")
        .join("user")
}

fn set_executable(path: &str) {
    #[cfg(unix)]
    {
        use std::os::unix::fs::PermissionsExt;
        fs::set_permissions(path, fs::Permissions::from_mode(0o755)).ok();
    }
}

/// Read the current user crontab, returns empty string if none.
fn read_crontab() -> String {
    Command::new("crontab")
        .arg("-l")
        .output()
        .ok()
        .filter(|o| o.status.success())
        .map(|o| String::from_utf8_lossy(&o.stdout).into_owned())
        .unwrap_or_default()
}

/// Write a new crontab from the given string.
fn write_crontab(content: &str) -> bool {
    use std::io::Write;
    let mut child = match Command::new("crontab")
        .arg("-")
        .stdin(std::process::Stdio::piped())
        .spawn()
    {
        Ok(c) => c,
        Err(e) => {
            eprintln!("  failed to run crontab: {e}");
            return false;
        }
    };
    if let Some(mut stdin) = child.stdin.take() {
        let _ = stdin.write_all(content.as_bytes());
    }
    child.wait().map(|s| s.success()).unwrap_or(false)
}

fn crontab_tag(bin: &Path) -> String {
    // Restart on failure only (matches systemd Restart=on-failure).
    // Retries up to 5 times with linear backoff (5s, 10s, …, 25s),
    // then gives up. A clean exit (code 0) stops immediately.
    format!(
        "@reboot /bin/sh -c 'n=0; while [ $n -lt 5 ]; do {} && exit 0; n=$((n+1)); sleep $((n*5)); done'",
        bin.display()
    )
}

// ── Install ─────────────────────────────────────────────────────────────

pub fn install() {
    eprintln!("Installing swactor...");

    let dest = bin_path();
    copy_self_to_bin(&dest);

    match detect_init() {
        InitSystem::Systemd => install_systemd(&dest),
        InitSystem::OpenRc if is_root() => install_openrc(&dest),
        InitSystem::SysVinit if is_root() => install_sysvinit(&dest),
        _ => install_crontab(&dest),
    }

    eprintln!("Done.");
}

fn install_systemd(bin: &Path) {
    let dir = systemd_user_dir();
    fs::create_dir_all(&dir).unwrap_or_else(|e| {
        eprintln!("  failed to create {}: {e}", dir.display());
        std::process::exit(1);
    });

    let path = dir.join("swactor.service");
    eprintln!("  writing {}", path.display());
    fs::write(&path, systemd_unit(bin)).unwrap_or_else(|e| {
        eprintln!("  failed to write service file: {e}");
        std::process::exit(1);
    });

    eprintln!("  enabling and restarting user service...");
    run("systemctl", &["--user", "daemon-reload"]);
    run("systemctl", &["--user", "enable", "swactor"]);
    run("systemctl", &["--user", "restart", "swactor"]);

    // Enable lingering so the service survives logout
    if let Ok(user) = std::env::var("USER") {
        run("loginctl", &["enable-linger", &user]);
    }

    eprintln!("  systemd user service installed and started");
}

fn install_openrc(bin: &Path) {
    let path = "/etc/init.d/swactor";
    eprintln!("  writing {path}");
    fs::write(path, openrc_init(bin)).unwrap_or_else(|e| {
        eprintln!("  failed to write init script: {e}");
        std::process::exit(1);
    });
    set_executable(path);

    eprintln!("  enabling and restarting service...");
    run("rc-update", &["add", "swactor", "default"]);
    run("rc-service", &["swactor", "restart"]);

    eprintln!("  OpenRC service installed and restarted");
}

fn install_sysvinit(bin: &Path) {
    let path = "/etc/init.d/swactor";
    eprintln!("  writing {path}");
    fs::write(path, sysvinit_script(bin)).unwrap_or_else(|e| {
        eprintln!("  failed to write init script: {e}");
        std::process::exit(1);
    });
    set_executable(path);

    eprintln!("  enabling and restarting service...");
    if !run("update-rc.d", &["swactor", "defaults"]) {
        run("chkconfig", &["--add", "swactor"]);
    }
    run("/etc/init.d/swactor", &["restart"]);

    eprintln!("  sysvinit service installed and restarted");
}

fn install_crontab(bin: &Path) {
    let tag = crontab_tag(bin);
    let legacy_tag = format!(
        "@reboot /bin/sh -c 'while true; do {} ; sleep 5; done'",
        bin.display()
    );
    let existing = read_crontab();

    if existing.lines().any(|l| l.trim() == tag) {
        eprintln!("  crontab @reboot entry already exists, skipping");
    } else {
        // Remove legacy entry if present, then add the new one.
        let mut new: String = existing
            .lines()
            .filter(|l| l.trim() != legacy_tag)
            .collect::<Vec<_>>()
            .join("\n");
        if !new.is_empty() && !new.ends_with('\n') {
            new.push('\n');
        }
        new.push_str(&tag);
        new.push('\n');
        eprintln!("  adding crontab @reboot entry");
        if !write_crontab(&new) {
            eprintln!("  failed to update crontab");
            std::process::exit(1);
        }
    }

    // Stop any running instance, then start fresh
    eprintln!("  stopping old swactor (if running)...");
    run("pkill", &["-INT", "-f", &bin.to_string_lossy()]);
    // Give it a moment to exit cleanly
    std::thread::sleep(std::time::Duration::from_secs(2));

    eprintln!("  starting swactor in background...");
    let bin_str = bin.to_string_lossy();
    match Command::new(&*bin_str)
        .stdin(std::process::Stdio::null())
        .stdout(std::process::Stdio::null())
        .stderr(std::process::Stdio::null())
        .spawn()
    {
        Ok(child) => eprintln!("  started (pid {})", child.id()),
        Err(e) => eprintln!("  failed to start: {e}"),
    }

    eprintln!("  crontab service installed (starts on @reboot)");
}

// ── Uninstall ───────────────────────────────────────────────────────────

pub fn uninstall() {
    eprintln!("Uninstalling swactor...");

    match detect_init() {
        InitSystem::Systemd => uninstall_systemd(),
        InitSystem::OpenRc if is_root() => uninstall_openrc(),
        InitSystem::SysVinit if is_root() => uninstall_sysvinit(),
        _ => uninstall_crontab(),
    }

    // Remove binary
    let dest = bin_path();
    if dest.exists() {
        eprintln!("  removing {}", dest.display());
        fs::remove_file(&dest).unwrap_or_else(|e| {
            eprintln!("  failed to remove binary: {e}");
        });
        // Remove bin/ dir if empty
        if let Some(parent) = dest.parent() {
            fs::remove_dir(parent).ok();
        }
    }

    eprintln!("  data left in ~/.swactor/ (not removed)");
    eprintln!("Done.");
}

fn uninstall_systemd() {
    eprintln!("  stopping and disabling user service...");
    run("systemctl", &["--user", "stop", "swactor"]);
    run("systemctl", &["--user", "disable", "swactor"]);

    let path = systemd_user_dir().join("swactor.service");
    if path.exists() {
        eprintln!("  removing {}", path.display());
        fs::remove_file(&path).unwrap_or_else(|e| {
            eprintln!("  failed to remove service file: {e}");
        });
    }

    run("systemctl", &["--user", "daemon-reload"]);
}

fn uninstall_openrc() {
    let path = "/etc/init.d/swactor";

    eprintln!("  stopping and disabling service...");
    run("rc-service", &["swactor", "stop"]);
    run("rc-update", &["del", "swactor", "default"]);

    if Path::new(path).exists() {
        eprintln!("  removing {path}");
        fs::remove_file(path).unwrap_or_else(|e| {
            eprintln!("  failed to remove init script: {e}");
        });
    }
}

fn uninstall_sysvinit() {
    let path = "/etc/init.d/swactor";

    eprintln!("  stopping service...");
    run(path, &["stop"]);

    if !run("update-rc.d", &["-f", "swactor", "remove"]) {
        run("chkconfig", &["--del", "swactor"]);
    }

    if Path::new(path).exists() {
        eprintln!("  removing {path}");
        fs::remove_file(path).unwrap_or_else(|e| {
            eprintln!("  failed to remove init script: {e}");
        });
    }
}

fn uninstall_crontab() {
    let dest = bin_path();
    let tag = crontab_tag(&dest);
    // Legacy format used an unconditional `while true` loop.
    let legacy_tag = format!(
        "@reboot /bin/sh -c 'while true; do {} ; sleep 5; done'",
        dest.display()
    );
    let existing = read_crontab();

    // Kill running process
    eprintln!("  stopping swactor...");
    run("pkill", &["-INT", "-f", &dest.to_string_lossy()]);

    let is_swactor_entry = |l: &&str| {
        let t = l.trim();
        t == tag || t == legacy_tag
    };

    if existing.lines().any(|l| is_swactor_entry(&l)) {
        eprintln!("  removing crontab @reboot entry");
        let new: String = existing
            .lines()
            .filter(|l| !is_swactor_entry(l))
            .collect::<Vec<_>>()
            .join("\n")
            + "\n";
        // If crontab is now empty (just whitespace), remove it entirely
        if new.trim().is_empty() {
            run("crontab", &["-r"]);
        } else {
            write_crontab(&new);
        }
    }
}
