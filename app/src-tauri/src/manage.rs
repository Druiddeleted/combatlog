//! Local log management: split one combat log into per-session files, and
//! archive completed logs into the `warcraftlogsarchive` folder inside the logs
//! folder (zip-per-file).
//!
//! Both operations stream (1 MiB buffers, zip64 enabled) so multi-GB combat
//! logs are handled without loading a whole file into memory. They run on a
//! blocking thread (see `main.rs`) and report progress via `manage:progress`.

use std::collections::HashSet;
use std::fs::{self, File};
use std::io::{BufRead, BufReader, BufWriter, Read, Write};
use std::path::{Path, PathBuf};

use anyhow::{anyhow, Context as _, Result};
use regex::Regex;
use serde::{Deserialize, Serialize};
use serde_json::json;
use tauri::{AppHandle, Emitter};

const BUF: usize = 1 << 20; // 1 MiB

fn emit_progress(app: &AppHandle, message: impl Into<String>, pct: u32) {
    let _ = app.emit(
        "manage:progress",
        json!({ "message": message.into(), "pct": pct }),
    );
}

// ---- Split -----------------------------------------------------------------

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct SplitArgs {
    pub source: String,
    pub output_dir: String,
}

#[derive(Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct SplitOutput {
    pub name: String,
    pub lines: u64,
    pub bytes: u64,
}

/// Split `source` at every `COMBAT_LOG_VERSION` marker (WoW writes one at the
/// start of each logging session) into per-session files under `output_dir`.
pub fn split_log(app: &AppHandle, args: SplitArgs) -> Result<Vec<SplitOutput>> {
    let src = PathBuf::from(&args.source);
    let out_dir = PathBuf::from(&args.output_dir);
    fs::create_dir_all(&out_dir).with_context(|| format!("creating {}", out_dir.display()))?;

    let total_bytes = fs::metadata(&src).map(|m| m.len()).unwrap_or(0).max(1);
    let file = File::open(&src).with_context(|| format!("opening {}", src.display()))?;
    let mut reader = BufReader::with_capacity(BUF, file);

    let mut outputs: Vec<SplitOutput> = Vec::new();
    let mut writer: Option<BufWriter<File>> = None;
    let mut used: HashSet<String> = HashSet::new();
    let mut cur = SplitOutput { name: String::new(), lines: 0, bytes: 0 };
    let mut read_bytes: u64 = 0;
    let mut idx: u32 = 0;
    let mut last_pct: u32 = 0;

    let mut line = String::new();
    loop {
        line.clear();
        let n = reader.read_line(&mut line)?;
        if n == 0 {
            break;
        }
        read_bytes += n as u64;

        if line.contains("COMBAT_LOG_VERSION") {
            if let Some(mut w) = writer.take() {
                w.flush()?;
                outputs.push(cur.clone());
            }
            idx += 1;
            cur = SplitOutput {
                name: session_filename(&line, idx, &mut used),
                lines: 0,
                bytes: 0,
            };
            let path = out_dir.join(&cur.name);
            writer = Some(BufWriter::with_capacity(
                BUF,
                File::create(&path).with_context(|| format!("creating {}", path.display()))?,
            ));
        }

        if let Some(w) = writer.as_mut() {
            w.write_all(line.as_bytes())?;
            cur.lines += 1;
            cur.bytes += n as u64;
        }
        // Lines before the first COMBAT_LOG_VERSION (a partial leading session)
        // are dropped — they can't be attributed to a session.

        let pct = ((read_bytes.saturating_mul(100)) / total_bytes) as u32;
        if pct >= last_pct + 2 {
            last_pct = pct;
            emit_progress(app, format!("Splitting… {pct}%"), pct);
        }
    }
    if let Some(mut w) = writer.take() {
        w.flush()?;
        outputs.push(cur);
    }

    if outputs.is_empty() {
        return Err(anyhow!(
            "No COMBAT_LOG_VERSION markers found — is this a WoW combat log?"
        ));
    }
    Ok(outputs)
}

/// Name a session file from the timestamp on its `COMBAT_LOG_VERSION` line,
/// matching WoW's own `WoWCombatLog-MMDDYY_HHMMSS.txt` shape. Falls back to a
/// sequential name when the line has no parseable (year-bearing) timestamp.
fn session_filename(version_line: &str, idx: u32, used: &mut HashSet<String>) -> String {
    let base = parse_session_stamp(version_line)
        .map(|s| format!("WoWCombatLog-{s}"))
        .unwrap_or_else(|| format!("WoWCombatLog-session{idx:02}"));
    let mut name = format!("{base}.txt");
    let mut dedup = 1;
    while used.contains(&name) {
        dedup += 1;
        name = format!("{base}_{dedup}.txt");
    }
    used.insert(name.clone());
    name
}

/// Parse `MMDDYY_HHMMSS` from a log line prefix like
/// `4/17/2024 20:13:45.123-4  COMBAT_LOG_VERSION,...`. Requires a year (recent
/// WoW logs include one); returns None for the older year-less format.
fn parse_session_stamp(line: &str) -> Option<String> {
    let re =
        Regex::new(r"^(\d{1,2})/(\d{1,2})/(\d{2,4})\s+(\d{1,2}):(\d{2}):(\d{2})").ok()?;
    let c = re.captures(line)?;
    let mm: u32 = c.get(1)?.as_str().parse().ok()?;
    let dd: u32 = c.get(2)?.as_str().parse().ok()?;
    let yy: i32 = c.get(3)?.as_str().parse().ok()?;
    let yy = ((yy % 100) + 100) % 100;
    let hh: u32 = c.get(4)?.as_str().parse().ok()?;
    let mi: u32 = c.get(5)?.as_str().parse().ok()?;
    let ss: u32 = c.get(6)?.as_str().parse().ok()?;
    Some(format!("{mm:02}{dd:02}{yy:02}_{hh:02}{mi:02}{ss:02}"))
}

// ---- Archive ---------------------------------------------------------------

/// Folder created next to the logs when no archive folder exists yet.
pub const ARCHIVE_DIR_NAME: &str = "warcraftlogsarchive";

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
pub struct ArchiveArgs {
    pub files: Vec<String>,
    /// Explicit destination. When omitted (the normal case) each log is
    /// archived into the archive folder inside its own logs folder — see
    /// [`archive_dir_for`].
    #[serde(default)]
    pub dest_dir: Option<String>,
    pub delete_originals: bool,
}

#[derive(Serialize, Clone)]
#[serde(rename_all = "camelCase")]
pub struct ArchiveResult {
    pub count: u32,
    /// Where the logs ended up, for the UI to report. Only set when every log
    /// went to the same folder (always true for a single-folder selection).
    pub dest_dir: Option<String>,
}

/// The archive folder for a log: an existing `*archive*` subfolder of the log's
/// own folder if there is one ("whatever it's called right now"), otherwise
/// `<logs folder>/warcraftlogsarchive`. A log already sitting inside an archive
/// folder archives in place.
fn archive_dir_for(src: &Path) -> PathBuf {
    let parent = src.parent().unwrap_or_else(|| Path::new("."));
    if is_archive_name(parent) {
        return parent.to_path_buf();
    }
    if let Ok(entries) = fs::read_dir(parent) {
        let mut found: Vec<PathBuf> = entries
            .flatten()
            .map(|e| e.path())
            .filter(|p| p.is_dir() && is_archive_name(p))
            .collect();
        found.sort();
        // Prefer the canonical name when several archive-ish folders exist.
        if let Some(exact) = found.iter().find(|p| {
            p.file_name()
                .and_then(|n| n.to_str())
                .map(|n| n.eq_ignore_ascii_case(ARCHIVE_DIR_NAME))
                .unwrap_or(false)
        }) {
            return exact.clone();
        }
        if let Some(first) = found.into_iter().next() {
            return first;
        }
    }
    parent.join(ARCHIVE_DIR_NAME)
}

fn is_archive_name(dir: &Path) -> bool {
    dir.file_name()
        .and_then(|n| n.to_str())
        .map(|n| n.to_ascii_lowercase().contains("archive"))
        .unwrap_or(false)
}

/// Zip each file in `files` (one entry per zip, zip64 for >4 GB) into the
/// archive folder, optionally deleting the source afterward. Returns the count
/// successfully archived and the folder used.
pub fn archive_logs(app: &AppHandle, args: ArchiveArgs) -> Result<ArchiveResult> {
    let explicit = args.dest_dir.as_ref().map(PathBuf::from);

    let total = args.files.len().max(1) as u32;
    let mut done: u32 = 0;
    let mut used: HashSet<PathBuf> = HashSet::new();
    let mut dests: HashSet<PathBuf> = HashSet::new();

    for (i, fpath) in args.files.iter().enumerate() {
        let src = PathBuf::from(fpath);
        let dest = explicit.clone().unwrap_or_else(|| archive_dir_for(&src));
        fs::create_dir_all(&dest).with_context(|| format!("creating {}", dest.display()))?;
        let display_name = src
            .file_name()
            .and_then(|n| n.to_str())
            .unwrap_or("log")
            .to_string();
        emit_progress(
            app,
            format!("Archiving {display_name} → {}…", dest.display()),
            (i as u32) * 100 / total,
        );
        let stem = src.file_stem().and_then(|s| s.to_str()).unwrap_or("log");
        let zip_path = unique_zip_path(&dest, stem, &mut used);
        zip_file(&src, &zip_path)
            .with_context(|| format!("archiving {}", src.display()))?;
        if args.delete_originals {
            fs::remove_file(&src)
                .with_context(|| format!("deleting {}", src.display()))?;
        }
        dests.insert(dest);
        done += 1;
    }
    let dest_dir = if dests.len() == 1 {
        dests
            .into_iter()
            .next()
            .map(|d| d.display().to_string())
    } else {
        None
    };
    Ok(ArchiveResult { count: done, dest_dir })
}

fn unique_zip_path(dir: &Path, stem: &str, used: &mut HashSet<PathBuf>) -> PathBuf {
    let mut path = dir.join(format!("{stem}.zip"));
    let mut dedup = 1;
    while used.contains(&path) || path.exists() {
        dedup += 1;
        path = dir.join(format!("{stem}_{dedup}.zip"));
    }
    used.insert(path.clone());
    path
}

fn zip_file(src: &Path, dest_zip: &Path) -> Result<()> {
    use zip::write::SimpleFileOptions;
    use zip::CompressionMethod;

    let out = File::create(dest_zip)
        .with_context(|| format!("creating {}", dest_zip.display()))?;
    let mut zw = zip::ZipWriter::new(BufWriter::with_capacity(BUF, out));
    let opts = SimpleFileOptions::default()
        .compression_method(CompressionMethod::Deflated)
        .compression_level(Some(6))
        .large_file(true);
    let entry = src.file_name().and_then(|n| n.to_str()).unwrap_or("log.txt");
    zw.start_file(entry, opts)?;

    let mut rf = BufReader::with_capacity(BUF, File::open(src)?);
    let mut buf = vec![0u8; BUF];
    loop {
        let n = rf.read(&mut buf)?;
        if n == 0 {
            break;
        }
        zw.write_all(&buf[..n])?;
    }
    // finish() returns the inner BufWriter; flush it so the last buffered bytes
    // reach disk instead of relying on drop (which ignores flush errors).
    zw.finish()?.flush()?;
    Ok(())
}
