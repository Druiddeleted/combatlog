//! Live log engine. Tails the active WoWCombatLog(-*)?.txt in a directory and
//! uploads segments to a live report, mirroring the Archon App's liveLogOperation.

use std::io::SeekFrom;
use std::path::{Path, PathBuf};
use std::time::{Duration, Instant, SystemTime, UNIX_EPOCH};

use anyhow::{anyhow, Context as _, Result};
use regex::Regex;
use serde::Deserialize;
use serde_json::{json, Value};
use tauri::{AppHandle, Emitter, Manager as _};
use tokio::io::{AsyncReadExt, AsyncSeekExt};
use tokio::sync::watch;

use crate::{parser, wcl};

// The standard combat log OR the dated per-session variants
// (WoWCombatLog-MMDDYY_HHMMSS.txt) WoW writes with per-session naming.
// find_newest_log() then picks whichever is being actively written (newest
// mtime), so it works regardless of the client's per-session-log setting.
const LOG_FILE_PATTERN: &str = r"^WoWCombatLog.*\.txt$";
const POLL_INTERVAL: Duration = Duration::from_secs(1);
const IDLE_THRESHOLD: Duration = Duration::from_secs(120);
const MAX_FILE_AGE: Duration = Duration::from_secs(6 * 3600);
const MAX_CHUNK_LINES: usize = 5_000;
const MAX_CHUNK_BYTES: u64 = 8 * 1024 * 1024;
/// Per poll, keep reading until the file is exhausted or this many lines are
/// gathered, and upload once. WoW dumps a final boss kill and the following
/// CHALLENGE_MODE_END in one buffered burst; uploading per 5k-line chunk let
/// the boss fight ship in one segment and the key-end land in a later one,
/// which WCL's live pipeline never reconciles (Pit of Saron, 2026-07-26).
const DRAIN_MAX_LINES: usize = 100_000;
const LIVE_RETRY_MAX: u32 = 120;
const LIVE_RETRY_DELAY: Duration = Duration::from_secs(30);
/// How long to wait on stop for WoW to flush its buffered log writes before the
/// last read. Without it the final pull's ENCOUNTER_END is often still unwritten.
const STOP_GRACE: Duration = Duration::from_secs(3);
/// A forced commit shorter than this is a sliver of trailing events, not a
/// fight; uploading it adds a junk sub-second "trash" entry to the report.
const MIN_FORCED_FIGHT_MS: i64 = 1000;

#[derive(Deserialize, Debug, Clone)]
#[serde(rename_all = "camelCase")]
pub struct LiveLogArgs {
    pub directory: String,
    pub email: String,
    pub password: String,
    pub region: i32,
    pub visibility: i32,
    pub guild_id: Option<i64>,
    pub include_entire_file_in_report: bool,
    pub enable_real_time_uploading: bool,
}

fn cancelled(rx: &watch::Receiver<bool>) -> bool {
    *rx.borrow() || rx.has_changed().is_err()
}

/// 1s sleep that returns early when the stop signal fires.
async fn idle_sleep(rx: &mut watch::Receiver<bool>) {
    tokio::select! {
        _ = tokio::time::sleep(POLL_INTERVAL) => {}
        _ = rx.changed() => {}
    }
}

fn now_ms() -> i64 {
    SystemTime::now()
        .duration_since(UNIX_EPOCH)
        .map(|d| d.as_millis() as i64)
        .unwrap_or(0)
}

/// Append-only JSONL record of everything the live session uploads, so a
/// misclassified report can be diffed against exactly what the client sent
/// (fight spans, params, content hashes, retries). One file per report code
/// under <app data>/live-journals/. Never breaks an upload: write errors are
/// swallowed after a single stderr note.
struct Journal {
    path: Option<PathBuf>,
}

impl Journal {
    fn new(app: &AppHandle, code: &str) -> Journal {
        let path = app
            .path()
            .app_data_dir()
            .ok()
            .map(|d| d.join("live-journals"))
            .and_then(|d| {
                if let Err(e) = std::fs::create_dir_all(&d) {
                    eprintln!("[live] journal dir failed: {e}");
                    return None;
                }
                Some(d.join(format!("live-{code}.jsonl")))
            });
        if let Some(p) = &path {
            eprintln!("[live] journal: {}", p.display());
        }
        Journal { path }
    }

    fn write(&self, event: &str, mut fields: Value) {
        let Some(path) = &self.path else { return };
        if let Some(o) = fields.as_object_mut() {
            o.insert("event".into(), json!(event));
            o.insert("ts".into(), json!(now_ms()));
        }
        let line = format!("{fields}\n");
        let res = std::fs::OpenOptions::new()
            .create(true)
            .append(true)
            .open(path)
            .and_then(|mut f| std::io::Write::write_all(&mut f, line.as_bytes()));
        if let Err(e) = res {
            eprintln!("[live] journal write failed: {e}");
        }
    }
}

/// First and last event timestamps (report-relative ms) of a fight's
/// eventsString, for journaling which span each upload carried.
fn fight_span(events: &str) -> (i64, i64) {
    let first = events
        .split('\n')
        .find(|l| !l.is_empty())
        .and_then(|l| l.split('|').next())
        .and_then(|t| t.parse().ok())
        .unwrap_or(-1);
    let last = events
        .rsplit('\n')
        .find(|l| !l.is_empty())
        .and_then(|l| l.split('|').next())
        .and_then(|t| t.parse().ok())
        .unwrap_or(-1);
    (first, last)
}

/// Cheap content fingerprint for journal records (not cryptographic).
fn content_hash(s: &str) -> String {
    use std::hash::{Hash, Hasher};
    let mut h = std::collections::hash_map::DefaultHasher::new();
    s.hash(&mut h);
    format!("{:016x}", h.finish())
}

struct LiveProgress {
    app: AppHandle,
    file: Option<String>,
    segments: u64,
    in_progress: bool,
}

impl LiveProgress {
    fn emit(&self, state: &str, message: impl Into<String>) {
        let _ = self.app.emit(
            "live:progress",
            json!({
                "state": state,
                "message": message.into(),
                "file": self.file,
                "segments": self.segments,
                "inProgress": self.in_progress,
            }),
        );
    }
}

pub async fn run_live_log(
    app: AppHandle,
    args: LiveLogArgs,
    mut cancel: watch::Receiver<bool>,
) -> Result<()> {
    let dir = PathBuf::from(&args.directory);
    if !dir.is_dir() {
        return Err(anyhow!("not a directory: {}", dir.display()));
    }
    let pattern = Regex::new(LOG_FILE_PATTERN)?;

    let mut progress = LiveProgress {
        app: app.clone(),
        file: None,
        segments: 0,
        in_progress: false,
    };

    progress.emit("waiting", "Initializing session...");
    // Live logging is Warcraft-only (it tails WoWCombatLog.txt).
    let session = wcl::WclSession::new("warcraft").await?;

    progress.emit("waiting", "Logging in...");
    let login = session.login(&args.email, &args.password).await?;
    let user_name = login
        .user
        .as_ref()
        .and_then(|u| u.user_name.as_deref())
        .unwrap_or("?")
        .to_string();
    progress.emit("waiting", format!("Logged in as {user_name}"));

    progress.emit("waiting", "Fetching latest parser...");
    let bundle = session.fetch_parser_code().await?;

    let harness = parser::harness_path(&app)?;
    progress.emit("waiting", "Starting parser...");
    let parser =
        parser::Parser::spawn(&app, &harness, &bundle.gamedata_code, &bundle.parser_code).await?;

    // from here on the sidecar must be closed and any created report terminated,
    // even when setup fails partway
    let mut report: Option<(String, String)> = None; // (code, url)
    let result = run_live_session(
        &app, args, &session, &parser, &bundle, &pattern, &dir, &mut cancel, &mut progress, &mut report,
    )
    .await;

    parser.close().await;
    if let Some((code, _)) = &report {
        if let Err(e) = session.terminate_report(code).await {
            eprintln!("[live] terminate_report failed: {e:#}");
        }
    }

    result?;
    if let Some((code, url)) = &report {
        // Full path of the log that was tailed, so the UI can offer to archive it.
        let file_path = progress
            .file
            .as_ref()
            .map(|f| dir.join(f).to_string_lossy().to_string());
        let _ = app.emit(
            "live:done",
            json!({"url": url, "code": code, "segments": progress.segments, "file": file_path}),
        );
    }
    Ok(())
}

#[allow(clippy::too_many_arguments)]
async fn run_live_session(
    app: &AppHandle,
    args: LiveLogArgs,
    session: &wcl::WclSession,
    parser: &parser::Parser,
    bundle: &wcl::ParserBundle,
    pattern: &Regex,
    dir: &Path,
    cancel: &mut watch::Receiver<bool>,
    progress: &mut LiveProgress,
    report: &mut Option<(String, String)>,
) -> Result<()> {
    parser.clear_state().await?;

    let start_ms = now_ms();
    let code = session
        .create_report(
            "live.log",
            start_ms,
            start_ms,
            args.region,
            args.visibility,
            args.guild_id,
            bundle.parser_version,
        )
        .await?;
    let url = format!("https://www.warcraftlogs.com/reports/{code}");
    let _ = app.emit("live:started", json!({"code": code, "url": url}));
    progress.emit("waiting", format!("Live report created: {code}"));
    *report = Some((code.clone(), url));

    if !args.include_entire_file_in_report {
        // the existing file is still parsed for actor/ability state,
        // but fights before this moment are excluded from the report
        parser.set_live_logging_start_time(start_ms).await?;
    }

    let journal = Journal::new(app, &code);
    journal.write(
        "session_start",
        json!({
            "code": code,
            "directory": args.directory,
            "region": args.region,
            "includeExisting": args.include_entire_file_in_report,
            "realTime": args.enable_real_time_uploading,
            "startMs": start_ms,
        }),
    );

    tail_loop(args, session, parser, &code, pattern, dir, cancel, progress, &journal).await
}

#[allow(clippy::too_many_arguments)]
async fn tail_loop(
    args: LiveLogArgs,
    session: &wcl::WclSession,
    parser: &parser::Parser,
    code: &str,
    pattern: &Regex,
    dir: &Path,
    cancel: &mut watch::Receiver<bool>,
    progress: &mut LiveProgress,
    journal: &Journal,
) -> Result<()> {
    let mut uploader = Uploader {
        session,
        parser,
        code,
        args,
        segment_id: 1,
        last_master_ids: None,
        journal,
    };

    let mut current_path: Option<PathBuf> = None;
    let mut offset: u64 = 0;
    let mut last_data = Instant::now();
    let mut dirty = false; // parsed lines since the last fight flush
    // An ENCOUNTER_START has been tailed with no ENCOUNTER_END yet. WoW buffers
    // log writes, so the file regularly goes >120s without growth right after a
    // pull starts; an idle flush at that moment force-commits a start-only
    // fight fragment, which WCL's live pipeline instantly demotes to trash
    // (boss 0, originalBoss kept) and never re-evaluates even when the rest of
    // the pull arrives. Proven live 2026-07-26 (report d1YHnMcCPK28QDFg fight
    // 10). Never idle-flush while an encounter is open.
    let mut encounter_open = false;
    // A keystone run is open (CHALLENGE_MODE_START tailed, no END yet). The
    // parser holds the whole key as ONE fight; an idle flush mid-run chops it
    // across segments, and WCL's live pipeline locks the key on the first
    // chop and never re-evaluates — the run shows unfinished even though the
    // CHALLENGE_MODE_END uploads promptly (Pit of Saron 07-26, Magisters' +
    // Windrunner 07-28; chopped-but-fast replays stitch fine, so it is the
    // same append-without-reclassify server behavior as raid demotions).
    // Hold like an open encounter; the run commits whole at its END.
    let mut key_open = false;
    // A CHALLENGE_MODE_END was tailed but no segment has been uploaded since:
    // the parser declines to commit tiny post-key buffers, so keep forcing the
    // push on every poll until something actually uploads.
    let mut pending_key = false;
    let mut waiting_logged = false;

    while !cancelled(cancel) {
        let Some(path) = find_newest_log(dir, pattern).await else {
            if current_path.is_none() && !waiting_logged {
                progress.emit("waiting", "Waiting for a combat log to appear...");
                waiting_logged = true;
            }
            idle_sleep(cancel).await;
            continue;
        };

        if Some(&path) != current_path.as_ref() {
            // rotation: drain what's left of the old file first
            if let Some(old) = current_path.take() {
                while !cancelled(cancel) {
                    let Ok(chunk) = read_chunk(&old, offset).await else {
                        break; // old file gone
                    };
                    if chunk.lines.is_empty() {
                        break;
                    }
                    offset = chunk.new_offset;
                    if let Err(e) = uploader.upload_part(&chunk.lines, false, false, cancel, progress).await {
                        if cancelled(cancel) {
                            break; // stop requested mid-retry: fall through to final flush
                        }
                        return Err(e);
                    }
                }
            }
            offset = 0;
            let name = path
                .file_name()
                .and_then(|f| f.to_str())
                .unwrap_or("")
                .to_string();
            if let Some(date) = wcl::parse_start_date(&name) {
                parser.set_start_date(&date).await?;
            }
            progress.file = Some(name.clone());
            progress.emit("tailing", format!("Tailing {name}"));
            journal.write("tail_file", json!({"file": name}));
            current_path = Some(path.clone());
            waiting_logged = false;
        }

        let chunk = match read_chunk(&path, offset).await {
            Ok(c) => c,
            Err(e) => {
                // transient (AV lock, file swapped out mid-read): no data this tick
                eprintln!("[live] read error on {}: {e:#}", path.display());
                journal.write("read_error", json!({"offset": offset, "error": format!("{e:#}")}));
                idle_sleep(cancel).await;
                continue;
            }
        };
        if chunk.file_size < offset {
            journal.write("truncated", json!({"offset": offset, "fileSize": chunk.file_size}));
            offset = 0;
            progress.emit("tailing", "Log truncated — reading from the beginning");
            continue;
        }

        let no_new_lines = chunk.lines.is_empty();
        let flush_result = if no_new_lines {
            if pending_key && !encounter_open && !key_open {
                // keep retrying the key-end push until a segment uploads
                match uploader.upload_part(&[], true, false, cancel, progress).await {
                    Ok(uploaded) => {
                        if uploaded {
                            pending_key = false;
                        }
                        Ok(())
                    }
                    Err(e) => Err(e),
                }
            } else if dirty && last_data.elapsed() > IDLE_THRESHOLD {
                if encounter_open || key_open {
                    // mid-pull / mid-key write lull: hold until the END arrives
                    journal.write(
                        "idle_flush_skipped",
                        json!({"offset": offset, "encounterOpen": encounter_open, "keyOpen": key_open}),
                    );
                    last_data = Instant::now(); // re-arm instead of spinning
                    Ok(())
                } else {
                    progress.emit("idle", "Log idle — flushing current fight");
                    journal.write("idle_flush", json!({"offset": offset}));
                    dirty = false;
                    uploader.upload_part(&[], true, true, cancel, progress).await.map(|_| ())
                }
            } else {
                Ok(())
            }
        } else {
            // Drain everything already on disk before uploading, so events WoW
            // wrote in one burst (boss END + CHALLENGE_MODE_END) can't be split
            // across segments by the per-read line cap.
            let mut lines = chunk.lines;
            offset = chunk.new_offset;
            while lines.len() < DRAIN_MAX_LINES {
                match read_chunk(&path, offset).await {
                    Ok(more) if !more.lines.is_empty() => {
                        offset = more.new_offset;
                        lines.extend(more.lines);
                    }
                    _ => break,
                }
            }
            last_data = Instant::now();
            dirty = true;
            // A boss ENCOUNTER_END auto-commits as a fight, so normal push=false
            // tailing catches it. A Mythic+ CHALLENGE_MODE_END does NOT
            // auto-commit, so without a push the run never finalizes and WCL
            // shows the key uncompleted. Force-commit on the chunk carrying the
            // key's end — and keep forcing (pending_key) until a segment truly
            // uploads, because the parser declines to commit tiny buffers.
            let mut key_ended = false;
            for l in &lines {
                if l.contains("ENCOUNTER_START") {
                    encounter_open = true;
                } else if l.contains("ENCOUNTER_END") {
                    encounter_open = false;
                } else if l.contains("CHALLENGE_MODE_START") {
                    key_open = true;
                } else if l.contains("CHALLENGE_MODE_END") {
                    // both real completions and abandon/reset markers close it
                    key_open = false;
                    key_ended = true;
                }
            }
            if key_ended {
                pending_key = true;
            }
            // never force while a newer encounter is open — a forced commit
            // would split it (the demotion bug all over again)
            let force = (key_ended || pending_key) && !encounter_open && !key_open;
            match uploader
                .upload_part(&lines, force, false, cancel, progress)
                .await
            {
                Ok(uploaded) => {
                    if uploaded {
                        pending_key = false;
                    }
                    Ok(())
                }
                Err(e) => Err(e),
            }
        };
        if let Err(e) = flush_result {
            if cancelled(cancel) {
                break; // stop requested mid-retry: fall through to final flush
            }
            return Err(e);
        }
        if no_new_lines {
            idle_sleep(cancel).await;
        }
    }

    // WoW buffers its combat-log writes, so when Stop is pressed shortly after a
    // pull the ENCOUNTER_END is usually still unwritten. Wait for it and drain
    // the file one last time BEFORE force-committing: a fight force-closed
    // without its end uploads as a finalized segment with no encounter end, and
    // WarcraftLogs demotes it to trash (boss 0, originalBoss set) instead of
    // showing the kill/wipe.
    progress.emit("uploading", "Stopping — waiting for final log writes...");
    journal.write("stop", json!({"offset": offset}));
    tokio::time::sleep(STOP_GRACE).await;
    if let Some(path) = current_path.as_ref() {
        loop {
            let Ok(chunk) = read_chunk(path, offset).await else {
                break; // file gone/locked: nothing more to drain
            };
            if chunk.lines.is_empty() {
                break;
            }
            offset = chunk.new_offset;
            if let Err(e) = uploader.upload_part(&chunk.lines, false, false, cancel, progress).await {
                eprintln!("[live] final drain failed: {e:#}");
                break;
            }
        }
    }

    // final flush so an in-progress fight makes it into the report
    progress.emit("uploading", "Stopping — flushing final data...");
    journal.write("final_flush", json!({"offset": offset}));
    if let Err(e) = uploader.upload_part(&[], true, true, cancel, progress).await {
        eprintln!("[live] final flush failed: {e:#}");
        journal.write("final_flush_failed", json!({"error": format!("{e:#}")}));
    }
    Ok(())
}

/// newest file in `dir` matching `pattern`, modified within MAX_FILE_AGE.
async fn find_newest_log(dir: &Path, pattern: &Regex) -> Option<PathBuf> {
    let mut rd = tokio::fs::read_dir(dir).await.ok()?;
    let mut newest: Option<(SystemTime, PathBuf)> = None;
    while let Ok(Some(entry)) = rd.next_entry().await {
        let name = entry.file_name();
        let Some(name) = name.to_str() else { continue };
        if !pattern.is_match(name) {
            continue;
        }
        let Ok(meta) = entry.metadata().await else { continue };
        if !meta.is_file() {
            continue;
        }
        let Ok(mtime) = meta.modified() else { continue };
        if mtime.elapsed().map(|age| age > MAX_FILE_AGE).unwrap_or(false) {
            continue;
        }
        if newest.as_ref().map(|(t, _)| mtime > *t).unwrap_or(true) {
            newest = Some((mtime, entry.path()));
        }
    }
    newest.map(|(_, p)| p)
}

struct Chunk {
    lines: Vec<String>,
    new_offset: u64,
    file_size: u64, // size at read time; < offset means the file was truncated
}

/// complete lines starting at `offset`; a partial trailing line is left for the
/// next poll (new_offset always lands just past a '\n').
async fn read_chunk(path: &Path, offset: u64) -> Result<Chunk> {
    let mut file = tokio::fs::File::open(path)
        .await
        .with_context(|| format!("opening {}", path.display()))?;
    let size = file.metadata().await?.len();
    if size <= offset {
        return Ok(Chunk { lines: Vec::new(), new_offset: offset, file_size: size });
    }
    file.seek(SeekFrom::Start(offset)).await?;
    let want = (size - offset).min(MAX_CHUNK_BYTES) as usize;
    let mut buf = vec![0u8; want];
    let mut filled = 0;
    while filled < want {
        let n = file.read(&mut buf[filled..]).await?;
        if n == 0 {
            break; // file shrank mid-read; use what we have
        }
        filled += n;
    }
    buf.truncate(filled);

    let mut lines = Vec::new();
    let mut consumed = 0usize;
    let mut start = 0usize;
    while lines.len() < MAX_CHUNK_LINES {
        let Some(nl) = buf[start..].iter().position(|&b| b == b'\n') else {
            break;
        };
        let mut line = &buf[start..start + nl];
        if line.last() == Some(&b'\r') {
            line = &line[..line.len() - 1];
        }
        lines.push(String::from_utf8_lossy(line).into_owned());
        consumed = start + nl + 1;
        start = consumed;
    }
    Ok(Chunk {
        lines,
        new_offset: offset + consumed as u64,
        file_size: size,
    })
}

/// A committed batch too short or too empty to be a real fight. Span is
/// measured from the fights' own event timestamps — fd's startTime/endTime are
/// session-global and useless for this (v0.5.8 uploaded 1-event specks because
/// of exactly that).
fn is_sliver(fd: &Value) -> bool {
    let Some(fights) = fd.get("fights").and_then(|f| f.as_array()) else {
        return true;
    };
    let events: i64 = fights
        .iter()
        .filter_map(|f| f.get("eventCount").and_then(|n| n.as_i64()))
        .sum();
    if events == 0 {
        return true;
    }
    let mut first = i64::MAX;
    let mut last = i64::MIN;
    for f in fights {
        let ev = f.get("eventsString").and_then(|s| s.as_str()).unwrap_or("");
        let (a, b) = fight_span(ev);
        if a >= 0 {
            first = first.min(a);
            last = last.max(b);
        }
    }
    first > last || last - first < MIN_FORCED_FIGHT_MS
}

fn fights_empty(v: &Value) -> bool {
    v.get("fights")
        .and_then(|f| f.as_array())
        .map(|a| a.is_empty())
        .unwrap_or(true)
}

struct Uploader<'a> {
    session: &'a wcl::WclSession,
    parser: &'a parser::Parser,
    code: &'a str,
    args: LiveLogArgs,
    segment_id: i64,
    last_master_ids: Option<(i64, i64, i64, i64)>,
    journal: &'a Journal,
}

impl Uploader<'_> {
    /// mirror of Archon's uploadFilePart. Returns whether a segment was
    /// actually uploaded (the parser declines to commit tiny buffers, so a
    /// push is not a guarantee). `drop_slivers` suppresses sub-second forced
    /// commits — must be false for key-end pushes, whose CHALLENGE_MODE_END
    /// fragment is itself sub-second (Magisters' Terrace's was 0.6s).
    async fn upload_part(
        &mut self,
        lines: &[String],
        push_fight: bool,
        drop_slivers: bool,
        cancel: &mut watch::Receiver<bool>,
        progress: &mut LiveProgress,
    ) -> Result<bool> {
        if !lines.is_empty() {
            self.parser.parse_lines(lines, self.args.region).await?;
        }
        let mut fd = self.parser.collect_fights(push_fight).await?;
        let mut in_progress_count: i64 = 0;
        let mut uploading_in_progress = false;

        // A forced commit (idle flush, stop, key end) closes whatever fragment is
        // open. Right after a fight ends that is a handful of trailing events —
        // uploading it lands a 1 ms "trash" fight in the report. Drop it, but
        // still clear so it can't be re-sent later.
        if push_fight && drop_slivers && !fights_empty(&fd) && is_sliver(&fd) {
            self.journal.write(
                "sliver_dropped",
                json!({
                    "startTime": fd.get("startTime"),
                    "endTime": fd.get("endTime"),
                    "eventCounts": fd.get("fights").and_then(|f| f.as_array()).map(|a| a
                        .iter()
                        .map(|f| f.get("eventCount").cloned().unwrap_or(json!(0)))
                        .collect::<Vec<_>>()),
                }),
            );
            self.parser.clear_fights().await?;
            return Ok(false);
        }

        if fights_empty(&fd) {
            let ip = self.parser.collect_in_progress_fight().await?;
            let has_in_progress = !fights_empty(&ip);
            if progress.in_progress != has_in_progress {
                progress.in_progress = has_in_progress;
                if has_in_progress {
                    progress.emit("tailing", "Fight in progress");
                }
            }
            if !self.args.enable_real_time_uploading || !has_in_progress {
                return Ok(false);
            }
            in_progress_count = ip
                .get("fights")
                .and_then(|f| f.as_array())
                .and_then(|a| a.first())
                .and_then(|f| f.get("eventCount"))
                .and_then(|n| n.as_i64())
                .unwrap_or(0);
            fd = ip;
            uploading_in_progress = true;
        } else {
            progress.in_progress = false;
        }

        // Mark ONLY genuine in-progress uploads as real-time/provisional. A
        // completed fight is finalized (isRealTime=false, inProgressEventCount=0)
        // so WarcraftLogs replaces the provisional segment with the real
        // encounter instead of leaving the boss stuck as "trash" / the key as a
        // partial run. (v0.2.0 used a session-wide isRealTime for both, which is
        // what left live-streamed fights misclassified.)
        let is_real_time = uploading_in_progress;
        let (session, code, segment_id) = (self.session, self.code, self.segment_id);
        let (email, password) = (self.args.email.clone(), self.args.password.clone());

        let mi = self.parser.collect_master_info().await?;
        let master_ids = wcl::master_ids(&mi);
        if Some(master_ids) != self.last_master_ids {
            let log_version = fd.get("logVersion").and_then(|v| v.as_i64()).unwrap_or(0);
            let game_version = fd.get("gameVersion").and_then(|v| v.as_i64()).unwrap_or(0);
            let master = wcl::build_master_string(&mi, log_version, game_version);
            let zipped = wcl::make_zip(&master)?;
            live_retry(session, &email, &password, cancel, progress, self.journal, || {
                let z = zipped.clone();
                async move {
                    session
                        .set_master_table(code, segment_id, is_real_time, z)
                        .await
                        .map(|_| 0i64)
                }
            })
            .await?;
            self.journal.write(
                "master_upload",
                json!({
                    "segmentId": segment_id,
                    "isRealTime": is_real_time,
                    "masterIds": [master_ids.0, master_ids.1, master_ids.2, master_ids.3],
                }),
            );
            self.last_master_ids = Some(master_ids);
        }

        let start_time = fd.get("startTime").and_then(|v| v.as_i64()).unwrap_or(0);
        let end_time = fd.get("endTime").and_then(|v| v.as_i64()).unwrap_or(0);
        let mythic = fd.get("mythic").and_then(|v| v.as_i64()).unwrap_or(0) as i32;
        // journal: per-fight spans + counts, so a demoted report can be diffed
        // against exactly what was sent
        let fight_meta: Vec<Value> = fd
            .get("fights")
            .and_then(|f| f.as_array())
            .map(|a| {
                a.iter()
                    .map(|f| {
                        let ev = f.get("eventsString").and_then(|s| s.as_str()).unwrap_or("");
                        let (first, last) = fight_span(ev);
                        json!({
                            "events": f.get("eventCount"),
                            "firstTs": first,
                            "lastTs": last,
                            "hash": content_hash(ev),
                        })
                    })
                    .collect()
            })
            .unwrap_or_default();
        let fights_str = wcl::build_fights_string(&fd);
        let zipped = wcl::make_zip(&fights_str)?;
        let next = live_retry(session, &email, &password, cancel, progress, self.journal, || {
            let z = zipped.clone();
            async move {
                session
                    .add_segment(
                        code,
                        segment_id,
                        start_time,
                        end_time,
                        mythic,
                        true,
                        is_real_time,
                        in_progress_count,
                        z,
                    )
                    .await
            }
        })
        .await?;
        self.journal.write(
            "segment_upload",
            json!({
                "segmentId": segment_id,
                "nextSegmentId": next,
                "startTime": start_time,
                "endTime": end_time,
                "mythic": mythic,
                "isRealTime": is_real_time,
                "inProgressEventCount": in_progress_count,
                "pushFight": push_fight,
                "fights": fight_meta,
                "blobHash": content_hash(&fights_str),
                "blobBytes": fights_str.len(),
            }),
        );
        if next > 0 {
            // in-progress segments come back with 0 and get overwritten in place
            self.segment_id = next;
            progress.segments += 1;
            progress.emit("uploading", format!("Uploaded segment {}", progress.segments));
        }
        self.parser.clear_fights().await?;
        Ok(true)
    }
}

/// Archon retries live uploads for up to an hour so a WCL blip doesn't kill a
/// whole raid night; 401 means the session expired -> re-login and retry.
async fn live_retry<T, F, Fut>(
    session: &wcl::WclSession,
    email: &str,
    password: &str,
    cancel: &mut watch::Receiver<bool>,
    progress: &LiveProgress,
    journal: &Journal,
    f: F,
) -> Result<T>
where
    F: Fn() -> Fut,
    Fut: std::future::Future<Output = Result<T>>,
{
    let mut last_err = None;
    for attempt in 1..=LIVE_RETRY_MAX {
        match f().await {
            Ok(v) => return Ok(v),
            Err(e) => {
                if cancelled(cancel) {
                    return Err(e);
                }
                let unauthorized = e
                    .root_cause()
                    .downcast_ref::<wcl::HttpStatus>()
                    .map(|s| s.0 == 401)
                    .unwrap_or(false);
                journal.write(
                    "upload_retry",
                    json!({"attempt": attempt, "unauthorized": unauthorized, "error": format!("{e:#}")}),
                );
                if unauthorized {
                    let _ = session.login(email, password).await;
                    tokio::select! {
                        _ = tokio::time::sleep(Duration::from_secs(3)) => {}
                        _ = cancel.changed() => {}
                    }
                } else {
                    progress.emit("retrying", format!("Upload failed (attempt {attempt}): {e:#}"));
                    tokio::select! {
                        _ = tokio::time::sleep(LIVE_RETRY_DELAY) => {}
                        _ = cancel.changed() => {}
                    }
                }
                last_err = Some(e);
            }
        }
    }
    Err(last_err.unwrap_or_else(|| anyhow!("upload failed")))
}
