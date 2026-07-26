#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

mod live;
mod manage;
mod parser;
mod wcl;

use std::path::PathBuf;

use anyhow::{anyhow, Context as _, Result};
use serde::{Deserialize, Serialize};
use serde_json::json;
use tauri::{AppHandle, Emitter, Manager as _};
use tauri_plugin_dialog::DialogExt;
use tauri_plugin_opener::OpenerExt;

const BATCH_SIZE: usize = 100_000;
const UPLOAD_UI_RESERVED_PCT: u32 = 10; // the first 10% are reserved for client-side read

#[derive(Deserialize, Debug)]
#[serde(rename_all = "camelCase")]
struct UploadArgs {
    log_path: String,
    email: String,
    password: String,
    region: i32,
    visibility: i32,
    guild_id: Option<i64>,
    /// RPGLogs site id ("warcraft", "ff", "eso", ...). Defaults to warcraft.
    #[serde(default)]
    game: String,
}

#[derive(Serialize)]
struct VersionInfo {
    app: &'static str,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct GuildOption {
    /// Guild id, or `null` for "Personal Logs" (WCL sends -1).
    id: Option<i64>,
    label: String,
    region_id: Option<i64>,
}

#[derive(Serialize)]
#[serde(rename_all = "camelCase")]
struct LoginResult {
    /// WCL account name; `null` means the credentials were rejected.
    user_name: Option<String>,
    guilds: Vec<GuildOption>,
}

#[derive(Serialize)]
struct FileInfo {
    path: String,
    name: String,
    size: u64,
}

#[tauri::command]
fn app_version() -> VersionInfo {
    VersionInfo {
        app: env!("CARGO_PKG_VERSION"),
    }
}

/// native file picker.
#[tauri::command]
async fn pick_log_file(app: AppHandle) -> Option<FileInfo> {
    let (tx, rx) = tokio::sync::oneshot::channel();
    app.dialog()
        .file()
        .add_filter("Combat log", &["txt"])
        .pick_file(move |path| {
            let _ = tx.send(path);
        });
    let path = rx.await.ok().flatten()?;
    let pb = path.as_path()?.to_path_buf();
    Some(describe_file(&pb))
}

/// file info for a user-dropped path 
#[tauri::command]
fn file_info(path: String) -> Result<FileInfo, String> {
    let pb = std::path::PathBuf::from(&path);
    if !pb.is_file() {
        return Err(format!("not a file: {path}"));
    }
    Ok(describe_file(&pb))
}

/// external URL handler
#[tauri::command]
fn open_url(app: AppHandle, url: String) -> Result<(), String> {
    app.opener()
        .open_url(url, None::<String>)
        .map_err(|e| format!("failed to open URL: {e}"))
}

fn describe_file(path: &std::path::Path) -> FileInfo {
    let name = path
        .file_name()
        .and_then(|f| f.to_str())
        .unwrap_or("")
        .to_string();
    let size = std::fs::metadata(path).map(|m| m.len()).unwrap_or(0);
    FileInfo {
        path: path.to_string_lossy().to_string(),
        name,
        size,
    }
}

#[tauri::command]
async fn fetch_guilds(
    email: String,
    password: String,
    game: Option<String>,
) -> Result<LoginResult, String> {
    let session = wcl::WclSession::new(game.as_deref().unwrap_or("warcraft"))
        .await
        .map_err(|e| format!("{e:#}"))?;
    let login = session
        .login(&email, &password)
        .await
        .map_err(|e| format!("{e:#}"))?;
    let user_name = login.user.as_ref().and_then(|u| u.user_name.clone());
    let items = login.guild_select_items.unwrap_or_default();
    let guilds = items
        .into_iter()
        .filter_map(|it| {
            let value = it.get("value").and_then(|v| v.as_i64())?;
            Some(GuildOption {
                id: if value < 0 { None } else { Some(value) },
                label: it
                    .get("label")
                    .and_then(|v| v.as_str())
                    .unwrap_or("")
                    .to_string(),
                region_id: it.get("regionId").and_then(|v| v.as_i64()),
            })
        })
        .collect();
    Ok(LoginResult { user_name, guilds })
}

/// start upload
#[tauri::command]
async fn start_upload(app: AppHandle, args: UploadArgs) -> Result<(), String> {
    tokio::spawn(async move {
        if let Err(e) = run_upload(&app, args).await {
            let _ = app.emit(
                "upload:error",
                json!({"message": format!("{e:#}")}),
            );
        }
    });
    Ok(())
}

/// Holds the live-log stop signal. The `u64` is a generation counter so a slow
/// shutdown of an old run can't clobber the state of a newer one.
#[derive(Default)]
struct LiveLogState(std::sync::Mutex<(u64, Option<tokio::sync::watch::Sender<bool>>)>);

/// native folder picker for the live log directory.
#[tauri::command]
async fn pick_log_directory(app: AppHandle) -> Option<FileInfo> {
    let (tx, rx) = tokio::sync::oneshot::channel();
    app.dialog().file().pick_folder(move |path| {
        let _ = tx.send(path);
    });
    let path = rx.await.ok().flatten()?;
    let pb = path.as_path()?.to_path_buf();
    Some(describe_file(&pb))
}

/// dir info for a user-dropped path
#[tauri::command]
fn dir_info(path: String) -> Result<FileInfo, String> {
    let pb = std::path::PathBuf::from(&path);
    if !pb.is_dir() {
        return Err(format!("not a directory: {path}"));
    }
    Ok(describe_file(&pb))
}

#[tauri::command]
async fn start_live_log(
    app: AppHandle,
    state: tauri::State<'_, LiveLogState>,
    args: live::LiveLogArgs,
) -> Result<(), String> {
    let (rx, my_gen) = {
        let mut guard = state.0.lock().unwrap();
        if guard.1.as_ref().map(|tx| !tx.is_closed()).unwrap_or(false) {
            return Err("live log already running".into());
        }
        let (tx, rx) = tokio::sync::watch::channel(false);
        guard.0 += 1;
        guard.1 = Some(tx);
        (rx, guard.0)
    };
    tokio::spawn(async move {
        if let Err(e) = live::run_live_log(app.clone(), args, rx).await {
            let _ = app.emit("live:error", json!({"message": format!("{e:#}")}));
        }
        let state = app.state::<LiveLogState>();
        let mut guard = state.0.lock().unwrap();
        if guard.0 == my_gen {
            guard.1 = None;
        }
    });
    Ok(())
}

#[tauri::command]
fn stop_live_log(state: tauri::State<'_, LiveLogState>) -> Result<(), String> {
    match state.0.lock().unwrap().1.take() {
        Some(tx) => {
            let _ = tx.send(true);
            Ok(())
        }
        None => Err("no live log running".into()),
    }
}

/// List `.txt` files in a folder (for the Manage Logs archive picker). Cheap:
/// directory metadata only, no file contents are read.
#[tauri::command]
fn scan_logs(path: String) -> Result<Vec<FileInfo>, String> {
    let dir = std::path::PathBuf::from(&path);
    if !dir.is_dir() {
        return Err(format!("not a directory: {path}"));
    }
    let mut out = Vec::new();
    for entry in std::fs::read_dir(&dir).map_err(|e| e.to_string())? {
        let entry = entry.map_err(|e| e.to_string())?;
        let p = entry.path();
        let is_txt = p
            .extension()
            .and_then(|x| x.to_str())
            .map(|x| x.eq_ignore_ascii_case("txt"))
            .unwrap_or(false);
        if is_txt && p.is_file() {
            out.push(describe_file(&p));
        }
    }
    out.sort_by(|a, b| a.name.cmp(&b.name));
    Ok(out)
}

/// Split a combat log into per-session files on a blocking thread.
#[tauri::command]
async fn start_split(app: AppHandle, args: manage::SplitArgs) -> Result<(), String> {
    tokio::task::spawn_blocking(move || match manage::split_log(&app, args) {
        Ok(files) => {
            let _ = app.emit(
                "manage:done",
                json!({ "kind": "split", "count": files.len(), "files": files }),
            );
        }
        Err(e) => {
            let _ = app.emit("manage:error", json!({ "message": format!("{e:#}") }));
        }
    });
    Ok(())
}

/// Zip + archive the selected logs on a blocking thread. With no `destDir` they
/// go to the archive folder inside their own logs folder.
#[tauri::command]
async fn start_archive(app: AppHandle, args: manage::ArchiveArgs) -> Result<(), String> {
    tokio::task::spawn_blocking(move || match manage::archive_logs(&app, args) {
        Ok(res) => {
            let _ = app.emit(
                "manage:done",
                json!({ "kind": "archive", "count": res.count, "destDir": res.dest_dir }),
            );
        }
        Err(e) => {
            let _ = app.emit("manage:error", json!({ "message": format!("{e:#}") }));
        }
    });
    Ok(())
}

fn main() {
    tauri::Builder::default()
        .plugin(tauri_plugin_shell::init())
        .plugin(tauri_plugin_dialog::init())
        .plugin(tauri_plugin_opener::init())
        .manage(LiveLogState::default())
        .invoke_handler(tauri::generate_handler![
            app_version,
            pick_log_file,
            file_info,
            open_url,
            fetch_guilds,
            start_upload,
            pick_log_directory,
            dir_info,
            start_live_log,
            stop_live_log,
            scan_logs,
            start_split,
            start_archive
        ])
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}

fn emit_progress(app: &AppHandle, step: &str, message: impl Into<String>, pct: u32) {
    let _ = app.emit(
        "upload:progress",
        json!({
            "step": step,
            "message": message.into(),
            "pct": pct,
        }),
    );
}

/// this is (hopefully) a mirror of `upload_worker` in `web/webapp.py`.
/// (TODO: I should really deduplicate this)
async fn run_upload(app: &AppHandle, args: UploadArgs) -> Result<()> {
    let log_path = PathBuf::from(&args.log_path);
    let filename = log_path
        .file_name()
        .and_then(|f| f.to_str())
        .unwrap_or("log.txt")
        .to_string();

    emit_progress(app, "read", "Reading log file...", 1);
    let raw = tokio::fs::read_to_string(&log_path)
        .await
        .with_context(|| format!("reading {}", log_path.display()))?;
    let all_lines: Vec<String> = raw
        .lines()
        .map(|s| s.to_string())
        .collect();
    let total = all_lines.len();
    emit_progress(
        app,
        "read",
        format!("Read {} lines", format_with_commas(total)),
        2,
    );

    emit_progress(app, "session", "Initializing session...", 3);
    let session = wcl::WclSession::new(&args.game).await?;

    emit_progress(app, "login", "Logging in...", 4);
    let login = session.login(&args.email, &args.password).await?;
    let user_name = login
        .user
        .as_ref()
        .and_then(|u| u.user_name.as_deref())
        .unwrap_or("?")
        .to_string();
    emit_progress(app, "login", format!("Logged in as {user_name}"), 5);

    emit_progress(app, "fetch-parser", "Fetching latest parser...", 6);
    let bundle = session.fetch_parser_code().await?;
    let parser_version = bundle.parser_version;
    emit_progress(
        app,
        "fetch-parser",
        format!("Parser v{parser_version} loaded"),
        7,
    );

    let harness = parser::harness_path(app)?;
    emit_progress(app, "parser", "Starting parser...", 8);
    let parser = parser::Parser::spawn(app, &harness, &bundle.gamedata_code, &bundle.parser_code)
        .await?;
    parser.clear_state().await?;
    if let Some(date) = wcl::parse_start_date(&filename) {
        parser.set_start_date(&date).await?;
    }
    emit_progress(app, "parser", "Parser ready", 9);

    let mut segment_id: i64 = 1;
    let mut report_code: Option<String> = None;
    let mut last_master_ids: Option<(i64, i64, i64, i64)> = None;
    let total_batches = (total + BATCH_SIZE - 1) / BATCH_SIZE;

    for (batch_idx, chunk) in all_lines.chunks(BATCH_SIZE).enumerate() {
        let batch_num = batch_idx + 1;
        let pct = UPLOAD_UI_RESERVED_PCT
            + (80 * batch_num as u32 / total_batches.max(1) as u32);

        parser.parse_lines(&chunk.to_vec(), args.region).await?;
        let fd = parser.collect_fights(true).await?;
        let fights = fd.get("fights").and_then(|v| v.as_array());
        if fights.map(|a| a.is_empty()).unwrap_or(true) {
            emit_progress(
                app,
                "parse",
                format!("Batch {batch_num}/{total_batches} — no fights yet"),
                pct,
            );
            continue;
        }

        if report_code.is_none() {
            let start_time = fd.get("startTime").and_then(|v| v.as_i64()).unwrap_or(0);
            let end_time = fd.get("endTime").and_then(|v| v.as_i64()).unwrap_or(0);
            let code = session
                .create_report(
                    &filename,
                    start_time,
                    end_time,
                    args.region,
                    args.visibility,
                    args.guild_id,
                    parser_version,
                )
                .await?;
            emit_progress(
                app,
                "report",
                format!("Report created: {code}"),
                pct,
            );
            report_code = Some(code);
        }
        let code = report_code.as_deref().unwrap();

        let mi = parser.collect_master_info().await?;
        let master_ids = wcl::master_ids(&mi);
        if Some(master_ids) != last_master_ids {
            let log_version = fd.get("logVersion").and_then(|v| v.as_i64()).unwrap_or(0);
            let game_version = fd.get("gameVersion").and_then(|v| v.as_i64()).unwrap_or(0);
            let master = wcl::build_master_string(&mi, log_version, game_version);
            let zipped = wcl::make_zip(&master)?;
            session.set_master_table(code, segment_id, false, zipped).await?;
            last_master_ids = Some(master_ids);
        }

        let evts: i64 = fights
            .map(|a| {
                a.iter()
                    .filter_map(|f| f.get("eventCount").and_then(|n| n.as_i64()))
                    .sum()
            })
            .unwrap_or(0);
        let start_time = fd.get("startTime").and_then(|v| v.as_i64()).unwrap_or(0);
        let end_time = fd.get("endTime").and_then(|v| v.as_i64()).unwrap_or(0);
        let mythic = fd.get("mythic").and_then(|v| v.as_i64()).unwrap_or(0) as i32;

        let fights_str = wcl::build_fights_string(&fd);
        let zipped = wcl::make_zip(&fights_str)?;
        let next = session
            .add_segment(
                code, segment_id, start_time, end_time, mythic, false, false, 0, zipped,
            )
            .await?;
        segment_id = if next > 0 { next } else { segment_id + 1 };
        parser.clear_fights().await?;
        emit_progress(
            app,
            "upload",
            format!(
                "Segment {batch_num}/{total_batches} — {} events",
                format_with_commas(evts as usize)
            ),
            pct,
        );
    }

    parser.close().await;

    match report_code {
        Some(code) => {
            session.terminate_report(&code).await?;
            let url = session.report_url(&code);
            let _ = app.emit("upload:done", json!({"url": url, "code": code}));
            Ok(())
        }
        None => Err(anyhow!("No fights found in log file.")),
    }
}

fn format_with_commas(n: usize) -> String {
    let s = n.to_string();
    let bytes = s.as_bytes();
    let mut out = String::with_capacity(s.len() + s.len() / 3);
    for (i, &b) in bytes.iter().enumerate() {
        if i > 0 && (bytes.len() - i) % 3 == 0 {
            out.push(',');
        }
        out.push(b as char);
    }
    out
}
