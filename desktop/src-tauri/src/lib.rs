// Hide the console window on Windows release builds.
#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

use serde::{Deserialize, Serialize};
use std::sync::Mutex;
use tauri::{
    menu::{Menu, MenuItem},
    tray::{MouseButton, MouseButtonState, TrayIconBuilder, TrayIconEvent},
    Emitter, Manager,
};
use tauri_plugin_autostart::MacosLauncher;

mod runner;

#[derive(Default, Serialize, Deserialize, Clone)]
struct AppConfig {
    node_id: Option<String>,
    token: Option<String>,
    api_url: Option<String>,
    port: Option<u16>,
    #[serde(default)]
    autostart: bool,
}

const STORE_FILE: &str = "config.json";

struct State {
    cfg: Mutex<AppConfig>,
    started: Mutex<bool>,
}

#[tauri::command]
fn get_config(state: tauri::State<'_, State>) -> AppConfig {
    state.cfg.lock().unwrap().clone()
}

#[tauri::command]
async fn save_config(
    new_cfg: AppConfig,
    state: tauri::State<'_, State>,
    app: tauri::AppHandle,
) -> Result<AppConfig, String> {
    {
        let mut cfg = state.cfg.lock().unwrap();
        *cfg = new_cfg.clone();
    }
    persist(&app, &new_cfg).map_err(|e| e.to_string())?;
    Ok(new_cfg)
}

#[tauri::command]
fn start_runner(state: tauri::State<'_, State>) -> Result<(), String> {
    let mut started = state.started.lock().unwrap();
    if *started {
        return Err("already running".into());
    }
    let cfg = state.cfg.lock().unwrap().clone();
    let node_id = cfg.node_id.clone().ok_or("missing node_id")?;
    let token = cfg.token.clone().ok_or("missing token")?;
    let api_url = cfg.api_url.unwrap_or_else(|| "https://pulsar-chat.fun".into());
    let port = cfg.port.unwrap_or(3030);

    runner::start(runner::RunnerConfig { api_url, node_id, token, port });
    *started = true;
    Ok(())
}

#[tauri::command]
fn runner_status() -> runner::StatsSnapshot {
    runner::snapshot()
}

#[tauri::command]
fn is_running(state: tauri::State<'_, State>) -> bool {
    *state.started.lock().unwrap()
}

/// Server-side token lookup — bypasses the WebView's CORS by doing the
/// HTTP call from Rust. Returns the parsed JSON the platform sends
/// back, or an error string for the UI to toast.
#[tauri::command]
async fn lookup_token(api_url: Option<String>, token: String) -> Result<serde_json::Value, String> {
    let base = api_url.unwrap_or_else(|| "https://pulsar-chat.fun".into());
    let url = format!("{}/api/v1/nodes/by-token", base.trim_end_matches('/'));
    let client = reqwest::Client::builder()
        .timeout(std::time::Duration::from_secs(10))
        .build()
        .map_err(|e| e.to_string())?;
    let res = client
        .get(&url)
        .bearer_auth(&token)
        .send()
        .await
        .map_err(|e| e.to_string())?;
    let status = res.status();
    if !status.is_success() {
        return Err(format!("HTTP {}", status.as_u16()));
    }
    res.json::<serde_json::Value>().await.map_err(|e| e.to_string())
}

fn config_path(app: &tauri::AppHandle) -> Result<std::path::PathBuf, String> {
    let dir = app
        .path()
        .app_config_dir()
        .map_err(|e| e.to_string())?;
    std::fs::create_dir_all(&dir).map_err(|e| e.to_string())?;
    Ok(dir.join(STORE_FILE))
}

fn load_or_default(app: &tauri::AppHandle) -> AppConfig {
    let Ok(p) = config_path(app) else { return AppConfig::default(); };
    if !p.exists() { return AppConfig::default(); }
    std::fs::read_to_string(&p)
        .ok()
        .and_then(|s| serde_json::from_str(&s).ok())
        .unwrap_or_default()
}

fn persist(app: &tauri::AppHandle, cfg: &AppConfig) -> Result<(), Box<dyn std::error::Error>> {
    let p = config_path(app).map_err(|e| std::io::Error::new(std::io::ErrorKind::Other, e))?;
    let json = serde_json::to_string_pretty(cfg)?;
    std::fs::write(p, json)?;
    Ok(())
}

fn build_tray_menu(app: &tauri::AppHandle) -> tauri::Result<Menu<tauri::Wry>> {
    let open = MenuItem::with_id(app, "open", "Open Pulsar Desktop", true, None::<&str>)?;
    let stats = MenuItem::with_id(app, "stats", "Show stats", true, None::<&str>)?;
    let separator = tauri::menu::PredefinedMenuItem::separator(app)?;
    let quit = MenuItem::with_id(app, "quit", "Quit", true, None::<&str>)?;
    Menu::with_items(app, &[&open, &stats, &separator, &quit])
}

fn show_main_window(app: &tauri::AppHandle) {
    if let Some(w) = app.get_webview_window("main") {
        let _ = w.show();
        let _ = w.unminimize();
        let _ = w.set_focus();
    }
}

#[cfg_attr(mobile, tauri::mobile_entry_point)]
pub fn run() {
    tauri::Builder::default()
        .plugin(tauri_plugin_autostart::init(
            MacosLauncher::LaunchAgent,
            None,
        ))
        .plugin(tauri_plugin_shell::init())
        .plugin(tauri_plugin_store::Builder::default().build())
        .plugin(tauri_plugin_dialog::init())
        .plugin(tauri_plugin_updater::Builder::new().build())
        .plugin(tauri_plugin_process::init())
        .invoke_handler(tauri::generate_handler![
            get_config,
            save_config,
            start_runner,
            runner_status,
            is_running,
            lookup_token,
        ])
        .setup(|app| {
            let app_handle = app.handle().clone();
            let cfg = load_or_default(&app_handle);
            let state = State {
                cfg: Mutex::new(cfg.clone()),
                started: Mutex::new(false),
            };
            app.manage(state);

            // Auto-start the runner if creds are present (silent boot).
            if cfg.node_id.is_some() && cfg.token.is_some() {
                let api_url = cfg.api_url.clone().unwrap_or_else(|| "https://pulsar-chat.fun".into());
                let port = cfg.port.unwrap_or(3030);
                runner::start(runner::RunnerConfig {
                    api_url,
                    node_id: cfg.node_id.unwrap(),
                    token: cfg.token.unwrap(),
                    port,
                });
                let s: tauri::State<State> = app.state();
                *s.started.lock().unwrap() = true;
            }

            // Build tray
            let menu = build_tray_menu(&app_handle)?;
            let _tray = TrayIconBuilder::with_id("main")
                .menu(&menu)
                .show_menu_on_left_click(false)
                .tooltip("Pulsar Desktop — relay node")
                .icon(app_handle.default_window_icon().cloned().ok_or("no icon")?)
                .on_menu_event(move |app, event| match event.id().as_ref() {
                    "open" => show_main_window(app),
                    "stats" => {
                        show_main_window(app);
                        let _ = app.emit("nav", "stats");
                    }
                    "quit" => app.exit(0),
                    _ => {}
                })
                .on_tray_icon_event(|tray, event| {
                    if let TrayIconEvent::Click { button, button_state, .. } = event {
                        if button == MouseButton::Left && button_state == MouseButtonState::Up {
                            show_main_window(tray.app_handle());
                        }
                    }
                })
                .build(app)?;

            Ok(())
        })
        // Keep the app running in tray when window is closed.
        .on_window_event(|window, event| {
            if let tauri::WindowEvent::CloseRequested { api, .. } = event {
                let _ = window.hide();
                api.prevent_close();
            }
        })
        .run(tauri::generate_context!())
        .expect("error while running tauri application");
}
