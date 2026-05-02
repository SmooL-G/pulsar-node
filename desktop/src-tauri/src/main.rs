// Thin binary wrapper. All logic lives in lib.rs so the crate can also
// be reused on mobile (Tauri 2 convention).
#![cfg_attr(not(debug_assertions), windows_subsystem = "windows")]

fn main() {
    pulsar_desktop_lib::run();
}
