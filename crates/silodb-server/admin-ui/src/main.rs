//! silodb admin — a Dioxus web (WASM) SPA served by silodb-server at
//! /admin. Pure Rust end to end: rsx! views over the official
//! dioxus-components (shadcn-style), a typed client speaking the same
//! JSON API curl uses, no node, no SSR. Built with `dx build --release`;
//! the output is committed and embedded into the server binary.

mod api;
mod components;
mod views;

use components::button::{Button, ButtonSize, ButtonVariant};
use components::input::Input;
use components::label::Label;
use components::tabs::{TabContent, TabList, TabTrigger, Tabs};
use dioxus::prelude::*;

const MAIN_CSS: Asset = asset!("/assets/main.css");
const THEME_CSS: Asset = asset!("/assets/dx-components-theme.css");

fn main() {
    dioxus::launch(App);
}

#[component]
fn App() -> Element {
    let mut authed = use_signal(|| !api::token().is_empty());
    rsx! {
        document::Title { "silodb admin" }
        document::Link { rel: "stylesheet", href: THEME_CSS }
        document::Link { rel: "stylesheet", href: MAIN_CSS }
        if authed() {
            Shell { on_logout: move |_| { api::clear_token(); authed.set(false); } }
        } else {
            Login { on_login: move |_| authed.set(true) }
        }
    }
}

#[component]
fn Login(on_login: EventHandler<()>) -> Element {
    let mut token = use_signal(String::new);
    let mut error = use_signal(|| None::<String>);
    let mut busy = use_signal(|| false);

    let mut submit = move || {
        let t = token();
        if t.is_empty() {
            return;
        }
        busy.set(true);
        spawn(async move {
            api::set_token(&t);
            match api::tables().await {
                Ok(_) => on_login.call(()),
                Err(e) => {
                    api::clear_token();
                    error.set(Some(if e.status == 401 {
                        "that token isn't valid".into()
                    } else {
                        e.message
                    }));
                }
            }
            busy.set(false);
        });
    };

    rsx! {
        div { class: "login-wrap",
            div { class: "panel login-card",
                div { class: "login-head",
                    h1 { "silodb" }
                    p { class: "muted", "time-series on SQLite — admin panel" }
                }
                div { class: "field",
                    Label { html_for: "", "API token" }
                    Input {
                        r#type: "password",
                        placeholder: "ddl token or sk_…",
                        value: "{token}",
                        style: "width: 100%",
                        oninput: move |e: FormEvent| token.set(e.value()),
                        onkeydown: move |e: KeyboardEvent| if e.key() == Key::Enter { submit() },
                    }
                }
                if let Some(e) = error() {
                    div { class: "error-note", "{e}" }
                }
                div { class: "login-btn",
                    Button { onclick: move |_| submit(), disabled: busy(), "Connect" }
                }
                p { class: "muted small",
                    "Tokens are checked against the server; only stored in this browser."
                }
            }
        }
    }
}

#[component]
fn Shell(on_logout: EventHandler<()>) -> Element {
    let tables = use_resource(|| async { api::tables().await });
    let keys = use_resource(|| async { api::keys().await });

    rsx! {
        div { class: "shell",
            header { class: "topbar",
                div { class: "brand",
                    span { class: "brand-mark", "◱" }
                    span { "silodb " span { class: "muted", "admin" } }
                }
                div { class: "topbar-spacer" }
                Button {
                    variant: ButtonVariant::Ghost,
                    size: ButtonSize::Sm,
                    onclick: move |_| on_logout.call(()),
                    "Sign out"
                }
            }
            main { class: "content",
                Tabs { default_value: "tables".to_string(),
                    TabList {
                        TabTrigger { value: "tables".to_string(), index: 0usize, "Tables" }
                        TabTrigger { value: "keys".to_string(), index: 1usize, "API keys" }
                        TabTrigger { value: "sql".to_string(), index: 2usize, "SQL" }
                    }
                    TabContent { value: "tables".to_string(), index: 0usize,
                        views::TablesView { tables, keys }
                    }
                    TabContent { value: "keys".to_string(), index: 1usize,
                        views::KeysView { keys, tables }
                    }
                    TabContent { value: "sql".to_string(), index: 2usize,
                        views::SqlView {}
                    }
                }
            }
        }
    }
}
