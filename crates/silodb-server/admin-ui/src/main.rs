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
use components::sidebar::{
    Sidebar, SidebarContent, SidebarFooter, SidebarGroup, SidebarGroupAction, SidebarGroupContent,
    SidebarGroupLabel, SidebarHeader, SidebarInset, SidebarMenu, SidebarMenuButton,
    SidebarMenuItem, SidebarMenuSub, SidebarMenuSubItem, SidebarProvider,
};
use dioxus::prelude::*;

#[derive(Clone, PartialEq)]
enum View {
    Keys,
    Sql,
    Table(String),
}

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
                    p { class: "muted", "admin panel" }
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
    let mut view = use_signal(|| View::Sql);
    let mut expanded = use_signal(Vec::<String>::new);
    let mut create_open = use_signal(|| false);
    let mut tables = use_resource(|| async { api::tables().await });
    let keys = use_resource(|| async { api::keys().await });

    rsx! {
        SidebarProvider {
            Sidebar {
                SidebarHeader {
                    div { class: "brand",
                        span { class: "brand-mark", "\u{25F1}" }
                        span { "silodb " span { class: "muted", "admin" } }
                    }
                }
                SidebarContent {
                    SidebarGroup {
                        SidebarGroupLabel { "Tables" }
                        SidebarGroupAction { title: "New table",
                            div { onclick: move |_| create_open.set(true), "\u{FF0B}" }
                        }
                        SidebarGroupContent {
                            SidebarMenu {
                                if let Some(Ok(list)) = &*tables.read() {
                                    if list.is_empty() {
                                        SidebarMenuItem {
                                            span { class: "muted small sidebar-note", "none yet; use \u{FF0B}" }
                                        }
                                    }
                                    for t in list.clone() {
                                        SidebarMenuItem {
                                            div {
                                                class: "table-node",
                                                onclick: {
                                                    let name = t.table.clone();
                                                    move |_| view.set(View::Table(name.clone()))
                                                },
                                                SidebarMenuButton {
                                                    is_active: view() == View::Table(t.table.clone()),
                                                    span {
                                                        class: "tree-chevron",
                                                        onclick: {
                                                            let name = t.table.clone();
                                                            move |e: MouseEvent| {
                                                                e.stop_propagation();
                                                                let mut ex = expanded();
                                                                match ex.iter().position(|x| *x == name) {
                                                                    Some(i) => { ex.remove(i); }
                                                                    None => ex.push(name.clone()),
                                                                }
                                                                expanded.set(ex);
                                                            }
                                                        },
                                                        if expanded().contains(&t.table) { "\u{25BE}" } else { "\u{25B8}" }
                                                    }
                                                    span { class: "menu-table mono", "{t.table}" }
                                                }
                                            }
                                            if expanded().contains(&t.table) {
                                                SidebarMenuSub {
                                                    for c in &t.columns {
                                                        SidebarMenuSubItem {
                                                            span { class: "tree-col mono",
                                                                "{c.name}"
                                                                span { class: "muted", " {c.ty.to_lowercase()}" }
                                                                if c.name == t.ts_column { span { class: "muted", " \u{23F1}" } }
                                                            }
                                                        }
                                                    }
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    }
                    SidebarGroup {
                        SidebarGroupLabel { "Server" }
                        SidebarGroupContent {
                            SidebarMenu {
                                for (v, label) in [(View::Sql, "SQL console"), (View::Keys, "API keys")] {
                                    SidebarMenuItem {
                                        div {
                                            onclick: {
                                                let v = v.clone();
                                                move |_| view.set(v.clone())
                                            },
                                            SidebarMenuButton { is_active: view() == v, "{label}" }
                                        }
                                    }
                                }
                            }
                        }
                    }
                }
                SidebarFooter {
                    Button {
                        variant: ButtonVariant::Ghost,
                        size: ButtonSize::Sm,
                        onclick: move |_| on_logout.call(()),
                        "Sign out"
                    }
                }
            }
            SidebarInset {
                main { class: "content",
                    match view() {
                        View::Keys => rsx! { views::KeysView { keys, tables } },
                        View::Sql => rsx! { views::SqlView { tables } },
                        View::Table(t) => rsx! { views::TableDetail { key: "{t}", table: t.clone(), tables, keys } },
                    }
                }
            }
        }
        if create_open() {
            views::CreateTableDialog {
                onclose: move |_| create_open.set(false),
                ondone: move |_| { create_open.set(false); tables.restart(); },
            }
        }
    }
}
