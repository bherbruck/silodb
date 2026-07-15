//! The three admin views, built on the official dioxus-components.
//! Dialogs are conditionally rendered with controlled `open`; every
//! mutation calls the typed client and restarts the relevant resource.

use crate::api::{self, ApiError, CreateKey, CreateTable, KeyInfo, SqlResult, TableInfo};
use crate::components::badge::{Badge, BadgeVariant};
use crate::components::button::{Button, ButtonSize, ButtonVariant};
use crate::components::card::{Card, CardAction, CardContent, CardDescription, CardHeader, CardTitle};
use crate::components::dialog::{Dialog, DialogDescription, DialogTitle};
use crate::components::input::Input;
use crate::components::label::Label;
use crate::components::textarea::Textarea;
use dioxus::prelude::*;

type Res<T> = Resource<Result<Vec<T>, ApiError>>;

// --- shared bits ----------------------------------------------------------

#[component]
fn ErrorNote(error: Option<String>) -> Element {
    rsx! {
        if let Some(e) = error {
            div { class: "error-note", "{e}" }
        }
    }
}

fn fmt_date(us: i64) -> String {
    // Days-precision civil date from epoch µs (Hinnant), enough for a list.
    let days = us.div_euclid(86_400_000_000);
    let z = days + 719_468;
    let era = z.div_euclid(146_097);
    let doe = z.rem_euclid(146_097);
    let yoe = (doe - doe / 1460 + doe / 36524 - doe / 146_096) / 365;
    let y = yoe + era * 400;
    let doy = doe - (365 * yoe + yoe / 4 - yoe / 100);
    let mp = (5 * doy + 2) / 153;
    let d = doy - (153 * mp + 2) / 5 + 1;
    let m = if mp < 10 { mp + 3 } else { mp - 9 };
    let y = if m <= 2 { y + 1 } else { y };
    format!("{y:04}-{m:02}-{d:02}")
}

// --- tables ---------------------------------------------------------------

#[component]
pub fn TablesView(tables: Res<TableInfo>, keys: Res<KeyInfo>) -> Element {
    let mut create_open = use_signal(|| false);
    let mut column_for = use_signal(|| None::<String>);
    let mut retention_for = use_signal(|| None::<TableInfo>);

    rsx! {
        Card {
            CardHeader {
                CardTitle { "Tables" }
                CardDescription { "hot SQLite tier + tiered parquet, one name each" }
                CardAction {
                    Button { size: ButtonSize::Sm, onclick: move |_| create_open.set(true), "＋ New table" }
                }
            }
            CardContent {
                match &*tables.read() {
                    None => rsx! { div { class: "empty", "loading…" } },
                    Some(Err(e)) => rsx! { div { class: "empty", "error: {e}" } },
                    Some(Ok(list)) if list.is_empty() => rsx! {
                        div { class: "empty", "No tables yet — create one, or let a ddl key autoschema it via /write." }
                    },
                    Some(Ok(list)) => rsx! {
                        table { class: "table",
                            thead { tr {
                                th { "Table" } th { "Columns" } th { "Tiers" } th { "Retention" }
                                th { class: "num", "Hot rows" } th { class: "num", "Cold rows" }
                                th { class: "num", "Files" } th {}
                            } }
                            tbody {
                                for t in list.clone() {
                                    TableRow {
                                        t: t.clone(),
                                        on_add_column: move |name| column_for.set(Some(name)),
                                        on_retention: move |t| retention_for.set(Some(t)),
                                    }
                                }
                            }
                        }
                    },
                }
            }
        }
        if create_open() {
            CreateTableDialog {
                onclose: move |_| create_open.set(false),
                ondone: move |_| { create_open.set(false); tables.restart(); keys.restart(); },
            }
        }
        if let Some(table) = column_for() {
            AddColumnDialog {
                table,
                onclose: move |_| column_for.set(None),
                ondone: move |_| { column_for.set(None); tables.restart(); },
            }
        }
        if let Some(t) = retention_for() {
            RetentionDialog {
                t,
                onclose: move |_| retention_for.set(None),
                ondone: move |_| { retention_for.set(None); tables.restart(); },
            }
        }
    }
}

#[component]
fn TableRow(t: TableInfo, on_add_column: EventHandler<String>, on_retention: EventHandler<TableInfo>) -> Element {
    let name = t.table.clone();
    let t2 = t.clone();
    rsx! {
        tr {
            td { class: "strong", "{t.table}" }
            td {
                div { class: "chips",
                    for c in &t.columns {
                        span { class: "chip mono", title: "{c.ty}",
                            "{c.name}"
                            if c.name == t.ts_column { span { class: "muted", " ⏱" } }
                        }
                    }
                }
            }
            td {
                div { class: "chips",
                    for tier in &t.tiers {
                        Badge { variant: BadgeVariant::Secondary, "{tier}" }
                    }
                }
            }
            td {
                if let Some(r) = &t.retention {
                    Badge { variant: BadgeVariant::Secondary, "{r}" }
                } else {
                    span { class: "muted small", "forever" }
                }
            }
            td { class: "num", "{t.hot_rows}" }
            td { class: "num", "{t.cold_rows}" }
            td { class: "num", "{t.active_files}" }
            td { class: "row-actions",
                Button { variant: ButtonVariant::Ghost, size: ButtonSize::IconSm, title: "Add column",
                    onclick: move |_| on_add_column.call(name.clone()), "▦" }
                Button { variant: ButtonVariant::Ghost, size: ButtonSize::IconSm, title: "Retention",
                    onclick: move |_| on_retention.call(t2.clone()), "◷" }
            }
        }
    }
}

#[component]
fn CreateTableDialog(onclose: EventHandler<()>, ondone: EventHandler<()>) -> Element {
    let mut name = use_signal(String::new);
    let mut schema = use_signal(|| "ts TIMESTAMP, device TEXT, value REAL".to_string());
    let mut tiers = use_signal(|| "1d,7d".to_string());
    let mut retention = use_signal(String::new);
    let mut error = use_signal(|| None::<String>);

    let submit = move |_| {
        let req = CreateTable {
            name: name(),
            schema: schema(),
            tiers: Some(tiers()).filter(|s| !s.is_empty()),
            retention: Some(retention()).filter(|s| !s.is_empty()),
        };
        spawn(async move {
            match api::create_table(&req).await {
                Ok(()) => ondone.call(()),
                Err(e) => error.set(Some(e.message)),
            }
        });
    };

    rsx! {
        Dialog { open: Some(true), on_open_change: move |o: bool| if !o { onclose.call(()) },
            DialogTitle { "Create table" }
            DialogDescription { "One TIMESTAMP column is the bucket axis." }
            div { class: "field",
                Label { html_for: "", "Name" }
                Input { placeholder: "readings", value: "{name}", style: "width: 100%",
                    oninput: move |e: FormEvent| name.set(e.value()) }
            }
            div { class: "field",
                Label { html_for: "", "Schema" }
                Input { class: "mono", value: "{schema}", style: "width: 100%",
                    oninput: move |e: FormEvent| schema.set(e.value()) }
            }
            div { class: "two-col",
                div { class: "field",
                    Label { html_for: "", "Tiers" }
                    Input { placeholder: "1d,7d,28d", value: "{tiers}", style: "width: 100%",
                        oninput: move |e: FormEvent| tiers.set(e.value()) }
                }
                div { class: "field",
                    Label { html_for: "", "Retention (blank = forever)" }
                    Input { placeholder: "2y", value: "{retention}", style: "width: 100%",
                        oninput: move |e: FormEvent| retention.set(e.value()) }
                }
            }
            ErrorNote { error: error() }
            div { class: "modal-actions",
                Button { variant: ButtonVariant::Outline, onclick: move |_| onclose.call(()), "Cancel" }
                Button { disabled: name().is_empty() || schema().is_empty(), onclick: submit, "Create" }
            }
        }
    }
}

#[component]
fn AddColumnDialog(table: String, onclose: EventHandler<()>, ondone: EventHandler<()>) -> Element {
    let mut coldef = use_signal(String::new);
    let mut error = use_signal(|| None::<String>);
    let submit = move |_| {
        let (t, c) = (table.clone(), coldef());
        spawn(async move {
            match api::add_column(&t, &c).await {
                Ok(()) => ondone.call(()),
                Err(e) => error.set(Some(e.message)),
            }
        });
    };
    rsx! {
        Dialog { open: Some(true), on_open_change: move |o: bool| if !o { onclose.call(()) },
            DialogTitle { "Add column" }
            DialogDescription { "Instant — existing rows (parquet included) read NULL." }
            div { class: "field",
                Label { html_for: "", "Column definition" }
                Input { class: "mono", placeholder: "humidity REAL", value: "{coldef}",
                    style: "width: 100%", oninput: move |e: FormEvent| coldef.set(e.value()) }
            }
            ErrorNote { error: error() }
            div { class: "modal-actions",
                Button { variant: ButtonVariant::Outline, onclick: move |_| onclose.call(()), "Cancel" }
                Button { disabled: coldef().is_empty(), onclick: submit, "Add column" }
            }
        }
    }
}

#[component]
fn RetentionDialog(t: TableInfo, onclose: EventHandler<()>, ondone: EventHandler<()>) -> Element {
    let mut retain = use_signal(String::new);
    let mut error = use_signal(|| None::<String>);
    let current = t.retention.clone().unwrap_or_else(|| "keep forever".into());
    let largest = t.tiers.last().cloned().unwrap_or_default();
    let name = t.table.clone();
    let name2 = t.table.clone();

    let apply = move |_| {
        let (n, r) = (name.clone(), retain());
        spawn(async move {
            match api::set_retention(&n, Some(&r)).await {
                Ok(()) => ondone.call(()),
                Err(e) => error.set(Some(e.message)),
            }
        });
    };
    let clear = move |_| {
        let n = name2.clone();
        spawn(async move {
            match api::set_retention(&n, None).await {
                Ok(()) => ondone.call(()),
                Err(e) => error.set(Some(e.message)),
            }
        });
    };

    rsx! {
        Dialog { open: Some(true), on_open_change: move |o: bool| if !o { onclose.call(()) },
            DialogTitle { "Retention for {t.table}" }
            DialogDescription {
                "Currently: {current}. Must be ≥ the largest tier ({largest}); files entirely "
                "older than the window are deleted by maintenance."
            }
            div { class: "field",
                Label { html_for: "", "New retention" }
                Input { placeholder: "8w", value: "{retain}", style: "width: 100%",
                    oninput: move |e: FormEvent| retain.set(e.value()) }
            }
            ErrorNote { error: error() }
            div { class: "modal-actions spread",
                Button { variant: ButtonVariant::Destructive, onclick: clear, "Keep forever" }
                div { class: "btn-row",
                    Button { variant: ButtonVariant::Outline, onclick: move |_| onclose.call(()), "Cancel" }
                    Button { disabled: retain().is_empty(), onclick: apply, "Apply" }
                }
            }
        }
    }
}

// --- keys -----------------------------------------------------------------

#[component]
pub fn KeysView(keys: Res<KeyInfo>, tables: Res<TableInfo>) -> Element {
    let mut create_open = use_signal(|| false);
    let mut secret = use_signal(|| None::<String>);
    let mut error = use_signal(|| None::<String>);

    let revoke = move |name: String| {
        spawn(async move {
            match api::revoke_key(&name).await {
                Ok(()) => keys.restart(),
                Err(e) => error.set(Some(e.message)),
            }
        });
    };

    rsx! {
        Card {
            CardHeader {
                CardTitle { "API keys" }
                CardDescription { "scoped credentials — only the SHA-256 hash is stored" }
                CardAction {
                    Button { size: ButtonSize::Sm, onclick: move |_| create_open.set(true), "＋ New key" }
                }
            }
            CardContent {
                ErrorNote { error: error() }
                match &*keys.read() {
                    None => rsx! { div { class: "empty", "loading…" } },
                    Some(Err(e)) => rsx! { div { class: "empty", "error: {e}" } },
                    Some(Ok(list)) if list.is_empty() => rsx! {
                        div { class: "empty", "No keys yet. Env tokens are the root credentials; mint scoped keys for clients." }
                    },
                    Some(Ok(list)) => rsx! {
                        table { class: "table",
                            thead { tr {
                                th { "Name" } th { "Role" } th { "Scope" } th { "Created" } th { "Status" } th {}
                            } }
                            tbody {
                                for k in list.clone() {
                                    tr { class: if k.revoked { "dim" } else { "" },
                                        td { class: "strong", "{k.name}" }
                                        td {
                                            Badge {
                                                variant: if k.role == "ddl" { BadgeVariant::Primary } else { BadgeVariant::Secondary },
                                                "{k.role}"
                                            }
                                        }
                                        td {
                                            if let Some(scope) = &k.scope {
                                                div { class: "chips",
                                                    for t in scope {
                                                        Badge { variant: BadgeVariant::Secondary, "{t}" }
                                                    }
                                                }
                                            } else {
                                                span { class: "muted small", "all tables" }
                                            }
                                        }
                                        td { class: "muted small", "{fmt_date(k.created_at)}" }
                                        td {
                                            if k.revoked {
                                                Badge { variant: BadgeVariant::Destructive, "revoked" }
                                            } else {
                                                Badge { variant: BadgeVariant::Outline, "active" }
                                            }
                                        }
                                        td { class: "row-actions",
                                            if !k.revoked {
                                                Button {
                                                    variant: ButtonVariant::Ghost,
                                                    size: ButtonSize::IconSm,
                                                    title: "Revoke",
                                                    onclick: {
                                                        let name = k.name.clone();
                                                        move |_| revoke(name.clone())
                                                    },
                                                    "⊘"
                                                }
                                            }
                                        }
                                    }
                                }
                            }
                        }
                    },
                }
            }
        }
        if create_open() {
            CreateKeyDialog {
                tables,
                onclose: move |_| create_open.set(false),
                oncreated: move |s| { create_open.set(false); secret.set(Some(s)); keys.restart(); },
            }
        }
        if let Some(s) = secret() {
            SecretDialog { secret: s, onclose: move |_| secret.set(None) }
        }
    }
}

#[component]
fn CreateKeyDialog(
    tables: Res<TableInfo>,
    onclose: EventHandler<()>,
    oncreated: EventHandler<String>,
) -> Element {
    let mut name = use_signal(String::new);
    let mut role = use_signal(|| "write".to_string());
    let mut scope = use_signal(Vec::<String>::new);
    let mut error = use_signal(|| None::<String>);

    let submit = move |_| {
        let req = CreateKey { name: name(), role: role(), scope: scope() };
        spawn(async move {
            match api::create_key(&req).await {
                Ok(secret) => oncreated.call(secret),
                Err(e) => error.set(Some(e.message)),
            }
        });
    };

    let table_names: Vec<String> = match &*tables.read() {
        Some(Ok(list)) => list.iter().map(|t| t.table.clone()).collect(),
        _ => Vec::new(),
    };

    rsx! {
        Dialog { open: Some(true), on_open_change: move |o: bool| if !o { onclose.call(()) },
            DialogTitle { "Create API key" }
            DialogDescription { "The secret is shown exactly once; only its hash is stored." }
            div { class: "field",
                Label { html_for: "", "Name" }
                Input { placeholder: "site-a", value: "{name}", style: "width: 100%",
                    oninput: move |e: FormEvent| name.set(e.value()) }
            }
            div { class: "field",
                Label { html_for: "", "Role" }
                select { class: "native-select", value: "{role}",
                    onchange: move |e: FormEvent| role.set(e.value()),
                    option { value: "read", "read — SELECT + Grafana only" }
                    option { value: "write", "write — insert into existing schema" }
                    option { value: "ddl", "ddl — may create/evolve its scoped tables" }
                }
            }
            div { class: "field",
                Label { html_for: "", "Scope — none selected = every table" }
                div { class: "scope-box",
                    if table_names.is_empty() {
                        span { class: "muted small", "No tables yet — this will be an unscoped key." }
                    }
                    for t in table_names {
                        button {
                            r#type: "button",
                            class: if scope().contains(&t) { "chip-toggle on" } else { "chip-toggle" },
                            onclick: {
                                let t = t.clone();
                                move |_| {
                                    let mut s = scope();
                                    if let Some(i) = s.iter().position(|x| *x == t) { s.remove(i); } else { s.push(t.clone()); }
                                    scope.set(s);
                                }
                            },
                            "{t}"
                        }
                    }
                }
            }
            ErrorNote { error: error() }
            div { class: "modal-actions",
                Button { variant: ButtonVariant::Outline, onclick: move |_| onclose.call(()), "Cancel" }
                Button { disabled: name().is_empty(), onclick: submit, "Create key" }
            }
        }
    }
}

#[component]
fn SecretDialog(secret: String, onclose: EventHandler<()>) -> Element {
    let mut copied = use_signal(|| false);
    let s = secret.clone();
    rsx! {
        Dialog { open: Some(true), on_open_change: move |o: bool| if !o { onclose.call(()) },
            DialogTitle { "Key created — copy the secret now" }
            DialogDescription { "Only its hash is stored — this secret will never be shown again." }
            div { class: "secret-row",
                code { class: "mono secret", "{secret}" }
                Button { variant: ButtonVariant::Outline, size: ButtonSize::Sm,
                    onclick: move |_| {
                        let s = s.clone();
                        spawn(async move {
                            let _ = document::eval(&format!(
                                "navigator.clipboard.writeText({})",
                                serde_json::to_string(&s).unwrap()
                            )).await;
                        });
                        copied.set(true);
                    },
                    if copied() { "✓ copied" } else { "copy" }
                }
            }
            div { class: "modal-actions",
                Button { onclick: move |_| onclose.call(()), "Done" }
            }
        }
    }
}

// --- sql ------------------------------------------------------------------

#[component]
pub fn SqlView() -> Element {
    let mut query = use_signal(|| {
        "SELECT name FROM sqlite_master WHERE type IN ('view','table') ORDER BY 1".to_string()
    });
    let mut result = use_signal(|| None::<SqlResult>);
    let mut error = use_signal(|| None::<String>);
    let mut busy = use_signal(|| false);

    let mut run = move || {
        let q = query();
        busy.set(true);
        spawn(async move {
            match api::sql(&q).await {
                Ok(r) => { error.set(None); result.set(Some(r)); }
                Err(e) => { result.set(None); error.set(Some(e.message)); }
            }
            busy.set(false);
        });
    };

    rsx! {
        Card {
            CardHeader {
                CardTitle { "SQL console" }
                CardDescription { "runs with this token's role — silodb_ts(), silodb_bucket(), rollup views, all of it" }
            }
            CardContent {
                Textarea { class: "mono sql-box", rows: 4,
                    value: "{query}",
                    oninput: move |e: FormEvent| query.set(e.value()),
                    onkeydown: move |e: KeyboardEvent| {
                        if e.key() == Key::Enter && (e.modifiers().ctrl() || e.modifiers().meta()) { run() }
                    },
                }
                div { class: "sql-bar",
                    span { class: "muted small", "⌘⏎ to run · one statement per request" }
                    Button { size: ButtonSize::Sm, disabled: busy(), onclick: move |_| run(), "▶ Run" }
                }
                ErrorNote { error: error() }
                if let Some(r) = result() {
                    ResultTable { r }
                }
            }
        }
    }
}

#[component]
fn ResultTable(r: SqlResult) -> Element {
    if let Some(n) = r.rows_affected {
        return rsx! { p { class: "muted small", "OK — {n} row(s) affected." } };
    }
    if r.rows.is_empty() {
        return rsx! { div { class: "empty", "No rows." } };
    }
    rsx! {
        div { class: "result-wrap",
            div { class: "result-scroll",
                table { class: "table result",
                    thead { tr { for c in &r.columns { th { "{c}" } } } }
                    tbody {
                        for row in &r.rows {
                            tr {
                                for v in row {
                                    if v.is_null() {
                                        td { class: "muted", i { "NULL" } }
                                    } else if let Some(s) = v.as_str() {
                                        td { "{s}" }
                                    } else {
                                        td { "{v}" }
                                    }
                                }
                            }
                        }
                    }
                }
            }
            div { class: "result-foot muted small",
                "{r.rows.len()} row(s)"
                if r.truncated { " — truncated at the server cap" }
            }
        }
    }
}
