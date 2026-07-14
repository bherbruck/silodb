//! Bearer-token auth: three static tokens from the environment, one per
//! role. A role with no configured token is disabled. Roles are enforced
//! twice — at the route (this module) and at the database (read-only
//! connections for `ReadOnly`, a SQLite authorizer for `ReadWrite`), so
//! clever SQL can't out-privilege its token.

use axum::http::HeaderMap;

#[derive(Clone, Copy, Debug, PartialEq, Eq, PartialOrd, Ord)]
pub enum Role {
    ReadOnly,
    ReadWrite,
    Ddl,
}

#[derive(Clone, Default)]
pub struct Tokens {
    pub readonly: Option<String>,
    pub readwrite: Option<String>,
    pub ddl: Option<String>,
}

impl Tokens {
    pub fn from_env() -> Tokens {
        let get = |k: &str| std::env::var(k).ok().filter(|v| !v.is_empty());
        Tokens {
            readonly: get("SILODB_READONLY_TOKEN"),
            readwrite: get("SILODB_READWRITE_TOKEN"),
            ddl: get("SILODB_DDL_TOKEN"),
        }
    }

    pub fn any_configured(&self) -> bool {
        self.readonly.is_some() || self.readwrite.is_some() || self.ddl.is_some()
    }

    /// Resolve the request's role from its `Authorization` header —
    /// `Bearer <token>`, or `Basic` where the token rides in the password
    /// slot (how Grafana's InfluxDB datasource sends credentials; the
    /// username is ignored).
    pub fn role(&self, headers: &HeaderMap) -> Option<Role> {
        let auth = headers.get("authorization")?.to_str().ok()?;
        if let Some(bearer) = auth.strip_prefix("Bearer ") {
            return self.role_for_secret(bearer.trim());
        }
        if let Some(b64) = auth.strip_prefix("Basic ") {
            let decoded = base64_decode(b64.trim())?;
            let creds = String::from_utf8(decoded).ok()?;
            let secret = creds.split_once(':').map(|(_, p)| p).unwrap_or(&creds);
            return self.role_for_secret(secret);
        }
        None
    }

    /// Match a bare secret (query-param `p=`, basic-auth password) to a
    /// role, highest privilege first.
    pub fn role_for_secret(&self, secret: &str) -> Option<Role> {
        for (tok, role) in [
            (&self.ddl, Role::Ddl),
            (&self.readwrite, Role::ReadWrite),
            (&self.readonly, Role::ReadOnly),
        ] {
            if let Some(t) = tok
                && ct_eq(t, secret)
            {
                return Some(role);
            }
        }
        None
    }
}

/// Minimal base64 (standard alphabet, `=` padding) — one header field
/// isn't worth a dependency.
fn base64_decode(s: &str) -> Option<Vec<u8>> {
    const ALPHA: &[u8] = b"ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";
    let mut out = Vec::with_capacity(s.len() * 3 / 4);
    let mut acc: u32 = 0;
    let mut bits = 0;
    for c in s.bytes() {
        if c == b'=' {
            break;
        }
        let v = ALPHA.iter().position(|&a| a == c)? as u32;
        acc = (acc << 6) | v;
        bits += 6;
        if bits >= 8 {
            bits -= 8;
            out.push((acc >> bits) as u8);
        }
    }
    Some(out)
}

/// Constant-time comparison — token checks must not leak length-of-match
/// through timing.
fn ct_eq(a: &str, b: &str) -> bool {
    let (a, b) = (a.as_bytes(), b.as_bytes());
    if a.len() != b.len() {
        return false;
    }
    a.iter().zip(b).fold(0u8, |acc, (x, y)| acc | (x ^ y)) == 0
}

#[cfg(test)]
mod tests {
    use super::*;

    #[test]
    fn bearer_resolves_highest_matching_role() {
        let tokens = Tokens {
            readonly: Some("r".into()),
            readwrite: Some("w".into()),
            ddl: Some("d".into()),
        };
        let mut h = HeaderMap::new();
        h.insert("authorization", "Bearer w".parse().unwrap());
        assert_eq!(tokens.role(&h), Some(Role::ReadWrite));
        h.insert("authorization", "Bearer nope".parse().unwrap());
        assert_eq!(tokens.role(&h), None);
        h.remove("authorization");
        assert_eq!(tokens.role(&h), None);
    }
}
