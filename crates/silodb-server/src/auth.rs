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

    /// Resolve the request's role from its `Authorization: Bearer` header.
    pub fn role(&self, headers: &HeaderMap) -> Option<Role> {
        let presented = headers
            .get("authorization")?
            .to_str()
            .ok()?
            .strip_prefix("Bearer ")?
            .trim();
        for (tok, role) in [
            (&self.ddl, Role::Ddl),
            (&self.readwrite, Role::ReadWrite),
            (&self.readonly, Role::ReadOnly),
        ] {
            if let Some(t) = tok
                && ct_eq(t, presented)
            {
                return Some(role);
            }
        }
        None
    }
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
