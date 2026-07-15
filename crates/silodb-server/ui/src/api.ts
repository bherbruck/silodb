// Thin client for silodb-server's admin + SQL APIs. The bearer token is
// held in localStorage and attached to every call; a 401 clears it and
// bounces back to the login gate.

export type TableInfo = {
  table: string;
  tiers: string[];
  retention: string | null;
  ts_column: string;
  base_dir: string;
  columns: { name: string; type: string }[];
  hot_rows: number;
  active_files: number;
  cold_rows: number;
  cold_range: [number, number] | null;
};

export type KeyInfo = {
  name: string;
  role: string;
  scope: string[] | null;
  created_at: number;
  revoked: boolean;
};

export type SqlResult = {
  columns?: string[];
  rows?: unknown[][];
  truncated?: boolean;
  rows_affected?: number;
};

const TOKEN_KEY = "silodb-admin-token";

export const getToken = () => localStorage.getItem(TOKEN_KEY) ?? "";
export const setToken = (t: string) => localStorage.setItem(TOKEN_KEY, t);
export const clearToken = () => localStorage.removeItem(TOKEN_KEY);

export class ApiError extends Error {
  status: number;
  constructor(status: number, message: string) {
    super(message);
    this.status = status;
  }
}

async function call<T>(method: string, path: string, body?: unknown): Promise<T> {
  const resp = await fetch(path, {
    method,
    headers: {
      authorization: `Bearer ${getToken()}`,
      "content-type": "application/json",
    },
    body: body === undefined ? undefined : JSON.stringify(body),
  });
  const text = await resp.text();
  const json = text ? JSON.parse(text) : {};
  if (!resp.ok) {
    if (resp.status === 401) clearToken();
    throw new ApiError(resp.status, json.error ?? resp.statusText);
  }
  return json as T;
}

export const api = {
  tables: () => call<{ tables: TableInfo[] }>("GET", "/admin/api/tables"),
  createTable: (body: { name: string; schema: string; tiers?: string; retention?: string }) =>
    call("POST", "/admin/api/tables", body),
  addColumn: (table: string, coldef: string) =>
    call("POST", `/admin/api/tables/${encodeURIComponent(table)}/columns`, { coldef }),
  setRetention: (table: string, retain: string | null) =>
    call("PUT", `/admin/api/tables/${encodeURIComponent(table)}/retention`, { retain }),
  keys: () => call<{ keys: KeyInfo[] }>("GET", "/admin/api/keys"),
  createKey: (body: { name: string; role: string; scope: string[] }) =>
    call<{ secret: string }>("POST", "/admin/api/keys", body),
  revokeKey: (name: string) => call("DELETE", `/admin/api/keys/${encodeURIComponent(name)}`),
  sql: (sql: string) => call<SqlResult>("POST", "/sql", { sql }),
};
