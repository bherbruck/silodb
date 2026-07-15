import { useState } from "react";
import { api, type KeyInfo, type TableInfo } from "../api";
import {
  Badge, Button, Card, CardHeader, Dialog, Empty, ErrorNote, Input, Label, Select, Table,
} from "../components/ui";
import { Copy, Check, Plus, Ban } from "lucide-react";

export function KeysView({
  keys, tables, refresh,
}: {
  keys: KeyInfo[];
  tables: TableInfo[];
  refresh: () => void;
}) {
  const [createOpen, setCreateOpen] = useState(false);
  const [secret, setSecret] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);

  const revoke = async (name: string) => {
    if (!confirm(`Revoke key '${name}'? Clients holding it stop working immediately.`)) return;
    setError(null);
    try {
      await api.revokeKey(name);
      refresh();
    } catch (e) {
      setError(String((e as Error).message));
    }
  };

  return (
    <Card>
      <CardHeader
        title="API keys"
        subtitle="Scoped credentials — only the SHA-256 hash is stored; secrets show once"
        action={
          <Button size="sm" onClick={() => setCreateOpen(true)}>
            <Plus size={14} /> New key
          </Button>
        }
      />
      <div className="px-5 pt-3 empty:hidden">
        <ErrorNote error={error} />
      </div>
      {keys.length === 0 ? (
        <Empty>No keys yet. The env tokens are the root credentials; mint scoped keys for clients.</Empty>
      ) : (
        <Table head={<><th>Name</th><th>Role</th><th>Scope</th><th>Created</th><th>Status</th><th></th></>}>
          {keys.map((k) => (
            <tr key={k.name} className={k.revoked ? "opacity-50" : ""}>
              <td className="font-medium">{k.name}</td>
              <td><Badge tone={k.role === "ddl" ? "default" : "muted"}>{k.role}</Badge></td>
              <td>
                {k.scope ? (
                  <div className="flex flex-wrap gap-1">
                    {k.scope.map((t) => <Badge key={t} tone="muted">{t}</Badge>)}
                  </div>
                ) : (
                  <span className="text-xs text-muted-foreground">all tables</span>
                )}
              </td>
              <td className="text-xs text-muted-foreground">
                {new Date(k.created_at / 1000).toLocaleDateString()}
              </td>
              <td>{k.revoked ? <Badge tone="destructive">revoked</Badge> : <Badge tone="muted">active</Badge>}</td>
              <td className="text-right">
                {!k.revoked && (
                  <Button size="sm" variant="ghost" title="Revoke" onClick={() => revoke(k.name)}>
                    <Ban size={14} className="text-destructive" />
                  </Button>
                )}
              </td>
            </tr>
          ))}
        </Table>
      )}
      <CreateKeyDialog
        open={createOpen}
        tables={tables}
        onClose={() => setCreateOpen(false)}
        created={(s) => {
          setSecret(s);
          refresh();
        }}
      />
      <SecretDialog secret={secret} onClose={() => setSecret(null)} />
    </Card>
  );
}

function CreateKeyDialog({
  open, tables, onClose, created,
}: {
  open: boolean;
  tables: TableInfo[];
  onClose: () => void;
  created: (secret: string) => void;
}) {
  const [name, setName] = useState("");
  const [role, setRole] = useState("write");
  const [scope, setScope] = useState<string[]>([]);
  const [error, setError] = useState<string | null>(null);

  const toggle = (t: string) =>
    setScope((s) => (s.includes(t) ? s.filter((x) => x !== t) : [...s, t]));

  const submit = async () => {
    setError(null);
    try {
      const { secret } = await api.createKey({ name, role, scope });
      onClose();
      created(secret);
      setName("");
      setScope([]);
    } catch (e) {
      setError(String((e as Error).message));
    }
  };

  return (
    <Dialog open={open} onClose={onClose} title="Create API key">
      <div className="space-y-3">
        <div>
          <Label>Name</Label>
          <Input value={name} onChange={(e) => setName(e.target.value)} placeholder="site-a" />
        </div>
        <div>
          <Label>Role</Label>
          <Select value={role} onChange={(e) => setRole(e.target.value)}>
            <option value="read">read — SELECT + Grafana only</option>
            <option value="write">write — insert into existing schema</option>
            <option value="ddl">ddl — may create/evolve its scoped tables</option>
          </Select>
        </div>
        <div>
          <Label>Scope — none selected = every table</Label>
          <div className="flex max-h-32 flex-wrap gap-1.5 overflow-y-auto rounded-md border border-border p-2">
            {tables.length === 0 && (
              <span className="text-xs text-muted-foreground">
                No tables yet — an unscoped key, or type scope tables after creating them.
              </span>
            )}
            {tables.map((t) => (
              <button
                key={t.table}
                type="button"
                onClick={() => toggle(t.table)}
                className={
                  "rounded-full border px-2.5 py-1 text-xs transition-colors " +
                  (scope.includes(t.table)
                    ? "border-transparent bg-primary text-primary-foreground"
                    : "border-border hover:bg-accent")
                }
              >
                {t.table}
              </button>
            ))}
          </div>
        </div>
        <ErrorNote error={error} />
        <div className="flex justify-end gap-2 pt-1">
          <Button variant="outline" onClick={onClose}>Cancel</Button>
          <Button onClick={submit} disabled={!name}>Create key</Button>
        </div>
      </div>
    </Dialog>
  );
}

function SecretDialog({ secret, onClose }: { secret: string | null; onClose: () => void }) {
  const [copied, setCopied] = useState(false);
  return (
    <Dialog open={secret !== null} onClose={onClose} title="Key created — copy the secret now">
      <div className="space-y-3">
        <p className="text-xs text-muted-foreground">
          Only its hash is stored — this secret will never be shown again.
        </p>
        <div className="flex items-center gap-2">
          <code className="flex-1 truncate rounded-md bg-muted px-3 py-2 font-mono text-xs">{secret}</code>
          <Button
            size="sm"
            variant="outline"
            onClick={async () => {
              await navigator.clipboard.writeText(secret ?? "");
              setCopied(true);
              setTimeout(() => setCopied(false), 1500);
            }}
          >
            {copied ? <Check size={14} /> : <Copy size={14} />}
          </Button>
        </div>
        <div className="flex justify-end pt-1">
          <Button onClick={onClose}>Done</Button>
        </div>
      </div>
    </Dialog>
  );
}
