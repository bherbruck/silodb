import { useState } from "react";
import { api, type TableInfo } from "../api";
import {
  Badge, Button, Card, CardHeader, Dialog, Empty, ErrorNote, Input, Label, Table,
} from "../components/ui";
import { Plus, Columns3, Clock } from "lucide-react";

export function TablesView({ tables, refresh }: { tables: TableInfo[]; refresh: () => void }) {
  const [createOpen, setCreateOpen] = useState(false);
  const [columnFor, setColumnFor] = useState<string | null>(null);
  const [retentionFor, setRetentionFor] = useState<TableInfo | null>(null);

  return (
    <Card>
      <CardHeader
        title="Tables"
        subtitle="Hypertables under maintenance — hot SQLite tier plus tiered parquet"
        action={
          <Button size="sm" onClick={() => setCreateOpen(true)}>
            <Plus size={14} /> New table
          </Button>
        }
      />
      {tables.length === 0 ? (
        <Empty>No tables yet — create one, or let a ddl key autoschema it via /write.</Empty>
      ) : (
        <Table
          head={
            <>
              <th>Table</th><th>Columns</th><th>Tiers</th><th>Retention</th>
              <th className="text-right">Hot rows</th><th className="text-right">Cold rows</th>
              <th className="text-right">Files</th><th></th>
            </>
          }
        >
          {tables.map((t) => (
            <tr key={t.table}>
              <td className="font-medium">{t.table}</td>
              <td>
                <div className="flex max-w-64 flex-wrap gap-1">
                  {t.columns.map((c) => (
                    <span key={c.name} className="rounded bg-muted px-1.5 py-0.5 font-mono text-[11px]" title={c.type}>
                      {c.name}
                      {c.name === t.ts_column && <span className="text-muted-foreground"> ⏱</span>}
                    </span>
                  ))}
                </div>
              </td>
              <td>
                <div className="flex gap-1">
                  {t.tiers.map((tier) => <Badge key={tier} tone="muted">{tier}</Badge>)}
                </div>
              </td>
              <td>{t.retention ? <Badge tone="muted">{t.retention}</Badge> : <span className="text-xs text-muted-foreground">forever</span>}</td>
              <td className="text-right tabular-nums">{t.hot_rows.toLocaleString()}</td>
              <td className="text-right tabular-nums">{t.cold_rows.toLocaleString()}</td>
              <td className="text-right tabular-nums">{t.active_files}</td>
              <td>
                <div className="flex justify-end gap-1">
                  <Button size="sm" variant="ghost" title="Add column" onClick={() => setColumnFor(t.table)}>
                    <Columns3 size={14} />
                  </Button>
                  <Button size="sm" variant="ghost" title="Retention" onClick={() => setRetentionFor(t)}>
                    <Clock size={14} />
                  </Button>
                </div>
              </td>
            </tr>
          ))}
        </Table>
      )}
      <CreateTableDialog open={createOpen} onClose={() => setCreateOpen(false)} done={refresh} />
      <AddColumnDialog table={columnFor} onClose={() => setColumnFor(null)} done={refresh} />
      <RetentionDialog table={retentionFor} onClose={() => setRetentionFor(null)} done={refresh} />
    </Card>
  );
}

function CreateTableDialog({ open, onClose, done }: { open: boolean; onClose: () => void; done: () => void }) {
  const [name, setName] = useState("");
  const [schema, setSchema] = useState("ts TIMESTAMP, device TEXT, value REAL");
  const [tiers, setTiers] = useState("1d,7d");
  const [retention, setRetention] = useState("");
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);

  const submit = async () => {
    setBusy(true);
    setError(null);
    try {
      await api.createTable({
        name,
        schema,
        tiers: tiers || undefined,
        retention: retention || undefined,
      });
      done();
      onClose();
    } catch (e) {
      setError(String((e as Error).message));
    } finally {
      setBusy(false);
    }
  };

  return (
    <Dialog open={open} onClose={onClose} title="Create table">
      <div className="space-y-3">
        <div>
          <Label>Name</Label>
          <Input value={name} onChange={(e) => setName(e.target.value)} placeholder="readings" />
        </div>
        <div>
          <Label>Schema (one TIMESTAMP column is the bucket axis)</Label>
          <Input value={schema} onChange={(e) => setSchema(e.target.value)} className="font-mono text-xs" />
        </div>
        <div className="grid grid-cols-2 gap-3">
          <div>
            <Label>Tiers</Label>
            <Input value={tiers} onChange={(e) => setTiers(e.target.value)} placeholder="1d,7d,28d" />
          </div>
          <div>
            <Label>Retention (blank = forever)</Label>
            <Input value={retention} onChange={(e) => setRetention(e.target.value)} placeholder="2y" />
          </div>
        </div>
        <ErrorNote error={error} />
        <div className="flex justify-end gap-2 pt-1">
          <Button variant="outline" onClick={onClose}>Cancel</Button>
          <Button onClick={submit} disabled={busy || !name || !schema}>Create</Button>
        </div>
      </div>
    </Dialog>
  );
}

function AddColumnDialog({ table, onClose, done }: { table: string | null; onClose: () => void; done: () => void }) {
  const [coldef, setColdef] = useState("");
  const [error, setError] = useState<string | null>(null);
  const submit = async () => {
    setError(null);
    try {
      await api.addColumn(table!, coldef);
      done();
      onClose();
      setColdef("");
    } catch (e) {
      setError(String((e as Error).message));
    }
  };
  return (
    <Dialog open={table !== null} onClose={onClose} title={`Add column to ${table ?? ""}`}>
      <div className="space-y-3">
        <div>
          <Label>Column definition — instant; existing rows read NULL</Label>
          <Input value={coldef} onChange={(e) => setColdef(e.target.value)} placeholder="humidity REAL" className="font-mono text-xs" />
        </div>
        <ErrorNote error={error} />
        <div className="flex justify-end gap-2 pt-1">
          <Button variant="outline" onClick={onClose}>Cancel</Button>
          <Button onClick={submit} disabled={!coldef}>Add column</Button>
        </div>
      </div>
    </Dialog>
  );
}

function RetentionDialog({ table, onClose, done }: { table: TableInfo | null; onClose: () => void; done: () => void }) {
  const [retain, setRetain] = useState("");
  const [error, setError] = useState<string | null>(null);
  const apply = async (value: string | null) => {
    setError(null);
    try {
      await api.setRetention(table!.table, value);
      done();
      onClose();
    } catch (e) {
      setError(String((e as Error).message));
    }
  };
  return (
    <Dialog open={table !== null} onClose={onClose} title={`Retention for ${table?.table ?? ""}`}>
      <div className="space-y-3">
        <p className="text-xs text-muted-foreground">
          Currently: <b>{table?.retention ?? "keep forever"}</b>. Must be at least the largest
          tier ({table?.tiers.at(-1)}). Files entirely older than the window are deleted by maintenance.
        </p>
        <div>
          <Label>New retention</Label>
          <Input value={retain} onChange={(e) => setRetain(e.target.value)} placeholder="8w" />
        </div>
        <ErrorNote error={error} />
        <div className="flex justify-between gap-2 pt-1">
          <Button variant="destructive" onClick={() => apply(null)}>Keep forever</Button>
          <div className="flex gap-2">
            <Button variant="outline" onClick={onClose}>Cancel</Button>
            <Button onClick={() => apply(retain)} disabled={!retain}>Apply</Button>
          </div>
        </div>
      </div>
    </Dialog>
  );
}
