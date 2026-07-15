import { useState } from "react";
import { api, type SqlResult } from "../api";
import { Button, Card, CardHeader, Empty, ErrorNote } from "../components/ui";
import { Play } from "lucide-react";

export function SqlView() {
  const [sql, setSql] = useState("SELECT name FROM sqlite_master WHERE type IN ('view','table') ORDER BY 1");
  const [result, setResult] = useState<SqlResult | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [busy, setBusy] = useState(false);
  const [ms, setMs] = useState<number | null>(null);

  const run = async () => {
    setBusy(true);
    setError(null);
    const t0 = performance.now();
    try {
      setResult(await api.sql(sql));
      setMs(performance.now() - t0);
    } catch (e) {
      setResult(null);
      setError(String((e as Error).message));
    } finally {
      setBusy(false);
    }
  };

  return (
    <Card>
      <CardHeader
        title="SQL console"
        subtitle="Runs with this token's role — silodb_ts(), silodb_bucket(), rollup views, all of it"
      />
      <div className="space-y-3 p-5">
        <textarea
          value={sql}
          onChange={(e) => setSql(e.target.value)}
          onKeyDown={(e) => {
            if ((e.metaKey || e.ctrlKey) && e.key === "Enter") run();
          }}
          rows={4}
          spellCheck={false}
          className="w-full resize-y rounded-md border border-border bg-transparent p-3 font-mono text-xs focus-visible:outline-2 focus-visible:outline-ring"
        />
        <div className="flex items-center justify-between">
          <span className="text-xs text-muted-foreground">⌘⏎ to run · one statement per request</span>
          <Button size="sm" onClick={run} disabled={busy}>
            <Play size={14} /> Run
          </Button>
        </div>
        <ErrorNote error={error} />
        {result && <ResultTable result={result} ms={ms} />}
      </div>
    </Card>
  );
}

function ResultTable({ result, ms }: { result: SqlResult; ms: number | null }) {
  if (result.rows_affected !== undefined) {
    return (
      <p className="text-xs text-muted-foreground">
        OK — {result.rows_affected} row(s) affected{ms !== null && ` in ${ms.toFixed(0)}ms`}.
      </p>
    );
  }
  const { columns = [], rows = [], truncated } = result;
  if (rows.length === 0) return <Empty>No rows.</Empty>;
  return (
    <div className="overflow-hidden rounded-md border border-border">
      <div className="max-h-96 overflow-auto">
        <table className="w-full text-xs">
          <thead className="sticky top-0 bg-muted">
            <tr className="[&>th]:px-3 [&>th]:py-2 [&>th]:text-left [&>th]:font-medium">
              {columns.map((c) => <th key={c}>{c}</th>)}
            </tr>
          </thead>
          <tbody className="font-mono [&>tr]:border-t [&>tr]:border-border [&>tr>td]:px-3 [&>tr>td]:py-1.5">
            {rows.map((r, i) => (
              <tr key={i}>
                {r.map((v, j) => (
                  <td key={j} className={v === null ? "text-muted-foreground italic" : ""}>
                    {v === null ? "NULL" : typeof v === "object" ? JSON.stringify(v) : String(v)}
                  </td>
                ))}
              </tr>
            ))}
          </tbody>
        </table>
      </div>
      <div className="border-t border-border bg-muted px-3 py-1.5 text-[11px] text-muted-foreground">
        {rows.length} row(s){truncated && " — truncated at the server cap"}{ms !== null && ` · ${ms.toFixed(0)}ms`}
      </div>
    </div>
  );
}
