import { useCallback, useEffect, useState } from "react";
import { api } from "../api.js";
import { formatBytes, formatDate } from "../util.js";

export default function Cluster({ user }) {
  const [status, setStatus] = useState(null);
  const [error, setError] = useState("");
  const [message, setMessage] = useState("");

  const load = useCallback(async () => {
    try {
      setStatus(await api("/v1/cluster/status"));
      setError("");
    } catch (e) {
      setError(e.message);
    }
  }, []);

  useEffect(() => {
    load();
    const timer = setInterval(load, 2000); // the page refreshes itself, so the repair progress can be watched live
    return () => clearInterval(timer);
  }, [load]);

  async function action(path, text) {
    try {
      await api(path, { method: "POST" });
      setMessage(text);
      load();
    } catch (e) {
      setError(e.message);
    }
  }

  if (!status) return <div className="card muted">{error || "Loading…"}</div>;
  const { nodes, chunks, degraded_objects: degraded, repair } = status;
  const maxChunks = Math.max(1, ...nodes.map((n) => n.chunks));
  const maxBytes = Math.max(1, ...nodes.map((n) => n.stored_bytes));
  const job = repair.running;

  return (
    <>
      {error && <div className="error">{error}</div>}
      <div className="cards">
        <Stat label="Nodes alive" value={`${status.nodes_alive} / ${status.nodes_total}`} tone={status.nodes_alive === status.nodes_total ? "good" : "bad"} />
        <Stat label="Free space" value={formatBytes(status.free_bytes_total)} note="sum over the alive nodes" />
        <Stat label="Unique chunks" value={chunks.unique.toLocaleString()} note={formatBytes(chunks.unique_bytes)} />
        <Stat label="Degraded chunks" value={chunks.degraded} tone={chunks.degraded ? "warn" : "good"} note={chunks.unavailable ? `${chunks.unavailable} unreadable` : "all readable"} />
        <Stat label="Degraded objects" value={degraded.count} tone={degraded.count ? "warn" : "good"} note={degraded.unavailable ? `${degraded.unavailable} unreadable` : "all readable"} />
        <Stat label="Search engine" value={status.search.engine === "elasticsearch" ? "Elasticsearch" : "PostgreSQL"} note={status.search.documents != null ? `${status.search.documents} documents` : "fallback mode"} tone={status.search.engine === "elasticsearch" ? "good" : "warn"} />
      </div>

      <div className="card">
        <h3>Nodes and chunk distribution</h3>
        <div className="nodes">
          {nodes.map((n) => (
            <div key={n.node_id} className={`node ${n.status}`}>
              <div className="row between">
                <strong>{n.node_id}</strong>
                <span className={`badge ${n.status === "alive" ? "healthy" : "unavailable"}`}>{n.status === "alive" ? "alive" : "FAILED"}</span>
              </div>
              <div className="muted small">{n.status === "alive" ? `heartbeat ${n.seconds_since_heartbeat}s ago` : `failed ${n.failed_at ? formatDate(n.failed_at) : `${n.seconds_since_heartbeat}s without heartbeat`}`}</div>
              <Meter label={`${n.chunks} chunks / shards`} value={n.chunks} max={maxChunks} tone={n.status} />
              <Meter label={`${formatBytes(n.stored_bytes)} stored`} value={n.stored_bytes} max={maxBytes} tone={n.status} />
              <div className="muted small">{formatBytes(n.free_bytes)} free</div>
            </div>
          ))}
        </div>
      </div>

      <div className="card">
        <div className="row between">
          <h3>Repair progress</h3>
          {user.role === "admin" && (
            <div className="row">
              <button className="ghost" onClick={() => action("/v1/cluster/repair/run", "Repair requested. A job appears in the table only if some chunk needs repair.")}>Run repair now</button>
              <button className="ghost" onClick={() => action("/v1/cluster/scrub/run", "Scrub run started.")}>Run scrubber now</button>
            </div>
          )}
        </div>
        {message && <div className="muted small">{message}</div>}
        {job ? (
          <div>
            <div className="row between">
              <span><span className="badge degraded">{job.status}</span> job #{job.id}: {job.reason}</span>
              <span className="muted">{job.chunks_done} / {job.chunks_total} chunks</span>
            </div>
            <div className="bar big"><div className="fill" style={{ width: `${job.chunks_total ? (100 * job.chunks_done) / job.chunks_total : 5}%` }} /></div>
          </div>
        ) : (
          <div className="muted">No repair job is running. {chunks.degraded ? "Some chunks are degraded: the next repair job will rebuild them." : "Every chunk has all its copies."}</div>
        )}
        <table>
          <thead>
            <tr><th>Job</th><th>Reason</th><th>Status</th><th className="num">Chunks</th><th className="num">Rebuilt pieces</th><th className="num">Read</th><th className="num">Written</th><th className="num">Seconds</th></tr>
          </thead>
          <tbody>
            {repair.recent_jobs.map((j) => (
              <tr key={j.id}>
                <td>#{j.id}</td>
                <td>{j.reason}{j.node_id ? ` (${j.node_id})` : ""}</td>
                <td><span className={`badge ${j.status === "done" ? "healthy" : j.status === "failed" ? "unavailable" : "degraded"}`}>{j.status}</span></td>
                <td className="num">{j.chunks_repaired}/{j.chunks_total}</td>
                <td className="num">{j.copies_rebuilt}</td>
                <td className="num">{formatBytes(j.bytes_read)}</td>
                <td className="num">{formatBytes(j.bytes_written)}</td>
                <td className="num">{j.seconds ?? "-"}</td>
              </tr>
            ))}
            {repair.recent_jobs.length === 0 && <tr><td colSpan="8" className="muted center-text">No repair jobs yet.</td></tr>}
          </tbody>
        </table>
        {repair.last_scrub && (
          <div className="muted small">
            Last scrub run #{repair.last_scrub.id} ({repair.last_scrub.trigger}): {repair.last_scrub.copies_checked} copies checked, {repair.last_scrub.corrupt_found} corrupted, {repair.last_scrub.missing_found} missing, {repair.last_scrub.repaired} repaired.
          </div>
        )}
      </div>

      <div className="card">
        <h3>Degraded objects</h3>
        {degraded.count === 0 ? (
          <div className="muted">All objects have all their copies or shards.</div>
        ) : (
          <table>
            <thead><tr><th>Bucket</th><th>Key</th><th>Version</th><th className="num">Size</th><th className="num">Degraded chunks</th><th>Readable?</th></tr></thead>
            <tbody>
              {degraded.items.map((o) => (
                <tr key={`${o.bucket}/${o.key}/${o.version}`}>
                  <td>{o.bucket}</td><td className="key">{o.key}</td><td>v{o.version}</td>
                  <td className="num">{formatBytes(o.size)}</td><td className="num">{o.degraded_chunks}</td>
                  <td><span className={`badge ${o.unavailable ? "unavailable" : "degraded"}`}>{o.unavailable ? "NO" : "yes, degraded read"}</span></td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
      </div>
    </>
  );
}

function Stat({ label, value, note, tone }) {
  return (
    <div className={`stat ${tone || ""}`}>
      <div className="muted small">{label}</div>
      <div className="value">{value}</div>
      {note && <div className="muted small">{note}</div>}
    </div>
  );
}

function Meter({ label, value, max, tone }) {
  return (
    <div className="meter">
      <div className="small">{label}</div>
      <div className="bar"><div className={tone === "alive" ? "fill" : "fill bad"} style={{ width: `${(100 * value) / max}%` }} /></div>
    </div>
  );
}
