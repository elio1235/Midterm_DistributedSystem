import { useEffect, useState } from "react";
import { api, downloadUrl } from "../api.js";
import { formatBytes, formatDate } from "../util.js";
import ObjectDetails from "./ObjectDetails.jsx";

const UNITS = { B: 1, KB: 1024, MB: 1024 ** 2, GB: 1024 ** 3 };

export default function Search() {
  const [buckets, setBuckets] = useState([]);
  const [form, setForm] = useState({ q: "", bucket: "", prefix: "", minSize: "", maxSize: "", unit: "KB", after: "", before: "", tags: "", sort: "" });
  const [result, setResult] = useState(null);
  const [error, setError] = useState("");
  const [details, setDetails] = useState(null);
  const set = (name) => (e) => setForm({ ...form, [name]: e.target.value });

  useEffect(() => {
    api("/v1/buckets").then(setBuckets).catch(() => {});
  }, []);

  async function run(e) {
    e && e.preventDefault();
    setError("");
    try {
      const tag = form.tags
        .split(",")
        .map((t) => t.trim())
        .filter(Boolean)
        .map((t) => t.replace("=", ":"));
      const mul = UNITS[form.unit];
      setResult(
        await api("/v1/search", {
          params: {
            q: form.q,
            bucket: form.bucket,
            prefix: form.prefix,
            min_size: form.minSize !== "" ? Math.round(form.minSize * mul) : "",
            max_size: form.maxSize !== "" ? Math.round(form.maxSize * mul) : "",
            created_after: form.after,
            created_before: form.before ? form.before + "T23:59:59" : "",
            tag,
            sort: form.sort,
            limit: 100,
          },
        })
      );
    } catch (err) {
      setError(err.message);
    }
  }

  return (
    <>
      <form className="card" onSubmit={run}>
        <h3>Search objects</h3>
        <div className="grid">
          <label>Words or part of the name<input value={form.q} onChange={set("q")} placeholder="sales, report, .png …" /></label>
          <label>Bucket
            <select value={form.bucket} onChange={set("bucket")}>
              <option value="">All buckets</option>
              {buckets.map((b) => <option key={b.name}>{b.name}</option>)}
            </select>
          </label>
          <label>Key prefix<input value={form.prefix} onChange={set("prefix")} placeholder="reports/2026/" /></label>
          <label>Tags (all must match)<input value={form.tags} onChange={set("tags")} placeholder="project=alpha, env=test" /></label>
          <label>Size from<input type="number" min="0" value={form.minSize} onChange={set("minSize")} /></label>
          <label>Size to<input type="number" min="0" value={form.maxSize} onChange={set("maxSize")} /></label>
          <label>Size unit
            <select value={form.unit} onChange={set("unit")}>{Object.keys(UNITS).map((u) => <option key={u}>{u}</option>)}</select>
          </label>
          <label>Sort by
            <select value={form.sort} onChange={set("sort")}>
              <option value="">Best match / newest</option>
              <option value="key">Name</option>
              <option value="size">Size</option>
              <option value="created_at">Date</option>
            </select>
          </label>
          <label>Created after<input type="date" value={form.after} onChange={set("after")} /></label>
          <label>Created before<input type="date" value={form.before} onChange={set("before")} /></label>
        </div>
        <div className="row">
          <button className="primary">Search</button>
          <button type="button" className="ghost" onClick={() => setForm({ ...form, q: "", prefix: "", minSize: "", maxSize: "", after: "", before: "", tags: "", bucket: "" })}>
            Clear
          </button>
        </div>
        {error && <div className="error">{error}</div>}
      </form>

      {result && (
        <div className="card">
          <div className="row between">
            <h3>
              {result.total} result{result.total === 1 ? "" : "s"}
            </h3>
            <span className={`badge ${result.engine === "elasticsearch" ? "healthy" : "degraded"}`} title="The engine that answered this search">
              {result.engine === "elasticsearch" ? "Elasticsearch" : "PostgreSQL (Elasticsearch not available)"}
            </span>
          </div>
          <table>
            <thead>
              <tr><th>Bucket</th><th>Key</th><th className="num">Size</th><th>Created</th><th>Tags</th><th /></tr>
            </thead>
            <tbody>
              {result.results.map((o) => (
                <tr key={o.bucket + "/" + o.key}>
                  <td>{o.bucket}</td>
                  <td className="key"><button className="link" onClick={() => setDetails(o)}>{o.key}</button></td>
                  <td className="num">{formatBytes(o.size)}</td>
                  <td className="muted">{formatDate(o.created_at)}</td>
                  <td>{Object.entries(o.tags).map(([k, v]) => <span key={k} className="chip">{k}: {v}</span>)}</td>
                  <td className="actions"><a className="button ghost" href={downloadUrl(o.bucket, o.key)}>Download</a></td>
                </tr>
              ))}
              {result.results.length === 0 && <tr><td colSpan="6" className="muted center-text">Nothing found.</td></tr>}
            </tbody>
          </table>
        </div>
      )}
      {details && <ObjectDetails bucket={details.bucket} objectKey={details.key} onClose={() => setDetails(null)} />}
    </>
  );
}
