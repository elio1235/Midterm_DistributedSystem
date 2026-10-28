import { useCallback, useEffect, useRef, useState } from "react";
import { api, downloadUrl, uploadFile } from "../api.js";
import { formatBytes, formatDate, parseTags } from "../util.js";
import ObjectDetails from "./ObjectDetails.jsx";

const PAGE = 50;

export default function Files() {
  const [buckets, setBuckets] = useState([]);
  const [bucket, setBucket] = useState(null);
  const [error, setError] = useState("");

  const loadBuckets = useCallback(async () => {
    try {
      const list = await api("/v1/buckets");
      const infos = await Promise.all(list.map((b) => api(`/v1/buckets/${b.name}`).catch(() => b)));
      setBuckets(infos);
      setBucket((current) => current || (infos[0] ? infos[0].name : null));
    } catch (e) {
      setError(e.message);
    }
  }, []);

  useEffect(() => {
    loadBuckets();
  }, [loadBuckets]);

  const current = buckets.find((b) => b.name === bucket); // undefined until the list has been loaded

  return (
    <div className="split">
      <aside>
        <BucketList buckets={buckets} selected={bucket} onSelect={setBucket} onChanged={loadBuckets} />
      </aside>
      <section>
        {error && <div className="error">{error}</div>}
        {current ? (
          <Browser key={current.name} bucket={current} onChanged={loadBuckets} onBucketDeleted={() => { setBucket(null); loadBuckets(); }} />
        ) : (
          <div className="card muted">Create a bucket to start.</div>
        )}
      </section>
    </div>
  );
}

function BucketList({ buckets, selected, onSelect, onChanged }) {
  const [name, setName] = useState("");
  const [kind, setKind] = useState("replication");
  const [error, setError] = useState("");

  async function create(e) {
    e.preventDefault();
    setError("");
    try {
      await api(`/v1/buckets/${name}`, { method: "PUT", body: kind === "erasure" ? { redundancy: "erasure", k: 4, m: 2 } : { redundancy: "replication" } });
      setName("");
      onSelect(name);
      onChanged();
    } catch (err) {
      setError(err.message);
    }
  }

  return (
    <div className="card">
      <h3>Buckets</h3>
      <ul className="bucket-list">
        {buckets.map((b) => (
          <li key={b.name} className={b.name === selected ? "selected" : ""} onClick={() => onSelect(b.name)}>
            <div>
              <strong>{b.name}</strong>
              <div className="muted small">
                {b.objects ?? 0} objects · {formatBytes(b.bytes ?? 0)}
              </div>
            </div>
            <span className={`badge ${b.scheme}`}>{b.scheme === "erasure" ? `EC ${b.k}+${b.m}` : `×${b.want} replicas`}</span>
          </li>
        ))}
        {buckets.length === 0 && <li className="muted">No buckets yet.</li>}
      </ul>
      <form onSubmit={create} className="stack">
        <input placeholder="new-bucket-name" value={name} onChange={(e) => setName(e.target.value)} />
        <select value={kind} onChange={(e) => setKind(e.target.value)}>
          <option value="replication">Replication (3 copies, W=2, R=2)</option>
          <option value="erasure">Erasure coding 4+2 (1.5× space)</option>
        </select>
        <button className="primary" disabled={!name}>
          Create bucket
        </button>
        {error && <div className="error">{error}</div>}
      </form>
    </div>
  );
}

function Browser({ bucket, onChanged, onBucketDeleted }) {
  const [prefix, setPrefix] = useState("");
  const [page, setPage] = useState(0);
  const [data, setData] = useState({ objects: [], total: 0 });
  const [error, setError] = useState("");
  const [details, setDetails] = useState(null);

  const load = useCallback(async () => {
    try {
      setData(await api(`/v1/buckets/${bucket.name}/objects`, { params: { prefix, limit: PAGE, offset: page * PAGE } }));
      setError("");
    } catch (e) {
      setError(e.message);
    }
  }, [bucket.name, prefix, page]);

  useEffect(() => {
    load();
  }, [load]);

  async function remove(o) {
    if (!window.confirm(`Delete ${o.key} (all versions)?`)) return;
    try {
      await api(`/v1/buckets/${bucket.name}/objects/${o.key.split("/").map(encodeURIComponent).join("/")}`, { method: "DELETE" });
      load();
      onChanged();
    } catch (e) {
      setError(e.message);
    }
  }

  async function deleteBucket() {
    if (!window.confirm(`Delete the empty bucket ${bucket.name}?`)) return;
    try {
      await api(`/v1/buckets/${bucket.name}`, { method: "DELETE" });
      onBucketDeleted();
    } catch (e) {
      setError(e.message);
    }
  }

  const pages = Math.max(1, Math.ceil(data.total / PAGE));

  return (
    <>
      <Uploader bucket={bucket.name} onDone={() => { load(); onChanged(); }} />
      <div className="card">
        <div className="row between">
          <h3>
            {bucket.name} <span className="muted small">({data.total} objects)</span>
          </h3>
          <div className="row">
            <input className="narrow" placeholder="Filter by key prefix, e.g. photos/" value={prefix} onChange={(e) => { setPrefix(e.target.value); setPage(0); }} />
            <button className="ghost" onClick={deleteBucket} title="Only possible when the bucket is empty">
              Delete bucket
            </button>
          </div>
        </div>
        {error && <div className="error">{error}</div>}
        <table>
          <thead>
            <tr>
              <th>Key</th>
              <th className="num">Size</th>
              <th>Type</th>
              <th>Version</th>
              <th>Created</th>
              <th>Tags</th>
              <th />
            </tr>
          </thead>
          <tbody>
            {data.objects.map((o) => (
              <tr key={o.key}>
                <td className="key">
                  <button className="link" onClick={() => setDetails(o)}>
                    {o.key}
                  </button>
                </td>
                <td className="num">{formatBytes(o.size)}</td>
                <td className="muted">{o.content_type}</td>
                <td>v{o.version}</td>
                <td className="muted">{formatDate(o.created_at)}</td>
                <td>
                  {Object.entries(o.tags).map(([k, v]) => (
                    <span key={k} className="chip">
                      {k}: {v}
                    </span>
                  ))}
                </td>
                <td className="actions">
                  <a className="button ghost" href={downloadUrl(bucket.name, o.key)}>
                    Download
                  </a>
                  <button className="ghost" onClick={() => setDetails(o)}>
                    Details
                  </button>
                  <button className="danger" onClick={() => remove(o)}>
                    Delete
                  </button>
                </td>
              </tr>
            ))}
            {data.objects.length === 0 && (
              <tr>
                <td colSpan="7" className="muted center-text">
                  No objects here.
                </td>
              </tr>
            )}
          </tbody>
        </table>
        {pages > 1 && (
          <div className="row between">
            <button className="ghost" disabled={page === 0} onClick={() => setPage(page - 1)}>
              ← Previous
            </button>
            <span className="muted">
              Page {page + 1} of {pages}
            </span>
            <button className="ghost" disabled={page + 1 >= pages} onClick={() => setPage(page + 1)}>
              Next →
            </button>
          </div>
        )}
      </div>
      {details && <ObjectDetails bucket={bucket.name} objectKey={details.key} onClose={() => setDetails(null)} />}
    </>
  );
}

function Uploader({ bucket, onDone }) {
  const [keyPrefix, setKeyPrefix] = useState("");
  const [tagText, setTagText] = useState("");
  const [uploads, setUploads] = useState([]); // { name, progress, state, message }
  const [over, setOver] = useState(false);
  const [error, setError] = useState("");
  const input = useRef(null);

  async function start(files) {
    let tags;
    try {
      tags = parseTags(tagText);
      setError("");
    } catch (e) {
      setError(e.message);
      return;
    }
    for (const file of files) {
      const key = keyPrefix + file.name;
      const update = (patch) => setUploads((list) => list.map((u) => (u.key === key && u.file === file ? { ...u, ...patch } : u)));
      setUploads((list) => [...list, { key, file, name: key, progress: 0, state: "uploading" }]);
      try {
        const res = await uploadFile({ bucket, key, file, tags, onProgress: (p) => update({ progress: p }) });
        update({ progress: 1, state: "done", message: `${res.chunks} chunks, ${res.chunks_deduplicated} already stored` });
        onDone();
      } catch (e) {
        update({ state: "error", message: e.message });
      }
    }
  }

  return (
    <div className="card">
      <h3>Upload to {bucket}</h3>
      <div className="row">
        <input className="narrow" placeholder="Folder prefix (optional), e.g. photos/2026/" value={keyPrefix} onChange={(e) => setKeyPrefix(e.target.value)} />
        <input className="narrow" placeholder="Tags (optional): project=alpha, env=test" value={tagText} onChange={(e) => setTagText(e.target.value)} />
      </div>
      {error && <div className="error">{error}</div>}
      <div
        className={over ? "drop over" : "drop"}
        onDragOver={(e) => { e.preventDefault(); setOver(true); }}
        onDragLeave={() => setOver(false)}
        onDrop={(e) => { e.preventDefault(); setOver(false); start([...e.dataTransfer.files]); }}
        onClick={() => input.current.click()}
      >
        Drop files here or click to choose
        <input ref={input} type="file" multiple hidden onChange={(e) => { start([...e.target.files]); e.target.value = ""; }} />
      </div>
      {uploads.map((u, i) => (
        <div key={i} className="upload">
          <div className="row between">
            <span>{u.name}</span>
            <span className={u.state === "error" ? "bad" : "muted"}>
              {u.state === "uploading" ? `${Math.round(u.progress * 100)} %` : u.state === "done" ? `✓ ${u.message}` : u.message}
            </span>
          </div>
          <div className="bar">
            <div className={u.state === "error" ? "fill bad" : "fill"} style={{ width: `${u.progress * 100}%` }} />
          </div>
        </div>
      ))}
    </div>
  );
}
