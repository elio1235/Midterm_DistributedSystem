import { useEffect, useState } from "react";
import { api, downloadUrl, encodeKey } from "../api.js";
import { formatBytes, formatDate, shortHash } from "../util.js";

export default function ObjectDetails({ bucket, objectKey, onClose }) {
  const [meta, setMeta] = useState(null);
  const [error, setError] = useState("");

  useEffect(() => {
    api(`/v1/buckets/${bucket}/metadata/${encodeKey(objectKey)}`).then(setMeta).catch((e) => setError(e.message));
  }, [bucket, objectKey]);

  return (
    <div className="modal-back" onClick={onClose}>
      <div className="modal" onClick={(e) => e.stopPropagation()}>
        <div className="row between">
          <h2>{objectKey}</h2>
          <button className="ghost" onClick={onClose}>
            Close
          </button>
        </div>
        {error && <div className="error">{error}</div>}
        {!meta && !error && <div className="muted">Loading…</div>}
        {meta && (
          <>
            <dl className="facts">
              <div><dt>Bucket</dt><dd>{meta.bucket}</dd></div>
              <div><dt>Size</dt><dd>{formatBytes(meta.size)} ({meta.size.toLocaleString()} bytes)</dd></div>
              <div><dt>Version</dt><dd>{meta.version}</dd></div>
              <div><dt>Type</dt><dd>{meta.content_type}</dd></div>
              <div><dt>Created</dt><dd>{formatDate(meta.created_at)}</dd></div>
              <div><dt>ETag</dt><dd className="mono">{meta.etag}</dd></div>
              <div>
                <dt>Redundancy</dt>
                <dd>
                  {meta.redundancy.scheme === "erasure"
                    ? `Erasure coding ${meta.redundancy.k}+${meta.redundancy.m}`
                    : `Replication ×${meta.redundancy.want} (W=${meta.redundancy.write_quorum}, R=${meta.redundancy.read_quorum})`}
                </dd>
              </div>
              <div>
                <dt>Health</dt>
                <dd><span className={`badge ${meta.health}`}>{meta.health}</span></dd>
              </div>
              <div>
                <dt>Tags</dt>
                <dd>
                  {Object.keys(meta.tags).length === 0 && <span className="muted">none</span>}
                  {Object.entries(meta.tags).map(([k, v]) => (
                    <span key={k} className="chip">{k}: {v}</span>
                  ))}
                </dd>
              </div>
            </dl>
            <a className="button primary" href={downloadUrl(bucket, objectKey, meta.version)}>
              Download
            </a>
            <h3>Where the chunks are stored</h3>
            <table>
              <thead>
                <tr><th>#</th><th>Chunk hash</th><th className="num">Size</th><th>State</th><th>Pieces (node, shard number for erasure coding)</th></tr>
              </thead>
              <tbody>
                {meta.chunks.map((c) => (
                  <tr key={c.index}>
                    <td>{c.index + 1}</td>
                    <td className="mono">{shortHash(c.hash)}…</td>
                    <td className="num">{formatBytes(c.size)}</td>
                    <td>
                      <span className={`badge ${c.state}`}>{c.state}</span> <span className="muted small">{c.pieces_alive}/{c.pieces_wanted}</span>
                    </td>
                    <td>
                      {c.pieces.map((p, i) => (
                        <span key={i} className={`node-chip ${p.status}`}>
                          {p.node_id}
                          {p.shard !== undefined ? ` · #${p.shard}` : ""}
                        </span>
                      ))}
                    </td>
                  </tr>
                ))}
              </tbody>
            </table>
          </>
        )}
      </div>
    </div>
  );
}
