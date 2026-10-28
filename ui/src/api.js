// Everything the UI says to the gateway goes through this file.
const BASE = "/api";

let token = sessionStorage.getItem("token") || "";
let onUnauthorized = () => {};

export const setUnauthorizedHandler = (fn) => (onUnauthorized = fn);
export const getToken = () => token;
export function setToken(value) {
  token = value || "";
  if (token) sessionStorage.setItem("token", token);
  else sessionStorage.removeItem("token");
}

// keys can contain "/", so every part is encoded on its own
export const encodeKey = (key) => key.split("/").map(encodeURIComponent).join("/");

function errorText(data, fallback) {
  const detail = data && data.detail;
  if (typeof detail === "string") return detail;
  if (detail) return JSON.stringify(detail);
  return fallback;
}

export async function api(path, { method = "GET", body, params } = {}) {
  const url = new URL(BASE + path, window.location.origin);
  Object.entries(params || {}).forEach(([name, value]) => {
    if (Array.isArray(value)) value.forEach((v) => url.searchParams.append(name, v));
    else if (value !== "" && value !== undefined && value !== null) url.searchParams.set(name, value);
  });
  const headers = {};
  if (token) headers.Authorization = `Bearer ${token}`;
  if (body) headers["Content-Type"] = "application/json";
  const res = await fetch(url, { method, headers, body: body ? JSON.stringify(body) : undefined });
  if (res.status === 401 && token) onUnauthorized();
  let data = null;
  try {
    data = await res.json();
  } catch {
    /* no JSON body */
  }
  if (!res.ok) throw new Error(errorText(data, `${res.status} ${res.statusText}`));
  return data;
}

// A normal link in the browser cannot send the Authorization header, so the link carries the token.
export function downloadUrl(bucket, key, version) {
  const q = new URLSearchParams({ access_token: token, download: "true" });
  if (version) q.set("version", version);
  return `${BASE}/v1/buckets/${encodeURIComponent(bucket)}/objects/${encodeKey(key)}?${q}`;
}

// XMLHttpRequest instead of fetch: it reports the upload progress. The file is sent as the raw body (streaming upload).
export function uploadFile({ bucket, key, file, tags, onProgress }) {
  return new Promise((resolve, reject) => {
    const xhr = new XMLHttpRequest();
    xhr.open("PUT", `${BASE}/v1/buckets/${encodeURIComponent(bucket)}/objects/${encodeKey(key)}`);
    xhr.setRequestHeader("Authorization", `Bearer ${token}`);
    xhr.setRequestHeader("Content-Type", file.type || "application/octet-stream");
    Object.entries(tags).forEach(([name, value]) => xhr.setRequestHeader(`X-Tag-${name}`, value));
    xhr.upload.onprogress = (e) => e.lengthComputable && onProgress(e.loaded / e.total);
    xhr.onload = () => {
      let data = null;
      try {
        data = JSON.parse(xhr.responseText);
      } catch {
        /* no JSON body */
      }
      if (xhr.status === 201) resolve(data);
      else reject(new Error(errorText(data, `upload failed (${xhr.status})`)));
    };
    xhr.onerror = () => reject(new Error("network error during the upload"));
    xhr.send(file);
  });
}
