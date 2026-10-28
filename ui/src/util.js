export function formatBytes(n) {
  if (n === null || n === undefined) return "-";
  if (n < 1024) return `${n} B`;
  const units = ["KB", "MB", "GB", "TB"];
  let value = n / 1024;
  let i = 0;
  while (value >= 1024 && i < units.length - 1) {
    value /= 1024;
    i++;
  }
  return `${value.toFixed(value < 10 ? 2 : 1)} ${units[i]}`;
}

export function formatDate(iso) {
  if (!iso) return "-";
  const d = new Date(iso);
  return isNaN(d) ? iso : d.toLocaleString();
}

// "project=alpha, env=test"  ->  { project: "alpha", env: "test" }
export function parseTags(text) {
  const tags = {};
  text
    .split(",")
    .map((p) => p.trim())
    .filter(Boolean)
    .forEach((pair) => {
      const [name, ...rest] = pair.split("=");
      if (!name.trim() || !rest.length) throw new Error(`Tag "${pair}" must look like name=value`);
      if (!/^[A-Za-z0-9][A-Za-z0-9_.-]{0,63}$/.test(name.trim())) throw new Error(`Tag name "${name.trim()}" is not valid (letters, digits, . _ -)`);
      tags[name.trim()] = rest.join("=").trim();
    });
  return tags;
}

export const shortHash = (h) => h.slice(0, 10);
