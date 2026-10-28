import { useCallback, useEffect, useState } from "react";
import { api, getToken, setToken, setUnauthorizedHandler } from "./api.js";
import Login from "./components/Login.jsx";
import Files from "./components/Files.jsx";
import Search from "./components/Search.jsx";
import Cluster from "./components/Cluster.jsx";

const TABS = [
  { id: "files", label: "Files" },
  { id: "search", label: "Search" },
  { id: "cluster", label: "Cluster" },
];

export default function App() {
  const [user, setUser] = useState(null); // { username, role }
  const [checking, setChecking] = useState(Boolean(getToken()));
  const [tab, setTab] = useState("files");

  const logout = useCallback(() => {
    setToken("");
    setUser(null);
  }, []);

  useEffect(() => {
    setUnauthorizedHandler(logout);
    if (getToken()) {
      api("/v1/me")
        .then(setUser)
        .catch(() => setToken(""))
        .finally(() => setChecking(false));
    }
  }, [logout]);

  if (checking) return <div className="center muted">Loading…</div>;
  if (!user) return <Login onLogin={setUser} />;

  return (
    <div className="app">
      <header className="topbar">
        <div className="brand">
          <span className="logo">◧</span> Object Storage
        </div>
        <nav>
          {TABS.map((t) => (
            <button key={t.id} className={tab === t.id ? "tab active" : "tab"} onClick={() => setTab(t.id)}>
              {t.label}
            </button>
          ))}
        </nav>
        <div className="who">
          <span className="muted">
            {user.username} <span className="badge">{user.role}</span>
          </span>
          <button className="ghost" onClick={logout}>
            Sign out
          </button>
        </div>
      </header>
      <main>
        {tab === "files" && <Files user={user} />}
        {tab === "search" && <Search />}
        {tab === "cluster" && <Cluster user={user} />}
      </main>
    </div>
  );
}
