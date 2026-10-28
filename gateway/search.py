"""Object search with Elasticsearch (plain REST calls with httpx, no extra library).

One document per object (latest version): bucket, key, size, content type, creation date and the user-defined tags.
If Elasticsearch is not reachable the gateway answers the same search from PostgreSQL instead (see main.py).
"""
import json
import time
from urllib.parse import quote

import httpx

import config

MAPPING = {
    "settings": {"number_of_shards": 1, "number_of_replicas": 0},
    "mappings": {
        "properties": {
            "bucket": {"type": "keyword"},
            "key": {"type": "keyword", "fields": {"text": {"type": "text"}}},   # key.text: searched word by word
            "version": {"type": "integer"},
            "size": {"type": "long"},
            "content_type": {"type": "keyword"},
            "created_at": {"type": "date"},
            "tags": {"type": "flattened"},                                     # every tag name works without a new mapping
        }
    },
}


def doc_id(bucket, key):
    return quote(f"{bucket}/{key}", safe="")


class Search:
    def __init__(self):
        self.enabled = bool(config.ES_URL)
        self.http = httpx.Client(base_url=config.ES_URL or "http://invalid", timeout=httpx.Timeout(10, connect=2))
        self._down_until = 0.0

    # ------------------------------------------------------------ availability
    def available(self):
        return self.enabled and time.monotonic() >= self._down_until

    def _fail(self, error):
        self._down_until = time.monotonic() + 10          # do not wait for a dead Elasticsearch on every request
        print(f"[search] Elasticsearch not available: {error}", flush=True)

    def ensure_index(self):
        """Create the index with its mapping if it does not exist. Returns False when Elasticsearch cannot be reached."""
        if not self.enabled:
            return False
        try:
            if self.http.head(f"/{config.ES_INDEX}").status_code == 404:
                r = self.http.put(f"/{config.ES_INDEX}", json=MAPPING)
                if r.status_code not in (200, 201):
                    raise RuntimeError(r.text)
            self._down_until = 0.0
            return True
        except Exception as e:
            self._fail(e)
            return False

    # ------------------------------------------------------------ indexing (never raises: a failed index must not fail an upload)
    def index(self, doc):
        if not self.available():
            return False
        try:
            r = self.http.put(f"/{config.ES_INDEX}/_doc/{doc_id(doc['bucket'], doc['key'])}", params={"refresh": "true"}, json=doc)
            return r.status_code in (200, 201)
        except Exception as e:
            self._fail(e)
            return False

    def remove(self, bucket, key):
        if not self.available():
            return False
        try:
            r = self.http.delete(f"/{config.ES_INDEX}/_doc/{doc_id(bucket, key)}", params={"refresh": "true"})
            return r.status_code in (200, 404)
        except Exception as e:
            self._fail(e)
            return False

    def bulk_index(self, docs):
        """Index many documents with one request. Returns how many were indexed."""
        if not docs or not self.available():
            return 0
        lines = []
        for d in docs:
            lines.append('{"index":{"_index":"%s","_id":%s}}' % (config.ES_INDEX, _json(f"{d['bucket']}/{d['key']}")))
            lines.append(_json(d))
        try:
            r = self.http.post("/_bulk", params={"refresh": "true"}, content="\n".join(lines) + "\n",
                               headers={"Content-Type": "application/x-ndjson"})
            r.raise_for_status()
            return sum(1 for it in r.json()["items"] if it["index"]["status"] in (200, 201))
        except Exception as e:
            self._fail(e)
            return 0

    # ------------------------------------------------------------ searching
    def build_query(self, bucket=None, prefix=None, q=None, min_size=None, max_size=None,
                    created_after=None, created_before=None, tags=None):
        flt = []
        if bucket:
            flt.append({"term": {"bucket": bucket}})
        if prefix:
            flt.append({"prefix": {"key": prefix}})
        if min_size is not None or max_size is not None:
            flt.append({"range": {"size": {k: v for k, v in (("gte", min_size), ("lte", max_size)) if v is not None}}})
        if created_after or created_before:
            flt.append({"range": {"created_at": {k: v for k, v in (("gte", created_after), ("lte", created_before)) if v}}})
        for name, value in (tags or {}).items():
            flt.append({"term": {f"tags.{name}": value}})
        query = {"bool": {"filter": flt}}
        if q:
            query["bool"]["must"] = [{"bool": {"should": [
                {"match": {"key.text": {"query": q}}},                                        # whole words
                {"wildcard": {"key": {"value": f"*{q}*", "case_insensitive": True}}},          # part of a word
            ], "minimum_should_match": 1}}]
        return query

    def search(self, sort="relevance", desc=True, limit=50, offset=0, **filters):
        """Returns {'total', 'items'} or None if Elasticsearch is not available."""
        if not self.available():
            return None
        body = {"query": self.build_query(**filters), "from": offset, "size": limit, "track_total_hits": True}
        if sort == "relevance":
            body["sort"] = ["_score", {"created_at": "desc"}]
        else:
            body["sort"] = [{sort: "desc" if desc else "asc"}, {"key": "asc"}]
        try:
            r = self.http.post(f"/{config.ES_INDEX}/_search", json=body)
            if r.status_code == 404:                     # index missing: create it and answer from PostgreSQL this time
                self.ensure_index()
                return None
            r.raise_for_status()
            j = r.json()
            return {"total": j["hits"]["total"]["value"], "items": [h["_source"] for h in j["hits"]["hits"]]}
        except Exception as e:
            self._fail(e)
            return None

    def count(self):
        if not self.available():
            return None
        try:
            return self.http.get(f"/{config.ES_INDEX}/_count").json()["count"]
        except Exception as e:
            self._fail(e)
            return None


def _json(value):
    return json.dumps(value, default=str)
