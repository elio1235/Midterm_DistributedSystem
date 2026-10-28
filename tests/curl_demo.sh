#!/bin/bash
# Walk through the whole API with curl (Git Bash, WSL, Linux, macOS).  Usage: bash tests/curl_demo.sh
G=${GATEWAY_URL:-http://localhost:8080}
USER=${ADMIN_USER:-admin}; PASS=${ADMIN_PASSWORD:-admin123}
B=curl-demo-$RANDOM
say() { echo; echo "=== $*"; }

say "1. login (JWT)"
TOKEN=$(curl -s -X POST $G/auth/login -H 'Content-Type: application/json' -d "{\"username\":\"$USER\",\"password\":\"$PASS\"}" | python3 -c 'import sys,json;print(json.load(sys.stdin)["access_token"])')
echo "token: ${TOKEN:0:40}..."
H="Authorization: Bearer $TOKEN"

say "2. request without credentials -> 401"
curl -s -o /dev/null -w "HTTP %{http_code}\n" $G/v1/buckets

say "3. create an erasure-coded bucket $B and list buckets"
curl -s -X PUT $G/v1/buckets/$B -H "$H" -H 'Content-Type: application/json' -d '{"redundancy":"erasure","k":4,"m":2}'; echo
curl -s $G/v1/buckets -H "$H"; echo

say "4. streaming upload of a 9 MB file with a tag"
head -c 9000000 /dev/urandom > /tmp/$B.bin
curl -s -X PUT $G/v1/buckets/$B/objects/reports/2026/big.bin -H "$H" -H 'Content-Type: application/octet-stream' -H 'X-Tag-project: alpha' --data-binary @/tmp/$B.bin; echo

say "5. multipart (form) upload"
echo "hello from a form" > /tmp/$B.txt
curl -s -X POST $G/v1/buckets/$B/upload -H "$H" -F "file=@/tmp/$B.txt" -F "key=forms/hello.txt" -F "tags=project=beta,env=dev"; echo

say "6. metadata only (HEAD)"
curl -sI $G/v1/buckets/$B/objects/reports/2026/big.bin -H "$H" | grep -i -E "HTTP|content-length|etag|accept-ranges"

say "7. byte range: bytes 100-199 -> 206 Partial Content"
curl -s -D - -o /tmp/$B.part $G/v1/buckets/$B/objects/reports/2026/big.bin -H "$H" -H 'Range: bytes=100-199' | grep -i -E "HTTP|content-range|content-length"
if cmp -s <(tail -c +101 /tmp/$B.bin | head -c 100) /tmp/$B.part; then echo "range bytes are identical to the original: OK"; else echo "RANGE MISMATCH"; fi

say "8. full download and compare"
curl -s -o /tmp/$B.down $G/v1/buckets/$B/objects/reports/2026/big.bin -H "$H"
cmp /tmp/$B.bin /tmp/$B.down && echo "downloaded file is identical: OK"

say "9. list objects with a prefix"
curl -s "$G/v1/buckets/$B/objects?prefix=reports/" -H "$H"; echo

say "10. search: key part + tag + minimum size"
curl -s "$G/v1/search?bucket=$B&q=big&tag=project:alpha&min_size=1000000" -H "$H"; echo

say "11. object metadata (chunks and where they are stored)"
curl -s $G/v1/buckets/$B/metadata/reports/2026/big.bin -H "$H" | python3 -c 'import sys,json;d=json.load(sys.stdin);print({k:d[k] for k in ("size","etag","health","tags")});[print(" chunk",c["index"],c["state"],[p["node_id"] for p in c["pieces"]]) for c in d["chunks"]]'

say "12. cluster status"
curl -s $G/v1/cluster/status -H "$H" | python3 -c 'import sys,json;d=json.load(sys.stdin);print("nodes alive",d["nodes_alive"],"/",d["nodes_total"],"| free bytes",d["free_bytes_total"],"| degraded objects",d["degraded_objects"]["count"]);[print(" ",n["node_id"],n["status"],"chunks",n["chunks"],"bytes",n["stored_bytes"]) for n in d["nodes"]]'

say "13. API key"
KEY=$(curl -s -X POST $G/v1/keys -H "$H" -H 'Content-Type: application/json' -d '{"name":"demo"}' | python3 -c 'import sys,json;print(json.load(sys.stdin)["api_key"])')
echo "key: ${KEY:0:12}..."
curl -s $G/v1/me -H "X-API-Key: $KEY"; echo

say "14. clean up"
curl -s -X DELETE $G/v1/buckets/$B/objects/reports/2026/big.bin -H "$H"; echo
curl -s -X DELETE $G/v1/buckets/$B/objects/forms/hello.txt -H "$H"; echo
curl -s -X DELETE $G/v1/buckets/$B -H "$H"; echo
rm -f /tmp/$B.*
