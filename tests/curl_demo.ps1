# Same walk-through as curl_demo.sh for Windows PowerShell.   Usage:  .\tests\curl_demo.ps1
# It calls curl.exe (the real curl that comes with Windows 10/11), not the PowerShell alias "curl".
$G    = if ($env:GATEWAY_URL) { $env:GATEWAY_URL } else { "http://localhost:8080" }
$user = if ($env:ADMIN_USER) { $env:ADMIN_USER } else { "admin" }
$pass = if ($env:ADMIN_PASSWORD) { $env:ADMIN_PASSWORD } else { "admin123" }
$B    = "curl-demo-" + (Get-Random -Maximum 99999)
$tmp  = $env:TEMP
function Say($t) { Write-Host ""; Write-Host "=== $t" -ForegroundColor Cyan }
# curl.exe and JSON in PowerShell: quotes get lost on the way, so the JSON is written to a file first
function Json($text) { $f = Join-Path $tmp "body.json"; Set-Content -Path $f -Value $text -Encoding ASCII -NoNewline; return "@$f" }

Say "1. login (JWT)"
$login = curl.exe -s -X POST "$G/auth/login" -H "Content-Type: application/json" -d (Json "{`"username`":`"$user`",`"password`":`"$pass`"}") | ConvertFrom-Json
$H = "Authorization: Bearer $($login.access_token)"
Write-Host "token: $($login.access_token.Substring(0,40))..."

Say "2. request without credentials -> 401"
curl.exe -s -o NUL -w "HTTP %{http_code}`n" "$G/v1/buckets"

Say "3. create an erasure-coded bucket $B and list buckets"
curl.exe -s -X PUT "$G/v1/buckets/$B" -H $H -H "Content-Type: application/json" -d (Json '{"redundancy":"erasure","k":4,"m":2}'); Write-Host ""
curl.exe -s "$G/v1/buckets" -H $H; Write-Host ""

Say "4. streaming upload of a 9 MB file with a tag"
$bytes = New-Object byte[] 9000000; (New-Object System.Random).NextBytes($bytes)
[IO.File]::WriteAllBytes("$tmp\$B.bin", $bytes)
curl.exe -s -X PUT "$G/v1/buckets/$B/objects/reports/2026/big.bin" -H $H -H "Content-Type: application/octet-stream" -H "X-Tag-project: alpha" --data-binary "@$tmp\$B.bin"; Write-Host ""

Say "5. multipart (form) upload"
Set-Content -Path "$tmp\$B.txt" -Value "hello from a form"
curl.exe -s -X POST "$G/v1/buckets/$B/upload" -H $H -F "file=@$tmp\$B.txt" -F "key=forms/hello.txt" -F "tags=project=beta,env=dev"; Write-Host ""

Say "6. metadata only (HEAD)"
curl.exe -sI "$G/v1/buckets/$B/objects/reports/2026/big.bin" -H $H

Say "7. byte range: bytes 100-199 -> 206 Partial Content"
curl.exe -s -D - -o "$tmp\$B.part" "$G/v1/buckets/$B/objects/reports/2026/big.bin" -H $H -H "Range: bytes=100-199"
$part = [IO.File]::ReadAllBytes("$tmp\$B.part"); $same = ($part.Length -eq 100)
for ($i = 0; $i -lt $part.Length; $i++) { if ($part[$i] -ne $bytes[100 + $i]) { $same = $false } }
Write-Host "range bytes are identical to the original: $same"

Say "8. full download and compare"
curl.exe -s -o "$tmp\$B.down" "$G/v1/buckets/$B/objects/reports/2026/big.bin" -H $H
Write-Host ("downloaded file is identical: " + ((Get-FileHash "$tmp\$B.bin").Hash -eq (Get-FileHash "$tmp\$B.down").Hash))

Say "9. list objects with a prefix"
curl.exe -s "$G/v1/buckets/$B/objects?prefix=reports/" -H $H; Write-Host ""

Say "10. search: key part + tag + minimum size"
curl.exe -s "$G/v1/search?bucket=$B&q=big&tag=project:alpha&min_size=1000000" -H $H; Write-Host ""

Say "11. object metadata"
curl.exe -s "$G/v1/buckets/$B/metadata/reports/2026/big.bin" -H $H | ConvertFrom-Json | Select-Object size, etag, health, tags | Format-List

Say "12. cluster status"
$s = curl.exe -s "$G/v1/cluster/status" -H $H | ConvertFrom-Json
Write-Host "nodes alive $($s.nodes_alive) / $($s.nodes_total) | free bytes $($s.free_bytes_total) | degraded objects $($s.degraded_objects.count)"
$s.nodes | Format-Table node_id, status, chunks, stored_bytes

Say "13. API key"
$k = curl.exe -s -X POST "$G/v1/keys" -H $H -H "Content-Type: application/json" -d (Json '{"name":"demo"}') | ConvertFrom-Json
Write-Host "key: $($k.api_key.Substring(0,12))..."
curl.exe -s "$G/v1/me" -H "X-API-Key: $($k.api_key)"; Write-Host ""

Say "14. clean up"
curl.exe -s -X DELETE "$G/v1/buckets/$B/objects/reports/2026/big.bin" -H $H; Write-Host ""
curl.exe -s -X DELETE "$G/v1/buckets/$B/objects/forms/hello.txt" -H $H; Write-Host ""
curl.exe -s -X DELETE "$G/v1/buckets/$B" -H $H; Write-Host ""
Remove-Item "$tmp\$B.*" -ErrorAction SilentlyContinue
