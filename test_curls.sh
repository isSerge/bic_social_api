#!/usr/bin/env bash
set -e

BASE=http://localhost:8080

ok()  { echo "OK: $1"; }
fail(){ echo "FAIL: $1"; exit 1; }

check() {
  local label=$1; local body=$2; local expected=$3
  if echo "$body" | grep -q "$expected"; then
    ok "$label"
  else
    echo "  body: $body"
    fail "$label (expected '$expected')"
  fi
}

echo "--- health ---"
check "live"  "$(curl -s $BASE/health/live)"  '"status":"alive"'
check "ready" "$(curl -s $BASE/health/ready)" '"status":"ready"'

echo "--- like (tok_user_1) ---"
R=$(curl -s -X POST $BASE/v1/likes \
  -H "Authorization: Bearer tok_user_1" \
  -H "Content-Type: application/json" \
  -d '{"content_type":"post","content_id":"731b0395-4888-4822-b516-05b4b7bf2089"}')
check "like" "$R" '"liked":true'

echo "--- status (tok_user_1) ---"
R=$(curl -s $BASE/v1/likes/post/731b0395-4888-4822-b516-05b4b7bf2089/status \
  -H "Authorization: Bearer tok_user_1")
check "status" "$R" '"liked":true'

echo "--- count (public GET) ---"
R=$(curl -s $BASE/v1/likes/post/731b0395-4888-4822-b516-05b4b7bf2089/count)
check "count" "$R" '"count":'

echo "--- like bonus_hunter (tok_user_2) ---"
R=$(curl -s -X POST $BASE/v1/likes \
  -H "Authorization: Bearer tok_user_2" \
  -H "Content-Type: application/json" \
  -d '{"content_type":"bonus_hunter","content_id":"c3d4e5f6-a7b8-9012-cdef-123456789012"}')
check "like bonus_hunter" "$R" '"liked":true'

echo "--- top (public GET) ---"
R=$(curl -s "$BASE/v1/likes/top?content_type=post&limit=5")
check "top" "$R" '"items":'

echo "--- user likes (tok_user_1) ---"
R=$(curl -s "$BASE/v1/likes/user?content_type=post" \
  -H "Authorization: Bearer tok_user_1")
check "user likes" "$R" '"items":'

echo "--- batch/counts (public POST, no auth) ---"
R=$(curl -s -X POST $BASE/v1/likes/batch/counts \
  -H "Content-Type: application/json" \
  -d '{"items":[{"content_type":"post","content_id":"731b0395-4888-4822-b516-05b4b7bf2089"}]}')
check "batch/counts" "$R" '"results":'

echo "--- unlike (tok_user_1) ---"
R=$(curl -s -X DELETE "$BASE/v1/likes/post/731b0395-4888-4822-b516-05b4b7bf2089" \
  -H "Authorization: Bearer tok_user_1")
check "unlike" "$R" '"liked":false'

echo ""
echo "All checks passed."
