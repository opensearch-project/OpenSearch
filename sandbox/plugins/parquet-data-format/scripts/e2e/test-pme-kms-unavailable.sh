#!/usr/bin/env bash
#
# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
#

set -uo pipefail

HOST="${1:-http://localhost:9200}"

PASS=0
FAIL=0
ERRORS=()
REST_SEQ=0
exec 3>&2

info()  { echo "[kms-unavail] $*"; }
ok()    { echo "[kms-unavail] ✓ $1"; PASS=$((PASS + 1)); }
fail()  { echo "[kms-unavail] ✗ $1 — $2" >&2; FAIL=$((FAIL + 1)); ERRORS+=("$1: $2"); }

log_rest_block() {
    local id="$1"
    local marker="$2"
    local content="$3"
    if [[ -z "${content}" ]]; then
        printf '[kms-unavail][REST %s] %s <empty>\n' "${id}" "${marker}" >&3
        return
    fi
    if command -v jq &>/dev/null && jq -e . >/dev/null 2>&1 <<< "${content}"; then
        while IFS= read -r line; do
            printf '[kms-unavail][REST %s] %s %s\n' "${id}" "${marker}" "${line}" >&3
        done < <(jq '.' <<< "${content}")
    else
        while IFS= read -r line; do
            printf '[kms-unavail][REST %s] %s %s\n' "${id}" "${marker}" "${line}" >&3
        done <<< "${content}"
    fi
}

os_req() {
    local method="$1"
    local path="$2"
    local body="${3:-}"
    REST_SEQ=$((REST_SEQ + 1))
    local request_id="$$.${REST_SEQ}.${RANDOM}"
    local args=(-s -w "\n%{http_code}" -X "${method}" "${HOST}${path}" -H "Content-Type: application/json")
    if [[ -n "${body}" ]]; then
        args+=(-d "${body}")
    fi
    printf '[kms-unavail][REST %s] > %s %s%s\n' "${request_id}" "${method}" "${HOST}" "${path}" >&3
    if [[ -n "${body}" ]]; then
        log_rest_block "${request_id}" ">" "${body}"
    fi
    local response
    response=$(curl "${args[@]}")
    local status
    status=$(tail -n1 <<< "${response}")
    local response_body
    response_body=$(sed '$d' <<< "${response}")
    printf '[kms-unavail][REST %s] < HTTP %s\n' "${request_id}" "${status}" >&3
    log_rest_block "${request_id}" "<" "${response_body}"
    printf '%s\n' "${response}"
}

http_status() { tail -n1 <<< "$1"; }
http_body()   { sed '$d' <<< "$1"; }

assert_status() {
    local name="$1" response="$2" expected="$3"
    local actual
    actual=$(http_status "${response}")
    if [[ "${actual}" == "${expected}" ]]; then
        ok "${name}"
    else
        fail "${name}" "expected HTTP ${expected}, got HTTP ${actual} — body: $(http_body "${response}")"
    fi
}

# ---- prerequisite check ------------------------------------------------------

info "Checking cluster health at ${HOST} ..."
health_resp=$(os_req GET "/_cluster/health")
if [[ "$(http_status "${health_resp}")" != "200" ]]; then
    echo "[kms-unavail] ERROR: cluster not reachable at ${HOST}" >&2
    exit 1
fi
info "Cluster reachable."

# Verify that mock-pme-kms is NOT installed (it must have been removed for this test).
plugins_resp=$(os_req GET "/_cat/plugins?h=component&format=json")
if jq -r '.[].component' <<< "$(http_body "${plugins_resp}")" 2>/dev/null | grep -q "mock-pme-kms"; then
    echo "[kms-unavail] ERROR: mock-pme-kms is still installed — restart cluster without it first." >&2
    exit 1
fi
info "Confirmed: mock-pme-kms plugin is NOT installed."

# ---- T9-A: Encrypted index must not be searchable without KMS ----------------
#
# When the key provider plugin is missing the search engine cannot decrypt the
# Parquet footer keys.  The query should therefore either:
#   (a) return an HTTP error (5xx), or
#   (b) return HTTP 200 but with 0 hits (e.g. if the shard is in a degraded state
#       and returns empty results rather than an error).
# We accept either outcome as a pass — the important invariant is that the
# plaintext document data is NOT returned.

info "--- T9-A: Encrypted index must not return plaintext docs without KMS ---"

search_resp=$(os_req GET "/pme-t9-persist/_search" '{"query":{"match_all":{}}}')
search_status=$(http_status "${search_resp}")
search_body=$(http_body "${search_resp}")
hit_count=$(jq -r '.hits.total.value // 0' <<< "${search_body}" 2>/dev/null || echo "0")

info "Search status: ${search_status}, hit count: ${hit_count}"

if [[ "${search_status}" != "200" ]]; then
    # Error response — encryption enforcement working correctly.
    ok "T9-A/encrypted-index-error-without-kms"
elif [[ "${hit_count}" == "0" ]]; then
    # Zero hits — data inaccessible without key provider.
    ok "T9-A/encrypted-index-zero-hits-without-kms"
else
    # Plaintext documents returned — this must NOT happen.
    fail "T9-A/encrypted-index-inaccessible-without-kms" \
        "got ${hit_count} hits (status=${search_status}): encrypted data MUST NOT be readable without KMS"
fi

# ---- T9-B: Creating a new encrypted index without KMS must fail --------------
#
# Attempting to create an index with index.store.parquet.crypto.key_provider set
# while the key provider plugin is absent must not produce a usable shard.  The
# current implementation accepts the index metadata but reports
# shards_acknowledged=false because the shard cannot start without the provider.

info "--- T9-B: Creating a new encrypted index without KMS must fail ---"

os_req DELETE "/pme-t9-new-enc" > /dev/null 2>&1 || true
new_enc_resp=$(os_req PUT "/pme-t9-new-enc" '{
  "settings": {
    "index.pluggable.dataformat.enabled": true,
    "index.pluggable.dataformat": "parquet",
    "index.store.parquet.crypto.key_provider": "test",
    "index.store.parquet.crypto.key_provider_type": "mock-pme",
    "number_of_shards": 1,
    "number_of_replicas": 0
  }
}')
new_enc_status=$(http_status "${new_enc_resp}")
new_enc_body=$(http_body "${new_enc_resp}")
if [[ "${new_enc_status}" == "400" || "${new_enc_status}" == "500" ]]; then
    ok "T9-B/new-encrypted-index-rejected-without-kms"
elif [[ "${new_enc_status}" == "200" ]] && [[ "$(jq -r '.shards_acknowledged // false' <<< "${new_enc_body}" 2>/dev/null)" == "false" ]]; then
    ok "T9-B/new-encrypted-index-shard-not-started-without-kms"
else
    fail "T9-B/new-encrypted-index-rejected-without-kms" \
        "expected 4xx/5xx or HTTP 200 with shards_acknowledged=false, got HTTP ${new_enc_status}"
fi

# ---- T9-C: Plain (unencrypted) index must still work -------------------------

info "--- T9-C: Plain index remains readable without KMS ---"

os_req DELETE "/pme-t9c-plain" > /dev/null 2>&1 || true
plain_resp=$(os_req PUT "/pme-t9c-plain" '{
  "settings": {"number_of_shards":1,"number_of_replicas":0},
  "mappings": {"properties":{"name":{"type":"keyword"}}}
}')
assert_status "T9-C/create-plain" "${plain_resp}" "200"

os_req POST "/pme-t9c-plain/_doc/0" '{"name":"unencrypted_doc"}' > /dev/null
os_req POST "/pme-t9c-plain/_refresh" > /dev/null

plain_search=$(os_req GET "/pme-t9c-plain/_search" '{"query":{"match_all":{}}}')
assert_status "T9-C/plain-search-status" "${plain_search}" "200"
plain_hits=$(jq -r '.hits.total.value' <<< "$(http_body "${plain_search}")" 2>/dev/null)
if [[ "${plain_hits}" == "1" ]]; then
    ok "T9-C/plain-readable-without-kms"
else
    fail "T9-C/plain-readable-without-kms" "expected 1 hit, got ${plain_hits}"
fi

os_req DELETE "/pme-t9c-plain" > /dev/null 2>&1 || true

# ---- summary -----------------------------------------------------------------

echo ""
echo "======================================="
echo " KMS-Unavailable Test Results"
echo "======================================="
echo " PASSED : ${PASS}"
echo " FAILED : ${FAIL}"
echo "======================================="

if [[ ${FAIL} -gt 0 ]]; then
    echo ""
    echo "Failed tests:"
    for e in "${ERRORS[@]}"; do
        echo "  - ${e}"
    done
    exit 1
fi
echo " All KMS-unavailable tests passed."
