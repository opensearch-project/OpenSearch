#!/usr/bin/env bash
#
# SPDX-License-Identifier: Apache-2.0
#
# The OpenSearch Contributors require contributions made to
# this file be licensed under the Apache-2.0 license or a
# compatible open source license.
#

set -uo pipefail

# ---- configuration -----------------------------------------------------------

HOST="${1:-http://localhost:9200}"
# Allow --host flag anywhere
for arg in "$@"; do
    case "${arg}" in
        --host=*) HOST="${arg#--host=}" ;;
        --host)   ;; # value follows — handled below
    esac
done

PASS=0
FAIL=0
XFAIL=0
ERRORS=()
XFAILS=()
REST_SEQ=0
exec 3>&2

# ---- helpers -----------------------------------------------------------------

info()  { echo "[test] $*"; }
ok()    { echo "[test] ✓ $1"; PASS=$((PASS + 1)); }
fail()  { echo "[test] ✗ $1 — $2" >&2; FAIL=$((FAIL + 1)); ERRORS+=("$1: $2"); }
xfail() { echo "[test] XFAIL $1 — $2"; XFAIL=$((XFAIL + 1)); XFAILS+=("$1: $2"); }

log_rest_block() {
    local id="$1"
    local marker="$2"
    local content="$3"
    if [[ -z "${content}" ]]; then
        printf '[test][REST %s] %s <empty>\n' "${id}" "${marker}" >&3
        return
    fi
    if command -v jq &>/dev/null && jq -e . >/dev/null 2>&1 <<< "${content}"; then
        while IFS= read -r line; do
            printf '[test][REST %s] %s %s\n' "${id}" "${marker}" "${line}" >&3
        done < <(jq '.' <<< "${content}")
    else
        while IFS= read -r line; do
            printf '[test][REST %s] %s %s\n' "${id}" "${marker}" "${line}" >&3
        done <<< "${content}"
    fi
}

# Execute a curl request and print status code + body.
# Usage: os_req METHOD PATH [body_json]
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
    printf '[test][REST %s] > %s %s%s\n' "${request_id}" "${method}" "${HOST}" "${path}" >&3
    if [[ -n "${body}" ]]; then
        log_rest_block "${request_id}" ">" "${body}"
    fi
    local response
    response=$(curl "${args[@]}")
    local status
    status=$(tail -n1 <<< "${response}")
    local response_body
    response_body=$(sed '$d' <<< "${response}")
    printf '[test][REST %s] < HTTP %s\n' "${request_id}" "${status}" >&3
    log_rest_block "${request_id}" "<" "${response_body}"
    printf '%s\n' "${response}"
}

# Parse HTTP status code from os_req output (last line).
http_status() { tail -n1 <<< "$1"; }

# Parse response body from os_req output (all lines except the last).
# NOTE: head -n -1 is GNU-only; sed '$d' works on both macOS (BSD) and Linux.
http_body() { sed '$d' <<< "$1"; }

# Assert that a jq expression evaluates to the expected string.
# Usage: assert_jq TEST_NAME json_string jq_expr expected_value
assert_jq() {
    local name="$1"
    local json="$2"
    local expr="$3"
    local expected="$4"
    local actual
    actual=$(jq -r "${expr}" <<< "${json}" 2>/dev/null || echo "__jq_error__")
    if [[ "${actual}" == "${expected}" ]]; then
        ok "${name}"
    else
        fail "${name}" "expected '${expected}', got '${actual}'"
        echo "[test]   response body:" >&2
        echo "${json}" | jq '.' 2>/dev/null || echo "${json}" >&2
    fi
}

# Assert HTTP status code.
assert_status() {
    local name="$1"
    local response="$2"
    local expected="$3"
    local actual
    actual=$(http_status "${response}")
    if [[ "${actual}" == "${expected}" ]]; then
        ok "${name}"
    else
        fail "${name}" "expected HTTP ${expected}, got HTTP ${actual} — body: $(http_body "${response}")"
    fi
}

is_dataformat_search_failure() {
    local response="$1"
    [[ "$(http_status "${response}")" == "500" ]] \
        && grep -q "DataFormatAwareEngine directly on IndexShard" <<< "$(http_body "${response}")"
}

assert_search_hits_or_xfail() {
    local name="$1"
    local index="$2"
    local query_json="$3"
    local expected="$4"
    local resp
    resp=$(os_req GET "/${index}/_search" "${query_json}")
    if is_dataformat_search_failure "${resp}"; then
        xfail "${name}" "standard _search is not yet wired for DataFormatAwareEngine-backed shards"
        return 0
    fi
    assert_status "${name}/status" "${resp}" "200"
    local count
    count=$(jq -r '.hits.total.value' <<< "$(http_body "${resp}")" 2>/dev/null || echo "__jq_error__")
    if [[ "${count}" == "${expected}" ]]; then
        ok "${name}"
    else
        fail "${name}" "expected ${expected} hits, got ${count}"
    fi
}

assert_search_jq_or_xfail() {
    local name="$1"
    local path="$2"
    local query_json="$3"
    local jq_expr="$4"
    local expected="$5"
    local resp
    resp=$(os_req GET "${path}" "${query_json}")
    if is_dataformat_search_failure "${resp}"; then
        xfail "${name}" "standard _search is not yet wired for DataFormatAwareEngine-backed shards"
        return 0
    fi
    assert_status "${name}/status" "${resp}" "200"
    assert_jq "${name}" "$(http_body "${resp}")" "${jq_expr}" "${expected}"
}

# Index settings for an encrypted Parquet index (mock-pme provider).
encrypted_index_settings() {
    cat <<'JSON'
{
  "settings": {
    "index.pluggable.dataformat.enabled": true,
    "index.pluggable.dataformat": "parquet",
    "index.store.parquet.crypto.key_provider": "test",
    "index.store.parquet.crypto.key_provider_type": "mock-pme",
    "number_of_shards": 1,
    "number_of_replicas": 0
  },
  "mappings": {
    "properties": {
      "title":  { "type": "keyword" },
      "amount": { "type": "long"    },
      "tag":    { "type": "keyword" }
    }
  }
}
JSON
}

# Index settings for an unencrypted Parquet index.
unencrypted_parquet_index_settings() {
    cat <<'JSON'
{
  "settings": {
    "index.pluggable.dataformat.enabled": true,
    "index.pluggable.dataformat": "parquet",
    "number_of_shards": 1,
    "number_of_replicas": 0
  },
  "mappings": {
    "properties": {
      "title":  { "type": "keyword" },
      "amount": { "type": "long"    },
      "tag":    { "type": "keyword" }
    }
  }
}
JSON
}

plain_index_settings() {
    cat <<'JSON'
{
  "settings": {
    "number_of_shards": 1,
    "number_of_replicas": 0
  },
  "mappings": {
    "properties": {
      "name": { "type": "keyword" }
    }
  }
}
JSON
}

# Assert that an index was created with the expected encrypted-Parquet settings.
# Usage: assert_encrypted_index_settings TEST_PREFIX index_name
assert_encrypted_index_settings() {
    local prefix="$1"
    local index="$2"
    local s
    s=$(os_req GET "/${index}/_settings")
    assert_status "${prefix}/settings-status" "${s}" "200"
    local body
    body=$(http_body "${s}")
    assert_jq "${prefix}/parquet-enabled" "${body}" ".[\"${index}\"].settings.index.pluggable[\"dataformat.enabled\"]" "true"
    assert_jq "${prefix}/parquet-format"  "${body}" ".[\"${index}\"].settings.index.pluggable.dataformat"              "parquet"
    assert_jq "${prefix}/crypto-provider" "${body}" ".[\"${index}\"].settings.index.store.parquet.crypto.key_provider_type" "mock-pme"
}

# Assert that an index was created with Parquet enabled and no encryption provider.
# Usage: assert_unencrypted_parquet_index_settings TEST_PREFIX index_name
assert_unencrypted_parquet_index_settings() {
    local prefix="$1"
    local index="$2"
    local s
    s=$(os_req GET "/${index}/_settings")
    assert_status "${prefix}/settings-status" "${s}" "200"
    local body
    body=$(http_body "${s}")
    assert_jq "${prefix}/parquet-enabled" "${body}" ".[\"${index}\"].settings.index.pluggable[\"dataformat.enabled\"]" "true"
    assert_jq "${prefix}/parquet-format"  "${body}" ".[\"${index}\"].settings.index.pluggable.dataformat"              "parquet"
    local provider
    provider=$(jq -r ".[\"${index}\"].settings.index.store.parquet.crypto.key_provider_type // \"null\"" <<< "${body}" 2>/dev/null)
    if [[ "${provider}" == "null" ]]; then
        ok "${prefix}/no-crypto-provider"
    else
        fail "${prefix}/no-crypto-provider" "expected no crypto provider setting, got '${provider}'"
        echo "[test]   response body:" >&2
        echo "${body}" | jq '.' 2>/dev/null || echo "${body}" >&2
    fi
}

# Assert that an index was created WITHOUT Parquet/encryption settings.
# Usage: assert_plain_index_settings TEST_PREFIX index_name
assert_plain_index_settings() {
    local prefix="$1"
    local index="$2"
    local s
    s=$(os_req GET "/${index}/_settings")
    assert_status "${prefix}/settings-status" "${s}" "200"
    local body
    body=$(http_body "${s}")
    local pf
    pf=$(jq -r ".[\"${index}\"].settings.index.pluggable.dataformat // \"null\"" <<< "${body}" 2>/dev/null)
    if [[ "${pf}" == "null" || -z "${pf}" ]]; then
        ok "${prefix}/no-parquet"
    else
        fail "${prefix}/no-parquet" "expected no parquet dataformat setting, got '${pf}'"
        echo "[test]   response body:" >&2
        echo "${body}" | jq '.' 2>/dev/null || echo "${body}" >&2
    fi
}

# ---- prerequisite check ------------------------------------------------------

info "Checking cluster health at ${HOST} ..."
health_resp=$(os_req GET "/_cluster/health")
if [[ "$(http_status "${health_resp}")" != "200" ]]; then
    echo "[test] ERROR: cluster not reachable at ${HOST}" >&2
    exit 1
fi
health_status=$(jq -r '.status' <<< "$(http_body "${health_resp}")" 2>/dev/null)
info "Cluster status: ${health_status}"

# Verify parquet-data-format plugin is installed.
plugins_resp=$(os_req GET "/_cat/plugins?h=component&format=json")
if ! jq -r '.[].component' <<< "$(http_body "${plugins_resp}")" 2>/dev/null | grep -q "parquet-data-format"; then
    echo "[test] ERROR: parquet-data-format plugin is not installed. Run setup-cluster.sh first." >&2
    exit 1
fi
if ! jq -r '.[].component' <<< "$(http_body "${plugins_resp}")" 2>/dev/null | grep -q "mock-pme-kms"; then
    echo "[test] ERROR: mock-pme-kms plugin is not installed. Run setup-cluster.sh first." >&2
    exit 1
fi
if ! jq -r '.[].component' <<< "$(http_body "${plugins_resp}")" 2>/dev/null | grep -q "analytics-backend-lucene"; then
    echo "[test] ERROR: analytics-backend-lucene plugin is not installed. It provides the CommitterFactory required by the pluggable data format engine." >&2
    exit 1
fi
if ! jq -r '.[].component' <<< "$(http_body "${plugins_resp}")" 2>/dev/null | grep -q "analytics-backend-datafusion"; then
    echo "[test] ERROR: analytics-backend-datafusion plugin is not installed. It provides the SearchBackEndPlugin for ParquetDataFormat." >&2
    exit 1
fi
info "Required plugins present."

# ---- cleanup from previous run -----------------------------------------------

for idx in pme-t1 pme-t7-alpha pme-t7-beta pme-t8-parquet pme-plain; do
    os_req DELETE "/${idx}" > /dev/null 2>&1 || true
done

# ---- T1: Create encrypted index ----------------------------------------------

info "--- T1: Create encrypted Parquet index ---"
resp=$(os_req PUT "/pme-t1" "$(encrypted_index_settings)")
assert_status "T1/create-status" "${resp}" "200"
assert_jq    "T1/create-ack"    "$(http_body "${resp}")" '.acknowledged' "true"

# ---- T1b: Verify index settings confirm Parquet + encryption -----------------

info "--- T1b: Verify index settings (Parquet + encryption) ---"
assert_encrypted_index_settings "T1b" "pme-t1"

# ---- T2: Index documents and refresh -----------------------------------------

info "--- T2: Index documents + refresh ---"
docs=(
    '{"title":"alpha","amount":100,"tag":"group-a"}'
    '{"title":"beta", "amount":200,"tag":"group-a"}'
    '{"title":"gamma","amount":300,"tag":"group-b"}'
    '{"title":"delta","amount":400,"tag":"group-b"}'
    '{"title":"epsilon","amount":500,"tag":"group-b"}'
)
for i in "${!docs[@]}"; do
    r=$(os_req POST "/pme-t1/_doc/${i}" "${docs[$i]}")
    assert_status "T2/index-doc-${i}" "${r}" "201"
done

r=$(os_req POST "/pme-t1/_refresh")
assert_status "T2/refresh" "${r}" "200"

# ---- T3: Match-all returns all docs ------------------------------------------

info "--- T3: Match-all returns all documents ---"
assert_search_hits_or_xfail "T3/match-all-count" "pme-t1" '{"query":{"match_all":{}}}' "5"

# ---- T4: Term query ----------------------------------------------------------

info "--- T4: Term query on encrypted field ---"
resp=$(os_req GET "/pme-t1/_search" '{"query":{"term":{"tag":"group-a"}}}')
if is_dataformat_search_failure "${resp}"; then
    xfail "T4/term-search" "standard _search is not yet wired for DataFormatAwareEngine-backed shards"
else
    assert_status "T4/term-status" "${resp}" "200"
    assert_jq    "T4/term-count"   "$(http_body "${resp}")" '.hits.total.value' "2"
    # Verify the returned document titles
    titles=$(jq -r '[.hits.hits[]._source.title] | sort | join(",")' <<< "$(http_body "${resp}")")
    if [[ "${titles}" == "alpha,beta" ]]; then
        ok "T4/term-titles"
    else
        fail "T4/term-titles" "expected 'alpha,beta', got '${titles}'"
    fi
fi

# ---- T5: Aggregation ---------------------------------------------------------

info "--- T5: Aggregation (sum + count) over encrypted field ---"
agg_query='{"size":0,"aggs":{"total_amount":{"sum":{"field":"amount"}},"doc_count":{"value_count":{"field":"amount"}}}}'
resp=$(os_req GET "/pme-t1/_search" "${agg_query}")
if is_dataformat_search_failure "${resp}"; then
    xfail "T5/agg-search" "standard _search is not yet wired for DataFormatAwareEngine-backed shards"
else
    assert_status "T5/agg-status"  "${resp}" "200"
    # OpenSearch returns sum as a float (1500.0); compare with floor to handle both int and float.
    agg_sum=$(jq -r '.aggregations.total_amount.value | floor' <<< "$(http_body "${resp}")" 2>/dev/null)
    if [[ "${agg_sum}" == "1500" ]]; then ok "T5/agg-sum"; else fail "T5/agg-sum" "expected 1500, got ${agg_sum}"; fi
    assert_jq    "T5/agg-count"    "$(http_body "${resp}")" '.aggregations.doc_count.value'   "5"
fi

# ---- T6: Delete encrypted index ----------------------------------------------

info "--- T6: Delete encrypted index ---"
resp=$(os_req DELETE "/pme-t1")
assert_status "T6/delete-status" "${resp}" "200"
assert_jq    "T6/delete-ack"    "$(http_body "${resp}")" '.acknowledged' "true"

# After deletion the index must not exist.
resp=$(os_req GET "/pme-t1/_search" '{"query":{"match_all":{}}}')
del_status=$(http_status "${resp}")
if [[ "${del_status}" == "404" ]]; then
    ok "T6/post-delete-404"
else
    fail "T6/post-delete-404" "expected HTTP 404 after deletion, got ${del_status}"
fi

# ---- T7: Two independent encrypted indices -----------------------------------

info "--- T7: Two independent encrypted indices ---"
resp=$(os_req PUT "/pme-t7-alpha" "$(encrypted_index_settings)")
assert_status "T7/create-alpha" "${resp}" "200"
assert_encrypted_index_settings "T7/settings-alpha" "pme-t7-alpha"
resp=$(os_req PUT "/pme-t7-beta" "$(encrypted_index_settings)")
assert_status "T7/create-beta"  "${resp}" "200"
assert_encrypted_index_settings "T7/settings-beta" "pme-t7-beta"

for i in 0 1 2; do
    os_req POST "/pme-t7-alpha/_doc/${i}" "{\"title\":\"a${i}\",\"amount\":$((i*10)),\"tag\":\"alpha\"}" > /dev/null
    os_req POST "/pme-t7-beta/_doc/${i}"  "{\"title\":\"b${i}\",\"amount\":$((i*100)),\"tag\":\"beta\"}"  > /dev/null
done

os_req POST "/pme-t7-alpha/_refresh" > /dev/null
os_req POST "/pme-t7-beta/_refresh"  > /dev/null

# Each index should see only its own documents once Parquet search is wired.
assert_search_hits_or_xfail "T7/alpha-count" "pme-t7-alpha" '{"query":{"match_all":{}}}' "3"
assert_search_hits_or_xfail "T7/beta-count" "pme-t7-beta" '{"query":{"match_all":{}}}' "3"

# A cross-index query must return exactly 6.
assert_search_jq_or_xfail "T7/cross-index-count" "/pme-t7-alpha,pme-t7-beta/_search" '{"query":{"match_all":{}}}' '.hits.total.value' "6"

# Data must stay isolated: alpha docs must not appear in beta and vice versa.
assert_search_jq_or_xfail "T7/alpha-no-beta-data" "/pme-t7-alpha/_search" '{"query":{"term":{"tag":"beta"}}}' '.hits.total.value' "0"
assert_search_jq_or_xfail "T7/beta-no-alpha-data" "/pme-t7-beta/_search" '{"query":{"term":{"tag":"alpha"}}}' '.hits.total.value' "0"

# ---- T8-A: Unencrypted Parquet index has same search limitation -------------

info "--- T8-A: Unencrypted Parquet index has same _search limitation ---"
resp=$(os_req PUT "/pme-t8-parquet" "$(unencrypted_parquet_index_settings)")
assert_status "T8-A/create-unencrypted-parquet" "${resp}" "200"
assert_unencrypted_parquet_index_settings "T8-A/settings-unencrypted-parquet" "pme-t8-parquet"

for i in 0 1; do
    os_req POST "/pme-t8-parquet/_doc/${i}" "{\"title\":\"parquet_${i}\",\"amount\":$((i+1)),\"tag\":\"plain-parquet\"}" > /dev/null
done
os_req POST "/pme-t8-parquet/_refresh" > /dev/null

assert_search_hits_or_xfail "T8-A/unencrypted-parquet-count" "pme-t8-parquet" '{"query":{"match_all":{}}}' "2"

# ---- T8-B: Plain Lucene index alongside Parquet indices ---------------------

info "--- T8-B: Plain Lucene index works alongside Parquet indices ---"
resp=$(os_req PUT "/pme-plain" "$(plain_index_settings)")
assert_status "T8-B/create-plain" "${resp}" "200"
assert_plain_index_settings "T8-B/settings-plain" "pme-plain"

for i in 0 1; do
    os_req POST "/pme-plain/_doc/${i}" "{\"name\":\"plain_doc_${i}\"}" > /dev/null
done
os_req POST "/pme-plain/_refresh" > /dev/null

resp=$(os_req GET "/pme-plain/_search" '{"query":{"match_all":{}}}')
assert_status "T8-B/plain-search-status" "${resp}" "200"
assert_jq "T8-B/plain-count" "$(http_body "${resp}")" '.hits.total.value' "2"

# ---- cleanup -----------------------------------------------------------------

for idx in pme-t7-alpha pme-t7-beta pme-t8-parquet pme-plain; do
    os_req DELETE "/${idx}" > /dev/null 2>&1 || true
done

# ---- T9: Negative — encrypted index readable only when KMS is available ------
# This index is intentionally NOT deleted here.  run-all.sh will restart the
# cluster without the mock-pme-kms plugin and verify that the index cannot be
# searched in that configuration (see test-pme-kms-unavailable.sh).

info "--- T9: Create persistent encrypted index for KMS-unavailable test ---"
os_req DELETE "/pme-t9-persist" > /dev/null 2>&1 || true
resp=$(os_req PUT "/pme-t9-persist" "$(encrypted_index_settings)")
assert_status "T9/create-persist" "${resp}" "200"
assert_encrypted_index_settings "T9/settings-persist" "pme-t9-persist"

for i in 0 1 2; do
    r=$(os_req POST "/pme-t9-persist/_doc/${i}" "{\"title\":\"secret_${i}\",\"amount\":$((i*7)),\"tag\":\"secret\"}")
    assert_status "T9/index-doc-${i}" "${r}" "201"
done
r=$(os_req POST "/pme-t9-persist/_refresh")
assert_status "T9/refresh" "${r}" "200"

assert_search_hits_or_xfail "T9/readable-with-kms" "pme-t9-persist" '{"query":{"match_all":{}}}' "3"
info "Index pme-t9-persist left alive for KMS-unavailable test phase."

# ---- summary -----------------------------------------------------------------

echo ""
echo "======================================="
echo " PME E2E Test Results"
echo "======================================="
echo " PASSED : ${PASS}"
echo " XFAIL  : ${XFAIL}"
echo " FAILED : ${FAIL}"
echo "======================================="

if [[ ${XFAIL} -gt 0 ]]; then
    echo ""
    echo "Expected failures:"
    for e in "${XFAILS[@]}"; do
        echo "  - ${e}"
    done
fi

if [[ ${FAIL} -gt 0 ]]; then
    echo ""
    echo "Failed tests:"
    for e in "${ERRORS[@]}"; do
        echo "  - ${e}"
    done
    exit 1
fi
echo " All tests passed."
