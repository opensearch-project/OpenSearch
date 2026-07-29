/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.dsl.types;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;

/**
 * Validates a parquet DSL {@code _search} response against a committed golden file.
 *
 * <p>Golden files live at {@code datasets/<type>/dsl/expected/q<N>.json} and capture the meaningful
 * parts of a search response:
 * <pre>
 *   {
 *     "hits": {
 *       "total": 3,
 *       "sources": [ { ... _source ... }, ... ]   // order-independent
 *     }
 *   }
 * </pre>
 * {@code total} is compared against {@code hits.total.value}; {@code sources} is compared against each
 * hit's {@code _source} as an unordered multiset with numeric tolerance. A {@code total} of 0 with an
 * empty (or absent) {@code sources} is a perfectly valid golden — e.g. an aggregation-only query or a
 * query that legitimately matches nothing.
 *
 * <p><b>The expected answer is the TRUE answer, not parquet's answer.</b> Expected answers are generated
 * out-of-band by running each dataset on a vanilla OpenSearch index (default Lucene backend, no parquet
 * settings) — the reference implementation — and committed. This suite only ever <i>validates</i> the
 * parquet response against the committed expected answer, so any place parquet deviates (null text
 * {@code _source}, multi-valued keyword / custom-id / geo / nested failures) correctly shows red. This
 * mirrors the PPL/SQL suite in {@code analytics-engine-rest}, whose expected files are known-correct
 * answers, never the engine's own output.
 */
public final class DslResponseValidator {

    private static final double NUMERIC_TOLERANCE = 1e-9;

    private DslResponseValidator() {}

    /**
     * Validate {@code actual} against the golden for {@code dataset}/q{@code queryNumber}.
     *
     * @return {@code null} when validation passes; otherwise a human-readable failure message
     */
    public static String validate(Dataset dataset, int queryNumber, Map<String, Object> actual) {
        String label = "DSL " + dataset.name + " Q" + queryNumber;
        String goldenPath = dataset.expectedResponseResourcePath("dsl", queryNumber);

        if (DslResponseValidator.class.getClassLoader().getResource(goldenPath) == null) {
            return label + ": no golden file at " + goldenPath;
        }

        Map<String, Object> golden;
        try {
            golden = parseJson(DatasetProvisioner.loadResource(goldenPath));
        } catch (Exception e) {
            return label + ": failed to load/parse golden " + goldenPath + ": " + e.getMessage();
        }

        return compare(golden, actual, label);
    }

    // ── comparison ───────────────────────────────────────────────────────────

    @SuppressWarnings("unchecked")
    private static String compare(Map<String, Object> golden, Map<String, Object> actual, String label) {
        Map<String, Object> goldenHits = (Map<String, Object>) golden.get("hits");
        if (goldenHits == null) {
            return label + ": golden has no \"hits\" block";
        }

        long expectedTotal = asLong(goldenHits.get("total"));
        long actualTotal = extractTotal(actual);
        if (expectedTotal != actualTotal) {
            return String.format(Locale.ROOT, "%s: total mismatch - expected %d, got %d", label, expectedTotal, actualTotal);
        }

        List<Map<String, Object>> expectedSources = asSourceList(goldenHits.get("sources"));
        List<Map<String, Object>> actualSources = extractSources(actual);

        if (expectedSources.size() != actualSources.size()) {
            return String.format(
                Locale.ROOT,
                "%s: hit count mismatch - expected %d sources, got %d",
                label,
                expectedSources.size(),
                actualSources.size()
            );
        }

        // Order-independent: sort both by canonical string form, then compare element-wise.
        List<Map<String, Object>> exp = new ArrayList<>(expectedSources);
        List<Map<String, Object>> act = new ArrayList<>(actualSources);
        Comparator<Map<String, Object>> byCanonical = Comparator.comparing(DslResponseValidator::canonical);
        exp.sort(byCanonical);
        act.sort(byCanonical);

        for (int i = 0; i < exp.size(); i++) {
            String diff = compareSource(exp.get(i), act.get(i), label + " source " + i);
            if (diff != null) {
                return diff;
            }
        }
        return null;
    }

    private static String compareSource(Map<String, Object> expected, Map<String, Object> actual, String label) {
        if (expected.size() != actual.size()) {
            return String.format(
                Locale.ROOT,
                "%s: field count mismatch - expected %s, got %s",
                label,
                new TreeMap<>(expected).keySet(),
                new TreeMap<>(actual).keySet()
            );
        }
        for (Map.Entry<String, Object> e : expected.entrySet()) {
            if (!actual.containsKey(e.getKey())) {
                return String.format(Locale.ROOT, "%s: missing field [%s]", label, e.getKey());
            }
            if (!valuesEqual(e.getValue(), actual.get(e.getKey()))) {
                return String.format(
                    Locale.ROOT,
                    "%s field [%s]: value mismatch - expected %s, got %s",
                    label,
                    e.getKey(),
                    e.getValue(),
                    actual.get(e.getKey())
                );
            }
        }
        return null;
    }

    @SuppressWarnings("unchecked")
    private static boolean valuesEqual(Object expected, Object actual) {
        if (expected instanceof Number && actual instanceof Number) {
            return Math.abs(((Number) expected).doubleValue() - ((Number) actual).doubleValue()) < NUMERIC_TOLERANCE;
        }
        if (expected instanceof List && actual instanceof List) {
            List<Object> e = new ArrayList<>((List<Object>) expected);
            List<Object> a = new ArrayList<>((List<Object>) actual);
            if (e.size() != a.size()) {
                return false;
            }
            Comparator<Object> byStr = Comparator.comparing(o -> o == null ? "" : o.toString());
            e.sort(byStr);
            a.sort(byStr);
            for (int i = 0; i < e.size(); i++) {
                if (!valuesEqual(e.get(i), a.get(i))) {
                    return false;
                }
            }
            return true;
        }
        if (expected instanceof Map && actual instanceof Map) {
            return compareSource((Map<String, Object>) expected, (Map<String, Object>) actual, "nested") == null;
        }
        return Objects.equals(expected, actual);
    }

    // ── extraction from a live response ────────────────────────────────────────

    @SuppressWarnings("unchecked")
    private static long extractTotal(Map<String, Object> response) {
        Map<String, Object> hits = (Map<String, Object>) response.get("hits");
        if (hits == null) {
            return 0;
        }
        Object total = hits.get("total");
        if (total instanceof Map) {
            return asLong(((Map<String, Object>) total).get("value"));
        }
        // Older/typed responses may carry a bare number.
        return asLong(total);
    }

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> extractSources(Map<String, Object> response) {
        List<Map<String, Object>> out = new ArrayList<>();
        Map<String, Object> hits = (Map<String, Object>) response.get("hits");
        if (hits == null) {
            return out;
        }
        List<Map<String, Object>> hitList = (List<Map<String, Object>>) hits.get("hits");
        if (hitList == null) {
            return out;
        }
        for (Map<String, Object> hit : hitList) {
            Object src = hit.get("_source");
            out.add(src instanceof Map ? (Map<String, Object>) src : Map.of());
        }
        return out;
    }

    // ── helpers ──────────────────────────────────────────────────────────────

    private static Map<String, Object> parseJson(String json) throws IOException {
        return org.opensearch.common.xcontent.XContentHelper.convertToMap(
            org.opensearch.common.xcontent.XContentType.JSON.xContent(),
            json,
            false
        );
    }

    private static long asLong(Object o) {
        return o instanceof Number ? ((Number) o).longValue() : 0L;
    }

    @SuppressWarnings("unchecked")
    private static List<Map<String, Object>> asSourceList(Object o) {
        List<Map<String, Object>> out = new ArrayList<>();
        if (o instanceof List) {
            for (Object e : (List<Object>) o) {
                if (e instanceof Map) {
                    out.add((Map<String, Object>) e);
                }
            }
        }
        return out;
    }

    /** Canonical string form of a source map: keys sorted recursively, for order-independent compare. */
    @SuppressWarnings("unchecked")
    private static String canonical(Object o) {
        if (o instanceof Map) {
            TreeMap<String, Object> sorted = new TreeMap<>((Map<String, Object>) o);
            StringBuilder sb = new StringBuilder("{");
            boolean first = true;
            for (Map.Entry<String, Object> e : sorted.entrySet()) {
                if (!first) {
                    sb.append(",");
                }
                first = false;
                sb.append('"').append(e.getKey()).append("\":").append(canonical(e.getValue()));
            }
            return sb.append("}").toString();
        }
        if (o instanceof List) {
            List<String> parts = new ArrayList<>();
            for (Object e : (List<Object>) o) {
                parts.add(canonical(e));
            }
            parts.sort(Comparator.naturalOrder());
            return "[" + String.join(",", parts) + "]";
        }
        if (o instanceof String) {
            return '"' + (String) o + '"';
        }
        return String.valueOf(o);
    }
}
