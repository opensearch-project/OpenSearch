/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner;

import java.util.Collections;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * ClickBench workload schema for SQL-driven planner tests.
 *
 * <p>A subset of the ClickBench {@code hits} index covering the basic SQL types
 * exercised by pushdown / filter-delegation tests: integer, long, short, keyword, date.
 * Add fields when a test needs them.
 *
 * <p>Backed by {@link LinkedHashMap} so column order is deterministic across JVM
 * launches. {@code Map.of}'s iteration order is JVM-seed-salted, which surfaces as
 * flaky tests when assertions depend on column ordering (e.g. plan-shape tests
 * comparing scan rowType field order).
 */
public final class ClickBench {

    public static final String INDEX = "hits";

    public static final Map<String, Map<String, Object>> BASIC_FIELDS;

    static {
        LinkedHashMap<String, Map<String, Object>> fields = new LinkedHashMap<>();
        fields.put("CounterID", Map.of("type", "integer"));
        fields.put("UserID", Map.of("type", "long"));
        fields.put("URL", Map.of("type", "keyword"));
        fields.put("Title", Map.of("type", "keyword"));
        fields.put("EventDate", Map.of("type", "date"));
        fields.put("AdvEngineID", Map.of("type", "short"));
        fields.put("ParamPrice", Map.of("type", "long"));
        BASIC_FIELDS = Collections.unmodifiableMap(fields);
    }

    private ClickBench() {}
}
