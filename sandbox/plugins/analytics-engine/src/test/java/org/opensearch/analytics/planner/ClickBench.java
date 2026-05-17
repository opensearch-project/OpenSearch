/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.planner;

import java.util.Map;

/**
 * ClickBench workload schema for SQL-driven planner tests.
 *
 * <p>A subset of the ClickBench {@code hits} index covering the basic SQL types
 * exercised by pushdown / filter-delegation tests: integer, long, short, keyword, date.
 * Add fields when a test needs them.
 */
public final class ClickBench {

    public static final String INDEX = "hits";

    public static final Map<String, Map<String, Object>> BASIC_FIELDS = Map.of(
        "CounterID",
        Map.of("type", "integer"),
        "UserID",
        Map.of("type", "long"),
        "URL",
        Map.of("type", "keyword"),
        "Title",
        Map.of("type", "keyword"),
        "EventDate",
        Map.of("type", "date"),
        "AdvEngineID",
        Map.of("type", "short"),
        "ParamPrice",
        Map.of("type", "long")
    );

    private ClickBench() {}
}
