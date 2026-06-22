/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa.planshape;

import org.opensearch.common.xcontent.yaml.YamlXContent;
import org.opensearch.core.xcontent.DeprecationHandler;
import org.opensearch.core.xcontent.NamedXContentRegistry;
import org.opensearch.core.xcontent.XContentParser;

import java.io.BufferedReader;
import java.util.Locale;
import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.nio.charset.StandardCharsets;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Loads one query's {@code q{N}.plan.yaml} golden into an {@link ExpectedQueryPlan}.
 *
 * <p>The golden declares the query EXACTLY ONE way: inline {@code ppl:} or a referenced
 * {@code ppl_file:} (path relative to the workload's query dir). It lists the combos it
 * {@code applies} to, and under {@code plans} gives, per combo, the expected text for each layer.
 * A layer value is either literal plan text or a {@code {same_as: <combo>}} reference (one hop),
 * resolved here. An omitted layer means the layer is expected absent.
 */
public final class ExpectedQueryPlanLoader {

    private ExpectedQueryPlanLoader() {}

    @SuppressWarnings("unchecked")
    public static ExpectedQueryPlan load(ClassLoader loader, String goldenResourcePath, String queryDir) throws IOException {
        Map<String, Object> root = parseYaml(loader, goldenResourcePath);

        String queryId = stringField(root, "query", goldenResourcePath);
        String queryText = resolveQueryText(loader, root, queryDir, goldenResourcePath);

        List<String> applies = (List<String>) root.get("applies");
        if (applies == null || applies.isEmpty()) {
            throw new IllegalStateException(
                String.format(Locale.ROOT, "golden '%s' has no 'applies' combos", goldenResourcePath)
            );
        }

        Map<String, Object> plans = (Map<String, Object>) root.get("plans");
        if (plans == null) {
            throw new IllegalStateException(
                String.format(Locale.ROOT, "golden '%s' has no 'plans' block", goldenResourcePath)
            );
        }

        // First pass: collect each combo's raw layer map (text or same_as ref) without resolving.
        Map<String, Map<String, Object>> rawByCombo = new LinkedHashMap<>();
        for (String combo : applies) {
            Object body = plans.get(combo);
            if (body == null) {
                throw new IllegalStateException(
                    String.format(Locale.ROOT, "golden '%s' applies to combo '%s' but has no plans for it", goldenResourcePath, combo)
                );
            }
            rawByCombo.put(combo, (Map<String, Object>) body);
        }

        // Second pass: resolve same_as (one hop) into concrete layer text.
        Map<String, Map<String, String>> resolved = new LinkedHashMap<>();
        for (String combo : applies) {
            Map<String, Object> raw = rawByCombo.get(combo);
            Map<String, String> byLayer = new LinkedHashMap<>();
            for (Map.Entry<String, Object> layer : raw.entrySet()) {
                byLayer.put(layer.getKey(), resolveLayer(layer.getValue(), rawByCombo, combo, layer.getKey(), goldenResourcePath));
            }
            resolved.put(combo, byLayer);
        }

        return new ExpectedQueryPlan(queryId, queryText, applies, resolved);
    }

    /** Resolve a layer value: a plain string is plan text; a {@code {same_as: <combo>}} map is a one-hop ref. */
    @SuppressWarnings("unchecked")
    private static String resolveLayer(
        Object value,
        Map<String, Map<String, Object>> rawByCombo,
        String combo,
        String layerKey,
        String goldenResourcePath
    ) {
        if (value instanceof String s) {
            return s;
        }
        if (value instanceof Map) {
            Object ref = ((Map<String, Object>) value).get("same_as");
            if (ref instanceof String refCombo) {
                Map<String, Object> target = rawByCombo.get(refCombo);
                if (target == null) {
                    throw new IllegalStateException(
                        String.format(Locale.ROOT, 
                            "golden '%s' combo '%s' layer '%s' references same_as '%s', which is not an applied combo",
                            goldenResourcePath, combo, layerKey, refCombo
                        )
                    );
                }
                Object targetValue = target.get(layerKey);
                if (!(targetValue instanceof String s)) {
                    throw new IllegalStateException(
                        String.format(Locale.ROOT, 
                            "golden '%s' combo '%s' layer '%s' same_as '%s' must point at literal text (no chained same_as / missing layer)",
                            goldenResourcePath, combo, layerKey, refCombo
                        )
                    );
                }
                return s;
            }
        }
        throw new IllegalStateException(
            String.format(Locale.ROOT, "golden '%s' combo '%s' layer '%s' is neither text nor a same_as map", goldenResourcePath, combo, layerKey)
        );
    }

    /** Resolve query text from inline {@code ppl:} XOR referenced {@code ppl_file:}. */
    private static String resolveQueryText(ClassLoader loader, Map<String, Object> root, String queryDir, String goldenResourcePath)
        throws IOException {
        Object inline = root.get("ppl");
        Object file = root.get("ppl_file");
        if ((inline == null) == (file == null)) {
            throw new IllegalStateException(
                String.format(Locale.ROOT, "golden '%s' must declare exactly one of 'ppl' or 'ppl_file'", goldenResourcePath)
            );
        }
        if (inline != null) {
            return ((String) inline).strip();
        }
        return loadResource(loader, queryDir + "/" + file).strip();
    }

    private static String stringField(Map<String, Object> root, String key, String goldenResourcePath) {
        Object v = root.get(key);
        if (!(v instanceof String s)) {
            throw new IllegalStateException(
                String.format(Locale.ROOT, "golden '%s' missing required string field '%s'", goldenResourcePath, key)
            );
        }
        return s;
    }

    private static Map<String, Object> parseYaml(ClassLoader loader, String resourcePath) throws IOException {
        try (InputStream is = loader.getResourceAsStream(resourcePath)) {
            if (is == null) {
                throw new IllegalStateException(
                    String.format(Locale.ROOT, "golden resource not found: %s", resourcePath)
                );
            }
            try (
                XContentParser parser = YamlXContent.yamlXContent.createParser(
                    NamedXContentRegistry.EMPTY,
                    DeprecationHandler.IGNORE_DEPRECATIONS,
                    is
                )
            ) {
                return parser.map();
            }
        }
    }

    private static String loadResource(ClassLoader loader, String resourcePath) throws IOException {
        try (InputStream is = loader.getResourceAsStream(resourcePath)) {
            if (is == null) {
                throw new IllegalStateException(
                    String.format(Locale.ROOT, "query resource not found: %s", resourcePath)
                );
            }
            try (BufferedReader reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))) {
                return reader.lines().collect(Collectors.joining("\n"));
            }
        }
    }
}
