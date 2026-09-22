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

    /**
     * Load one query's {@code q{N}.plan.yaml} golden (at {@code goldenResourcePath} on the
     * classpath) into an {@link ExpectedQueryPlan}: resolves the query text (inline {@code ppl} or a
     * {@code ppl_file} under {@code queryDir}) and the per-combo, per-layer expected plan text,
     * resolving any {@code same_as} references.
     */
    @SuppressWarnings("unchecked")
    public static ExpectedQueryPlan loadGolden(String goldenResourcePath, String queryDir) throws IOException {
        Map<String, Object> root = parseYaml(goldenResourcePath);

        String queryId = requiredString(root, "query", goldenResourcePath);
        String queryText = resolveQueryText(root, queryDir, goldenResourcePath);

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
            Object comboPlans = plans.get(combo);
            if (comboPlans == null) {
                throw new IllegalStateException(
                    String.format(Locale.ROOT, "golden '%s' applies to combo '%s' but has no plans for it", goldenResourcePath, combo)
                );
            }
            rawByCombo.put(combo, (Map<String, Object>) comboPlans);
        }

        // Second pass: resolve same_as (one hop) into concrete layer text.
        Map<String, Map<String, String>> resolvedByCombo = new LinkedHashMap<>();
        for (String combo : applies) {
            Map<String, Object> rawLayers = rawByCombo.get(combo);
            Map<String, String> textByLayerKey = new LinkedHashMap<>();
            for (Map.Entry<String, Object> layer : rawLayers.entrySet()) {
                textByLayerKey.put(layer.getKey(), resolveLayerText(layer.getValue(), rawByCombo, combo, layer.getKey(), goldenResourcePath));
            }
            resolvedByCombo.put(combo, textByLayerKey);
        }

        return new ExpectedQueryPlan(queryId, queryText, applies, resolvedByCombo);
    }

    /**
     * The expected plan text for one layer of one combo. A layer value in the golden is either the
     * literal plan text, or a {@code {same_as: <otherCombo>}} map that reuses that other combo's text
     * for the same layer verbatim (so identical plans aren't duplicated across combos). Resolves the
     * latter — one hop only; the target must itself be literal text.
     */
    @SuppressWarnings("unchecked")
    private static String resolveLayerText(
        Object rawLayerValue,
        Map<String, Map<String, Object>> rawByCombo,
        String combo,
        String layerKey,
        String goldenResourcePath
    ) {
        if (rawLayerValue instanceof String literalText) {
            return literalText;
        }
        if (rawLayerValue instanceof Map) {
            Object sameAs = ((Map<String, Object>) rawLayerValue).get("same_as");
            if (sameAs instanceof String referencedCombo) {
                Map<String, Object> referencedLayers = rawByCombo.get(referencedCombo);
                if (referencedLayers == null) {
                    throw new IllegalStateException(
                        String.format(Locale.ROOT,
                            "golden '%s' combo '%s' layer '%s' references same_as '%s', which is not an applied combo",
                            goldenResourcePath, combo, layerKey, referencedCombo
                        )
                    );
                }
                Object referencedText = referencedLayers.get(layerKey);
                if (!(referencedText instanceof String literalText)) {
                    throw new IllegalStateException(
                        String.format(Locale.ROOT,
                            "golden '%s' combo '%s' layer '%s' same_as '%s' must point at literal text (no chained same_as / missing layer)",
                            goldenResourcePath, combo, layerKey, referencedCombo
                        )
                    );
                }
                return literalText;
            }
        }
        throw new IllegalStateException(
            String.format(Locale.ROOT, "golden '%s' combo '%s' layer '%s' is neither text nor a same_as map", goldenResourcePath, combo, layerKey)
        );
    }

    /** Resolve query text from inline {@code ppl:} XOR referenced {@code ppl_file:}. */
    private static String resolveQueryText(Map<String, Object> root, String queryDir, String goldenResourcePath) throws IOException {
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
        return loadResource(queryDir + "/" + file).strip();
    }

    /** Read a required top-level string field from the parsed golden, failing if absent or non-string. */
    private static String requiredString(Map<String, Object> root, String key, String goldenResourcePath) {
        Object value = root.get(key);
        if (!(value instanceof String s)) {
            throw new IllegalStateException(
                String.format(Locale.ROOT, "golden '%s' missing required string field '%s'", goldenResourcePath, key)
            );
        }
        return s;
    }

    private static Map<String, Object> parseYaml(String resourcePath) throws IOException {
        try (InputStream is = resourceStream(resourcePath, "golden")) {
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

    private static String loadResource(String resourcePath) throws IOException {
        try (
            InputStream is = resourceStream(resourcePath, "query");
            BufferedReader reader = new BufferedReader(new InputStreamReader(is, StandardCharsets.UTF_8))
        ) {
            return reader.lines().collect(Collectors.joining("\n"));
        }
    }

    /** Open a classpath resource (from this class's loader), failing with a {@code kind}-tagged message. */
    private static InputStream resourceStream(String resourcePath, String kind) {
        InputStream is = ExpectedQueryPlanLoader.class.getClassLoader().getResourceAsStream(resourcePath);
        if (is == null) {
            throw new IllegalStateException(String.format(Locale.ROOT, "%s resource not found: %s", kind, resourcePath));
        }
        return is;
    }
}
