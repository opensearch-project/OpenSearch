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

import java.io.IOException;
import java.util.Locale;
import java.io.InputStream;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;

/**
 * Loads and provides the named {@link SettingsCombo}s from the global {@code planshape/combos.yaml}
 * — the single source of truth mapping each combo name to its plan-shape-affecting settings.
 * Goldens reference combos only by name.
 */
public final class SettingsComboRegistry {

    /** Insertion-ordered (file order) so {@link #allNames()} reflects the combos.yaml order. */
    private final LinkedHashMap<String, SettingsCombo> byName;
    private final List<String> defaults;

    private SettingsComboRegistry(LinkedHashMap<String, SettingsCombo> byName, List<String> defaults) {
        this.byName = byName;
        this.defaults = defaults;
    }

    /** Look up a combo by name, failing loudly if a golden references an undefined combo. */
    public SettingsCombo byName(String name) {
        SettingsCombo combo = byName.get(name);
        if (combo == null) {
            throw new IllegalArgumentException(
                String.format(Locale.ROOT, "Unknown combo '%s' — defined combos: %s", name, byName.keySet())
            );
        }
        return combo;
    }

    /** Combo names CI runs by default (the file's {@code defaults} list). */
    public List<String> defaults() {
        return defaults;
    }

    /** Every defined combo name, in file order. */
    public List<String> allNames() {
        return List.copyOf(byName.keySet());
    }

    /** Load and parse a {@code combos.yaml} classpath resource into named {@link SettingsCombo}s. */
    @SuppressWarnings("unchecked")
    public static SettingsComboRegistry load(String resourcePath) throws IOException {
        try (InputStream is = SettingsComboRegistry.class.getClassLoader().getResourceAsStream(resourcePath)) {
            if (is == null) {
                throw new IllegalStateException(
                    String.format(Locale.ROOT, "combos resource not found: %s", resourcePath)
                );
            }
            Map<String, Object> root = parseYaml(is);

            Map<String, Object> combos = (Map<String, Object>) root.get("combos");
            if (combos == null || combos.isEmpty()) {
                throw new IllegalStateException(
                    String.format(Locale.ROOT, "combos.yaml has no 'combos' block: %s", resourcePath)
                );
            }

            LinkedHashMap<String, SettingsCombo> byName = new LinkedHashMap<>();
            for (Map.Entry<String, Object> entry : combos.entrySet()) {
                String comboName = entry.getKey();
                Map<String, Object> comboBody = (Map<String, Object>) entry.getValue();

                Map<String, Object> cluster = (Map<String, Object>) comboBody.getOrDefault("cluster", Map.of());
                Map<String, Object> index = (Map<String, Object>) comboBody.getOrDefault("index", Map.of());
                if (!index.containsKey(SettingsCombo.NUMBER_OF_SHARDS)) {
                    throw new IllegalStateException(
                        String.format(Locale.ROOT, "combo '%s' must declare index.%s", comboName, SettingsCombo.NUMBER_OF_SHARDS)
                    );
                }

                byName.put(comboName, new SettingsCombo(comboName, cluster, index));
            }

            List<String> defaults = (List<String>) root.get("defaults");
            if (defaults == null || defaults.isEmpty()) {
                throw new IllegalStateException(
                    String.format(Locale.ROOT, "combos.yaml has no 'defaults' list: %s", resourcePath)
                );
            }
            return new SettingsComboRegistry(byName, defaults);
        }
    }

    private static Map<String, Object> parseYaml(InputStream is) throws IOException {
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
