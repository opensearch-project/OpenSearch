/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.node;

import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.common.settings.Settings;

import java.util.Arrays;
import java.util.Locale;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Node attribute that distinguishes which engine a data node runs.
 *
 * <p>Set on a node via:
 * <pre>
 *   node.attr.engine_mode: lucene     # default Lucene engine
 *   node.attr.engine_mode: analytics  # DataFormatAware analytics engine
 * </pre>
 *
 * The attribute is optional. When absent the node is treated as {@link EngineMode#LUCENE},
 * which preserves the pre-existing behavior of every node before this attribute was introduced.
 *
 * @opensearch.internal
 */
public final class EngineModeNodeAttribute {

    /** Bare attribute key as it appears on a {@link DiscoveryNode}'s attribute map. */
    public static final String ENGINE_MODE_ATTRIBUTE_KEY = "engine_mode";

    /** Fully-qualified setting key (i.e. {@code node.attr.engine_mode}) as it appears in {@link Settings}. */
    public static final String ENGINE_MODE_SETTING_KEY = Node.NODE_ATTRIBUTES.getKey() + ENGINE_MODE_ATTRIBUTE_KEY;

    private EngineModeNodeAttribute() {}

    /**
     * Allowed values for {@code node.attr.engine_mode}.
     */
    public enum EngineMode {
        /** Default Lucene-backed engine ({@code InternalEngine}). */
        LUCENE,
        /** DataFormat-aware analytics engine. */
        ANALYTICS;

        /** Lower-cased name used as the on-the-wire attribute value. */
        public String attributeValue() {
            return name().toLowerCase(Locale.ROOT);
        }
    }

    /** Default mode applied when the attribute is absent. */
    public static final EngineMode DEFAULT = EngineMode.LUCENE;

    /**
     * Parse an attribute value into an {@link EngineMode}. Comparison is case-insensitive.
     *
     * @throws IllegalArgumentException if the value is not one of the known modes
     */
    public static EngineMode parse(String value) {
        if (value == null || value.isEmpty()) {
            return DEFAULT;
        }
        String normalized = value.toLowerCase(Locale.ROOT);
        for (EngineMode mode : EngineMode.values()) {
            if (mode.attributeValue().equals(normalized)) {
                return mode;
            }
        }
        throw new IllegalArgumentException(
            "Unknown "
                + ENGINE_MODE_SETTING_KEY
                + " ["
                + value
                + "], must be one of "
                + Arrays.stream(EngineMode.values()).map(EngineMode::attributeValue).collect(Collectors.toList())
        );
    }

    /** Return the {@link EngineMode} for the given node, falling back to {@link #DEFAULT} when absent. */
    public static EngineMode get(DiscoveryNode node) {
        return parse(node.getAttributes().get(ENGINE_MODE_ATTRIBUTE_KEY));
    }

    /** Return the {@link EngineMode} for the given node attribute map, falling back to {@link #DEFAULT} when absent. */
    public static EngineMode get(Map<String, String> attributes) {
        return parse(attributes.get(ENGINE_MODE_ATTRIBUTE_KEY));
    }

    /** Return the {@link EngineMode} configured for the local node via {@code node.attr.engine_mode} in {@link Settings}. */
    public static EngineMode get(Settings settings) {
        return parse(settings.get(ENGINE_MODE_SETTING_KEY));
    }

    /**
     * Validate a candidate {@code engine_mode} attribute value. Called by {@link Node#NODE_ATTRIBUTES}'s
     * validator so the error surfaces at node startup rather than later when something tries to read it.
     *
     * @throws IllegalArgumentException if the value is not one of the known modes
     */
    public static void validate(String value) {
        parse(value);
    }
}
