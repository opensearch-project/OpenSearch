/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa.planshape;

import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;

/**
 * The expected plans for ONE query, parsed from its {@code q{N}.plan.yaml} golden. Holds the query
 * text and, per applicable {@link SettingsCombo combo}, the expected plan text for each
 * {@link PlanShapeLayer layer}.
 *
 * <p>A layer entry is either literal plan text or a {@code same_as: <combo>} reference (one hop, no
 * recursion), resolved at load time. A layer is uniformly an {@link Optional}: present = expected
 * text, empty = the layer is expected ABSENT under that combo (e.g. single-shard has no coordinator
 * stage, or a Lucene fast-path stage produces no DataFusion physical plan).
 */
public final class ExpectedQueryPlan {

    /**
     * The plan layers asserted per (query, combo), and their YAML keys under a combo's plans block.
     * Physical layers are backend-agnostic: a stage answered by Lucene yields an empty physical
     * layer (no DF plan). {@code chosen_backend} / {@code tree_shape} are stage properties carried
     * in the {@link #FRAGMENT} layer's per-stage text.
     */
    public enum PlanShapeLayer {
        /** Whole post-CBO Calcite plan, before the DAG cut: {@code profile.full_plan}. */
        POST_CBO("post_cbo"),
        /** Per-stage Calcite slice from the DAG cut + chosen_backend + tree_shape: {@code profile.stages[*]}. */
        FRAGMENT("fragment"),
        /** Shard physical plan: {@code stages[SHARD_FRAGMENT].tasks[*].physical_plan}; empty for a Lucene stage. */
        SHARD_PHYSICAL("shard_physical"),
        /** Coordinator physical plan: {@code stages[COORDINATOR_REDUCE].tasks[*].physical_plan}; empty if no coord stage. */
        COORD_PHYSICAL("coord_physical");

        private final String yamlKey;

        PlanShapeLayer(String yamlKey) {
            this.yamlKey = yamlKey;
        }

        public String yamlKey() {
            return yamlKey;
        }
    }

    private final String queryId;
    private final String queryText;
    private final List<String> appliesCombos;
    // comboName -> (layer yamlKey -> resolved plan text). A missing key means the layer is absent.
    private final Map<String, Map<String, String>> plansByCombo;

    public ExpectedQueryPlan(
        String queryId,
        String queryText,
        List<String> appliesCombos,
        Map<String, Map<String, String>> plansByCombo
    ) {
        this.queryId = queryId;
        this.queryText = queryText;
        this.appliesCombos = appliesCombos;
        this.plansByCombo = plansByCombo;
    }

    public String queryId() {
        return queryId;
    }

    /** The PPL/SQL query text (resolved from inline {@code ppl} or referenced {@code ppl_file}). */
    public String queryText() {
        return queryText;
    }

    /** Combo names this query is asserted under. */
    public List<String> appliesCombos() {
        return appliesCombos;
    }

    /**
     * Expected plan text for a (combo, layer), or {@link Optional#empty()} if the layer is expected
     * absent under that combo. {@code same_as} references are already resolved at load time.
     */
    public Optional<String> expected(String comboName, PlanShapeLayer layer) {
        return expectedTextForLayerKey(comboName, layer.yamlKey());
    }

    /**
     * Expected plan text for a (combo, layer key), or {@link Optional#empty()} if absent. The layer
     * key is the golden's YAML key for a {@link PlanShapeLayer} ({@code post_cbo} / {@code fragment} /
     * {@code shard_physical} / {@code coord_physical}), or a shard segment-layout sub-key
     * ({@code shard_physical_1seg} / {@code shard_physical_nseg}) that exists only when the single-
     * and multi-segment shard plans diverge.
     */
    public Optional<String> expectedTextForLayerKey(String comboName, String layerKey) {
        Map<String, String> textByLayerKey = plansByCombo.get(comboName);
        if (textByLayerKey == null) {
            throw new IllegalArgumentException(
                String.format(Locale.ROOT, "query '%s' has no plans for combo '%s'", queryId, comboName)
            );
        }
        return Optional.ofNullable(textByLayerKey.get(layerKey));
    }
}
