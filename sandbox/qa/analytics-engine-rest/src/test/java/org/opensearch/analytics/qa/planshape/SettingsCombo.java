/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.qa.planshape;

import java.util.Collections;
import java.util.Map;

/**
 * One named combination of plan-shape-affecting settings, loaded from the global
 * {@code planshape/combos.yaml}. Goldens reference a combo only by {@link #name()}.
 *
 * <p>A combo IS its settings — there is no knob-token indirection. {@link #clusterSettings()} are
 * applied via {@code PUT /_cluster/settings}.
 *
 * <p>{@code number_of_shards} is a REQUIRED index setting — the universal root knob (it decides
 * whether the plan splits into a coordinator stage), so every combo must declare it under
 * {@code index:}. TODAY it is the ONLY index setting the harness applies: it is read via the typed
 * {@link #numberOfShards()} and injected at the index {@code PUT}. Any OTHER key in
 * {@link #indexSettings()} is currently parsed but NOT applied — wiring the full map through
 * provisioning is future work for when a second index-scoped knob is added.
 *
 * <p>Example {@code combos.yaml}:
 * <pre>
 * combos:
 *   prod:
 *     index:
 *       number_of_shards: 2
 *     cluster:
 *       analytics.shard_bucket_oversampling_factor: 2.0
 *       search.concurrent.max_slice_count: 4
 *       datafusion.reduce.target_partitions: 4
 *   prod1s:
 *     index:
 *       number_of_shards: 1
 *     cluster: { ... }
 * defaults: [prod, prod1s]
 * </pre>
 */
public final class SettingsCombo {

    /** The required index setting every combo must declare — the universal root knob. */
    public static final String NUMBER_OF_SHARDS = "number_of_shards";

    private final String name;
    private final Map<String, Object> clusterSettings;
    private final Map<String, Object> indexSettings;

    public SettingsCombo(String name, Map<String, Object> clusterSettings, Map<String, Object> indexSettings) {
        this.name = name;
        this.clusterSettings = Collections.unmodifiableMap(clusterSettings);
        this.indexSettings = Collections.unmodifiableMap(indexSettings);
    }

    public String name() {
        return name;
    }

    /** Required shard count for this combo — the universal root knob (split vs no-split). */
    public int numberOfShards() {
        return ((Number) indexSettings.get(NUMBER_OF_SHARDS)).intValue();
    }

    /** Cluster-scope settings applied via {@code PUT /_cluster/settings} for this combo. */
    public Map<String, Object> clusterSettings() {
        return clusterSettings;
    }

    /**
     * Index-scope settings as declared under {@code index:}. NOTE: only {@code number_of_shards} is
     * applied today (via {@link #numberOfShards()}); other keys are not yet wired into provisioning.
     */
    public Map<String, Object> indexSettings() {
        return indexSettings;
    }

    @Override
    public String toString() {
        return name;
    }
}
