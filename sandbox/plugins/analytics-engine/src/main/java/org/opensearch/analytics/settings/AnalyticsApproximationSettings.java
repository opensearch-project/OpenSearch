/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.analytics.settings;

import org.opensearch.common.settings.Setting;

import java.util.List;

/** Cluster-level settings for analytics approximation behavior. */
public final class AnalyticsApproximationSettings {

    public static final Setting<Double> SHARD_BUCKET_OVERSAMPLING_FACTOR = Setting.doubleSetting(
        "analytics.shard_bucket_oversampling_factor",
        0.0,
        0.0,
        Setting.Property.NodeScope,
        Setting.Property.Dynamic
    );

    public static List<Setting<?>> all() {
        return List.of(SHARD_BUCKET_OVERSAMPLING_FACTOR);
    }

    private AnalyticsApproximationSettings() {}
}
