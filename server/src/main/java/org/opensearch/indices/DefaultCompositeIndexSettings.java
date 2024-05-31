/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.index.compositeindex.CompositeIndexSettings;

/**
 * Utility to provide a {@link CompositeIndexSettings} instance containing all defaults
 *
 * @opensearch.experimental
 */
@ExperimentalApi
public final class DefaultCompositeIndexSettings {
    private DefaultCompositeIndexSettings() {}

    public static final CompositeIndexSettings INSTANCE = new CompositeIndexSettings(
        new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
    );
}
