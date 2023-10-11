/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices.recovery;

import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;

/**
 * Utility to provide a {@link RecoverySettings} instance containing all defaults
 */
public final class DefaultRecoverySettings {
    private DefaultRecoverySettings() {}

    public static final RecoverySettings INSTANCE = new RecoverySettings(
        Settings.EMPTY,
        new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
    );
}
