/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.indices;

import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;

/**
 * Utility to provide a {@link RemoteStoreSettings} instance containing all defaults
 *
 * @opensearch.internal
 */
public final class DefaultRemoteStoreSettings {
    private DefaultRemoteStoreSettings() {}

    public static final RemoteStoreSettings INSTANCE = new RemoteStoreSettings(
        Settings.EMPTY,
        new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS)
    );
}
