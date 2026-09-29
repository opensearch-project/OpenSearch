/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.snapshots;

import org.opensearch.action.admin.cluster.settings.ClusterUpdateSettingsResponse;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.util.FeatureFlags;
import org.opensearch.test.OpenSearchIntegTestCase;

import static org.hamcrest.Matchers.containsString;

@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, numDataNodes = 0)
public class SnapshotResilienceSettingsIT extends OpenSearchIntegTestCase {

    @Override
    protected Settings featureFlagSettings() {
        return Settings.builder().put(FeatureFlags.SNAPSHOT_RESILIENCE, true).build();
    }

    public void testIoTimeoutDynamicUpdateIsAcknowledgedAndReflectedInState() {
        internalCluster().startNode();
        ClusterUpdateSettingsResponse response = client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setTransientSettings(Settings.builder().put("snapshot.repository.io_timeout", "10m").build())
            .get();
        assertTrue(response.isAcknowledged());

        client().admin()
            .cluster()
            .prepareUpdateSettings()
            .setTransientSettings(Settings.builder().put("snapshot.repository.io_timeout", "20m").build())
            .get();

        Settings settings = client().admin().cluster().prepareState().get().getState().metadata().transientSettings();
        assertEquals("20m", settings.get("snapshot.repository.io_timeout"));
    }

    public void testIoTimeoutRejectsInvalidValue() {
        internalCluster().startNode();
        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setTransientSettings(Settings.builder().put("snapshot.repository.io_timeout", "0s").build())
                .get()
        );
        assertThat(e.getMessage(), containsString("snapshot.repository.io_timeout"));
    }

    public void testSettingsRejectedWhenFlagDisabled() {
        // Start a node with the flag explicitly disabled
        internalCluster().startNode(Settings.builder().put(FeatureFlags.SNAPSHOT_RESILIENCE, false).build());

        IllegalArgumentException e = expectThrows(
            IllegalArgumentException.class,
            () -> client().admin()
                .cluster()
                .prepareUpdateSettings()
                .setTransientSettings(Settings.builder().put("snapshot.repository.io_timeout", "10m").build())
                .get()
        );
        assertThat(e.getMessage(), containsString("feature flag"));
        assertThat(e.getMessage(), containsString("disabled"));
    }
}
