/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.upgrades;

import org.opensearch.client.Request;
import org.opensearch.client.Response;

import java.io.IOException;
import java.util.Map;

/**
 * Rolling upgrade test that verifies cluster manager task throttling settings
 * persisted in cluster state survive a rolling upgrade without causing node
 * startup failures.
 * <p>
 * This test guards against the scenario where removing
 * registerClusterManagerTask() calls for task keys that are persisted in cluster
 * metadata caused fatal IllegalArgumentException during cluster state restoration.
 * <p>
 * The test sets throttling thresholds in the old cluster and verifies they are
 * still present and valid after rolling upgrade to the new version.
 */
public class ClusterManagerThrottlingSettingsIT extends AbstractRollingTestCase {

    public void testThrottlingSettingsSurviveRollingUpgrade() throws Exception {
        if (CLUSTER_TYPE == ClusterType.OLD) {
            // On the old cluster, persist throttling settings for several task types
            // These will be written into cluster state metadata
            setThrottlingSetting("put-mapping", 5000);
            setThrottlingSetting("create-index", 100);

            // Verify the settings were persisted
            verifyThrottlingSettingsInClusterState("put-mapping", 5000);
            verifyThrottlingSettingsInClusterState("create-index", 100);
        } else {
            // After upgrade (mixed or fully upgraded), verify the persisted settings
            // are still valid and the cluster is healthy (no node startup failures)
            verifyThrottlingSettingsInClusterState("put-mapping", 5000);
            verifyThrottlingSettingsInClusterState("create-index", 100);

            // Also verify cluster health — if settings restoration failed, nodes would
            // have crashed and the cluster would not be green/yellow
            verifyClusterHealth();
        }
    }

    private void setThrottlingSetting(String taskKey, int threshold) throws IOException {
        Request request = new Request("PUT", "_cluster/settings");
        request.setJsonEntity(
            "{\"persistent\": {\"cluster_manager.throttling.thresholds." + taskKey + ".value\": " + threshold + "}}"
        );
        Response response = client().performRequest(request);
        assertEquals(200, response.getStatusLine().getStatusCode());
    }

    @SuppressWarnings("unchecked")
    private void verifyThrottlingSettingsInClusterState(String taskKey, int expectedValue) throws IOException {
        Request request = new Request("GET", "_cluster/settings");
        Response response = client().performRequest(request);
        assertEquals(200, response.getStatusLine().getStatusCode());
        Map<String, Object> responseMap = entityAsMap(response);

        Map<String, Object> persistent = (Map<String, Object>) responseMap.get("persistent");
        assertNotNull("Persistent settings should not be null", persistent);

        // Navigate the nested structure: cluster_manager.throttling.thresholds.<key>.value
        Map<String, Object> clusterManager = (Map<String, Object>) persistent.get("cluster_manager");
        assertNotNull("cluster_manager settings should exist in persistent settings", clusterManager);

        Map<String, Object> throttling = (Map<String, Object>) clusterManager.get("throttling");
        assertNotNull("cluster_manager.throttling settings should exist", throttling);

        Map<String, Object> thresholds = (Map<String, Object>) throttling.get("thresholds");
        assertNotNull("cluster_manager.throttling.thresholds should exist", thresholds);

        Map<String, Object> taskSettings = (Map<String, Object>) thresholds.get(taskKey);
        assertNotNull(
            "Throttling settings for task key '" + taskKey + "' should still be persisted after upgrade. "
                + "If this is null, the task key may have been removed from the ClusterManagerTask enum "
                + "or its registration was removed, causing settings validation to fail during upgrade.",
            taskSettings
        );

        String value = (String) taskSettings.get("value");
        assertEquals(
            "Throttling threshold for '" + taskKey + "' should match the value set in old cluster",
            String.valueOf(expectedValue),
            value
        );
    }

    private void verifyClusterHealth() throws IOException {
        Request request = new Request("GET", "_cluster/health");
        Response response = client().performRequest(request);
        assertEquals(200, response.getStatusLine().getStatusCode());
        Map<String, Object> health = entityAsMap(response);
        String status = (String) health.get("status");
        // During rolling upgrade, yellow is acceptable (replicas may be temporarily unassigned)
        assertTrue(
            "Cluster health should be green or yellow but was: " + status
                + ". A red status after upgrade may indicate node startup failures "
                + "caused by cluster state settings restoration errors.",
            "green".equals(status) || "yellow".equals(status)
        );
    }
}
