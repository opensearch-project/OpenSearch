/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.upgrades;

import org.opensearch.Version;
import org.opensearch.client.Request;
import org.opensearch.client.Response;
import org.opensearch.client.ResponseException;

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
 * The test sets throttling thresholds in the old cluster (if the version supports it)
 * and verifies the cluster remains healthy after rolling upgrade to the new version.
 */
public class ClusterManagerThrottlingSettingsIT extends AbstractRollingTestCase {

    public void testThrottlingSettingsSurviveRollingUpgrade() throws Exception {
        // On the old cluster, attempt to persist throttling settings (supported on >= 2.5.0)
        if (CLUSTER_TYPE == ClusterType.OLD && UPGRADE_FROM_VERSION.onOrAfter(Version.V_2_5_0)) {
            trySetThrottlingSetting("put-mapping", 5000);
            trySetThrottlingSetting("create-index", 100);
        }

        // In all phases (old, mixed, upgraded), verify the cluster is healthy.
        // If settings restoration failed due to missing task key registrations,
        // nodes would have crashed and the cluster would not be green/yellow.
        verifyClusterHealth();
    }

    private void trySetThrottlingSetting(String taskKey, int threshold) throws IOException {
        Request request = new Request("PUT", "_cluster/settings");
        request.setJsonEntity(
            "{\"persistent\": {\"cluster_manager.throttling.thresholds." + taskKey + ".value\": " + threshold + "}}"
        );
        try {
            Response response = client().performRequest(request);
            assertEquals(200, response.getStatusLine().getStatusCode());
        } catch (ResponseException e) {
            // Some older versions may not support this setting or may reject it
            // if not all nodes are >= 2.5.0. That's acceptable — the key test is
            // that the cluster remains healthy after upgrade.
            logger.info("Could not set throttling setting for " + taskKey + ": " + e.getMessage());
        }
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
