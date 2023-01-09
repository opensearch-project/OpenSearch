/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.awarenesshealth;

import org.hamcrest.Matchers;
import org.opensearch.cluster.health.ClusterHealthStatus;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.io.stream.StreamInput;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.test.OpenSearchTestCase;

import java.io.IOException;
import java.util.Collections;

public class ClusterAwarenessHealthResponseTests extends OpenSearchTestCase {

    public void testClusterAwarenessHealth() throws IOException {
        int activeShards = randomIntBetween(0, 200);
        int unassignedShards = randomIntBetween(0, 100);
        int initializingShards = randomIntBetween(0, 200);
        int relocatingShards = randomIntBetween(0, 200);
        int numberOfDataNodes = randomIntBetween(1, 10);
        int numberOfNodes = randomIntBetween(numberOfDataNodes, numberOfDataNodes + 10);
        int pendingTasks = randomIntBetween(1, 20);
        int inFlight = randomIntBetween(1, 20);
        int activePrimaryShards = randomIntBetween(1, 20);
        int delayedUnassignedShards = randomIntBetween(1, 20);
        double activeShardsPercent = randomDouble();
        boolean hasDiscoveredClusterManager = randomBoolean();
        TimeValue maxWaitTime = TimeValue.timeValueMillis(10);
        String awarenessAttribute = "zone";

        ClusterAwarenessHealthResponse clusterAwarenessHealthResponse = new ClusterAwarenessHealthResponse(
            "bla",
            activeShards,
            unassignedShards,
            initializingShards,
            relocatingShards,
            numberOfDataNodes,
            numberOfNodes,
            pendingTasks,
            inFlight,
            maxWaitTime,
            activePrimaryShards,
            activeShardsPercent,
            delayedUnassignedShards,
            hasDiscoveredClusterManager,
            awarenessAttribute,
            ClusterHealthStatus.GREEN,
            Collections.emptyMap()
        );
        clusterAwarenessHealthResponse = serializeResponse(clusterAwarenessHealthResponse);
        assertThat(clusterAwarenessHealthResponse.getActiveShards(), Matchers.equalTo(activeShards));
        assertThat(clusterAwarenessHealthResponse.getRelocatingShards(), Matchers.equalTo(relocatingShards));
        assertThat(clusterAwarenessHealthResponse.getInitializingShards(), Matchers.equalTo(initializingShards));
        assertThat(clusterAwarenessHealthResponse.getUnassignedShards(), Matchers.equalTo(unassignedShards));
        assertThat(clusterAwarenessHealthResponse.getClusterName(), Matchers.equalTo("bla"));
        assertThat(clusterAwarenessHealthResponse.getNumberOfDataNodes(), Matchers.equalTo(numberOfDataNodes));
        assertThat(clusterAwarenessHealthResponse.getNumberOfNodes(), Matchers.equalTo(numberOfNodes));
        assertThat(clusterAwarenessHealthResponse.getNumberOfPendingTasks(), Matchers.equalTo(pendingTasks));
        assertThat(clusterAwarenessHealthResponse.getNumberOfInflightFetches(), Matchers.equalTo(inFlight));
        assertThat(clusterAwarenessHealthResponse.getDelayedUnassignedShards(), Matchers.equalTo(delayedUnassignedShards));
    }

    ClusterAwarenessHealthResponse serializeResponse(ClusterAwarenessHealthResponse clusterAwarenessHealthResponse) throws IOException {
        if (randomBoolean()) {
            BytesStreamOutput out = new BytesStreamOutput();
            clusterAwarenessHealthResponse.writeTo(out);
            StreamInput in = out.bytes().streamInput();
            clusterAwarenessHealthResponse = ClusterAwarenessHealthResponse.readResponse(in);
        }
        return clusterAwarenessHealthResponse;
    }
}
