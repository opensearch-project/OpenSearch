/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.stats;

import org.opensearch.Version;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.test.OpenSearchTestCase;

import java.util.Set;

public class ClusterStatsRequestTests extends OpenSearchTestCase {

    public void testSerializationWithVersion3x() throws Exception {
        ClusterStatsRequest clusterStatsRequest = getClusterStatsRequest();

        Version testVersion = Version.V_3_0_0;

        BytesStreamOutput output = new BytesStreamOutput();
        output.setVersion(testVersion);
        clusterStatsRequest.writeTo(output);

        StreamInput streamInput = output.bytes().streamInput();
        streamInput.setVersion(testVersion);
        ClusterStatsRequest deserializedClusterStatsRequest = new ClusterStatsRequest(streamInput);

        validateClusterStatsRequest(
            Set.of(ClusterStatsRequest.Metric.OS, ClusterStatsRequest.Metric.PLUGINS, ClusterStatsRequest.Metric.INDICES),
            Set.of(
                ClusterStatsRequest.IndexMetric.SHARDS,
                ClusterStatsRequest.IndexMetric.QUERY_CACHE,
                ClusterStatsRequest.IndexMetric.MAPPINGS
            ),
            Version.V_3_0_0,
            deserializedClusterStatsRequest
        );
        assertEquals(-1, streamInput.read());
    }

    public void testSerializationOnVersionBelow3x() throws Exception {
        ClusterStatsRequest clusterStatsRequest = getClusterStatsRequest();

        Version testVersion = Version.V_2_17_0;

        BytesStreamOutput output = new BytesStreamOutput();
        output.setVersion(testVersion);
        clusterStatsRequest.writeTo(output);

        StreamInput streamInput = output.bytes().streamInput();
        streamInput.setVersion(testVersion);
        ClusterStatsRequest deserializedClusterStatsRequest = new ClusterStatsRequest(streamInput);

        validateClusterStatsRequest(
            Set.of(ClusterStatsRequest.Metric.OS, ClusterStatsRequest.Metric.PLUGINS, ClusterStatsRequest.Metric.INDICES),
            Set.of(
                ClusterStatsRequest.IndexMetric.SHARDS,
                ClusterStatsRequest.IndexMetric.QUERY_CACHE,
                ClusterStatsRequest.IndexMetric.MAPPINGS
            ),
            Version.V_2_17_0,
            deserializedClusterStatsRequest
        );
        assertEquals(-1, streamInput.read());
    }

    private ClusterStatsRequest getClusterStatsRequest() {
        ClusterStatsRequest clusterStatsRequest = new ClusterStatsRequest();
        clusterStatsRequest.computeAllMetrics(true);
        clusterStatsRequest.addMetric(ClusterStatsRequest.Metric.OS);
        clusterStatsRequest.addMetric(ClusterStatsRequest.Metric.PLUGINS);
        clusterStatsRequest.addMetric(ClusterStatsRequest.Metric.INDICES);
        clusterStatsRequest.addIndexMetric(ClusterStatsRequest.IndexMetric.SHARDS);
        clusterStatsRequest.addIndexMetric(ClusterStatsRequest.IndexMetric.QUERY_CACHE);
        clusterStatsRequest.addIndexMetric(ClusterStatsRequest.IndexMetric.MAPPINGS);
        clusterStatsRequest.useAggregatedNodeLevelResponses(true);
        return clusterStatsRequest;
    }

    private void validateClusterStatsRequest(
        Set<ClusterStatsRequest.Metric> metrics,
        Set<ClusterStatsRequest.IndexMetric> indexMetrics,
        Version version,
        ClusterStatsRequest deserializedClusterStatsRequest
    ) {
        if (version.before(Version.V_3_0_0)) {
            assertEquals(true, deserializedClusterStatsRequest.computeAllMetrics());
            assertTrue(deserializedClusterStatsRequest.requestedMetrics().isEmpty());
            assertTrue(deserializedClusterStatsRequest.indicesMetrics().isEmpty());
        } else {
            assertEquals(true, deserializedClusterStatsRequest.computeAllMetrics());
            assertEquals(metrics, deserializedClusterStatsRequest.requestedMetrics());
            assertEquals(indexMetrics, deserializedClusterStatsRequest.indicesMetrics());
        }
    }

}
