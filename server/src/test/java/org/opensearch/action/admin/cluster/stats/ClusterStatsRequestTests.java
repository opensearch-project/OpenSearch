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
import org.opensearch.test.VersionUtils;

public class ClusterStatsRequestTests extends OpenSearchTestCase {

    public void testSerialization() throws Exception {
        ClusterStatsRequest clusterStatsRequest = new ClusterStatsRequest();
        clusterStatsRequest.computeAllMetrics(randomBoolean());
        clusterStatsRequest.addMetric(ClusterStatsRequest.Metric.OS);
        clusterStatsRequest.addMetric(ClusterStatsRequest.Metric.PLUGINS);
        clusterStatsRequest.addMetric(ClusterStatsRequest.Metric.INDICES);
        clusterStatsRequest.addIndexMetric(ClusterStatsRequest.IndexMetric.SHARDS);
        clusterStatsRequest.addIndexMetric(ClusterStatsRequest.IndexMetric.QUERY_CACHE);
        clusterStatsRequest.addIndexMetric(ClusterStatsRequest.IndexMetric.MAPPINGS);
        clusterStatsRequest.useAggregatedNodeLevelResponses(randomBoolean());

        Version testVersion = VersionUtils.randomVersionBetween(random(), Version.CURRENT.minimumCompatibilityVersion(), Version.CURRENT);

        BytesStreamOutput output = new BytesStreamOutput();
        output.setVersion(testVersion);
        clusterStatsRequest.writeTo(output);

        StreamInput streamInput = output.bytes().streamInput();
        streamInput.setVersion(testVersion);
        ClusterStatsRequest deserializedClusterStatsRequest = new ClusterStatsRequest(streamInput);

        assertEquals(clusterStatsRequest.computeAllMetrics(), deserializedClusterStatsRequest.computeAllMetrics());
        assertEquals(clusterStatsRequest.requestedMetrics(), deserializedClusterStatsRequest.requestedMetrics());
        assertEquals(clusterStatsRequest.indicesMetrics(), deserializedClusterStatsRequest.indicesMetrics());
        assertEquals(
            clusterStatsRequest.useAggregatedNodeLevelResponses(),
            deserializedClusterStatsRequest.useAggregatedNodeLevelResponses()
        );
    }

}
