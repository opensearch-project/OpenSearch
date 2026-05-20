/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.cluster.wlm;

import org.opensearch.Version;
import org.opensearch.action.FailedNodeException;
import org.opensearch.cluster.ClusterName;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.wlm.ResourceType;
import org.opensearch.wlm.stats.WlmStats;
import org.opensearch.wlm.stats.WorkloadGroupStats;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

public class WlmStatsResponseTests extends OpenSearchTestCase {
    ClusterName clusterName = new ClusterName("test-cluster");
    String testWorkloadGroupId = "safjgagnaeekg-3r3fads";
    DiscoveryNode node = new DiscoveryNode(
        "node-1",
        buildNewFakeTransportAddress(),
        new HashMap<>(),
        Set.of(DiscoveryNodeRole.DATA_ROLE),
        Version.CURRENT
    );
    Map<String, WorkloadGroupStats.WorkloadGroupStatsHolder> statsHolderMap = new HashMap<>();
    WorkloadGroupStats workloadGroupStats = new WorkloadGroupStats(
        Map.of(
            testWorkloadGroupId,
            new WorkloadGroupStats.WorkloadGroupStatsHolder(
                0,
                0,
                1,
                0,
                Map.of(
                    ResourceType.CPU,
                    new WorkloadGroupStats.ResourceStats(0, 0, 0),
                    ResourceType.MEMORY,
                    new WorkloadGroupStats.ResourceStats(0, 0, 0)
                )
            )
        )
    );
    WlmStats wlmStats = new WlmStats(node, workloadGroupStats);
    List<WlmStats> wlmStatsList = List.of(wlmStats);
    List<FailedNodeException> failedNodeExceptionList = new ArrayList<>();

    public void testSerializationAndDeserialization() throws IOException {
        WlmStatsResponse workloadGroupStatsResponse = new WlmStatsResponse(clusterName, wlmStatsList, failedNodeExceptionList);
        BytesStreamOutput out = new BytesStreamOutput();
        workloadGroupStatsResponse.writeTo(out);
        StreamInput in = out.bytes().streamInput();
        WlmStatsResponse deserializedResponse = new WlmStatsResponse(in);
        assertEquals(workloadGroupStatsResponse.getClusterName(), deserializedResponse.getClusterName());
        assertEquals(workloadGroupStatsResponse.getNodes().size(), deserializedResponse.getNodes().size());
    }

    public void testToString() {
        WlmStatsResponse workloadGroupStatsResponse = new WlmStatsResponse(clusterName, wlmStatsList, failedNodeExceptionList);
        String responseString = workloadGroupStatsResponse.toString();
        assertEquals(
            "{\n"
                + "  \"node-1\" : {\n"
                + "    \"workload_groups\" : {\n"
                + "      \"safjgagnaeekg-3r3fads\" : {\n"
                + "        \"total_completions\" : 0,\n"
                + "        \"total_rejections\" : 0,\n"
                + "        \"total_cancellations\" : 0,\n"
                + "        \"cpu\" : {\n"
                + "          \"current_usage\" : 0.0,\n"
                + "          \"cancellations\" : 0,\n"
                + "          \"rejections\" : 0\n"
                + "        },\n"
                + "        \"memory\" : {\n"
                + "          \"current_usage\" : 0.0,\n"
                + "          \"cancellations\" : 0,\n"
                + "          \"rejections\" : 0\n"
                + "        }\n"
                + "      }\n"
                + "    }\n"
                + "  }\n"
                + "}",
            responseString
        );
    }
}
