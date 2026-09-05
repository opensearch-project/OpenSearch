/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.wlm.stats;

import org.opensearch.Version;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.common.xcontent.json.JsonXContent;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.test.AbstractWireSerializingTestCase;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.VersionUtils;
import org.opensearch.wlm.ResourceType;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;

import static java.util.Collections.emptyMap;

public class WorkloadGroupStatsTests extends AbstractWireSerializingTestCase<WorkloadGroupStats> {

    public void testToXContent() throws IOException {
        final Map<String, WorkloadGroupStats.WorkloadGroupStatsHolder> stats = new HashMap<>();
        final String workloadGroupId = "afakjklaj304041-afaka";
        stats.put(
            workloadGroupId,
            new WorkloadGroupStats.WorkloadGroupStatsHolder(
                123456789,
                13,
                2,
                0,
                5,
                Map.of(ResourceType.CPU, new WorkloadGroupStats.ResourceStats(0.3, 13, 2))
            )
        );
        XContentBuilder builder = JsonXContent.contentBuilder();
        WorkloadGroupStats workloadGroupStats = new WorkloadGroupStats(stats);
        builder.startObject();
        workloadGroupStats.toXContent(builder, ToXContent.EMPTY_PARAMS);
        builder.endObject();
        assertEquals(
            "{\"workload_groups\":{\"afakjklaj304041-afaka\":{\"total_completions\":123456789,\"total_rejections\":13,\"total_cancellations\":0,\"total_throttled\":5,\"cpu\":{\"current_usage\":0.3,\"cancellations\":13,\"rejections\":2}}}}",
            builder.toString()
        );
    }

    public void testThrottledIsVersionGatedAndKeepsOlderStreamsAligned() throws IOException {
        WorkloadGroupStats original = new WorkloadGroupStats(
            Map.of(
                "group-1",
                new WorkloadGroupStats.WorkloadGroupStatsHolder(
                    100,
                    13,
                    2,
                    7,
                    5,
                    Map.of(ResourceType.CPU, new WorkloadGroupStats.ResourceStats(0.3, 11, 4))
                )
            )
        );

        // A 3.9 peer exchanges total_throttled, and everything after it on the wire stays aligned.
        WorkloadGroupStats.WorkloadGroupStatsHolder current = copyInstance(original, Version.V_3_9_0).getStats().get("group-1");
        assertEquals(5, current.getThrottled());
        assertEquals(100, current.getCompletions());
        assertEquals(0.3, current.getResourceStats().get(ResourceType.CPU).getCurrentUsage(), 0.0);

        // A pre-throttling peer never writes total_throttled, so it must read back as 0 and -- the actual hazard --
        // the resourceStats map that follows it must still deserialize instead of being consumed as the throttled slot.
        WorkloadGroupStats.WorkloadGroupStatsHolder legacy = copyInstance(original, Version.V_3_8_0).getStats().get("group-1");
        assertEquals(0, legacy.getThrottled());
        assertEquals(100, legacy.getCompletions());
        assertEquals(13, legacy.getRejections());
        assertEquals(7, legacy.getCancellations());
        assertEquals(0.3, legacy.getResourceStats().get(ResourceType.CPU).getCurrentUsage(), 0.0);
        assertEquals(11, legacy.getResourceStats().get(ResourceType.CPU).getCancellations());
        assertEquals(4, legacy.getResourceStats().get(ResourceType.CPU).getRejections());
    }

    @Override
    protected Writeable.Reader<WorkloadGroupStats> instanceReader() {
        return WorkloadGroupStats::new;
    }

    @Override
    protected WorkloadGroupStats createTestInstance() {
        Map<String, WorkloadGroupStats.WorkloadGroupStatsHolder> stats = new HashMap<>();
        stats.put(
            randomAlphaOfLength(10),
            new WorkloadGroupStats.WorkloadGroupStatsHolder(
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                Map.of(
                    ResourceType.CPU,
                    new WorkloadGroupStats.ResourceStats(
                        randomDoubleBetween(0.0, 0.90, false),
                        randomNonNegativeLong(),
                        randomNonNegativeLong()
                    )
                )
            )
        );
        DiscoveryNode discoveryNode = new DiscoveryNode(
            "node",
            OpenSearchTestCase.buildNewFakeTransportAddress(),
            emptyMap(),
            DiscoveryNodeRole.BUILT_IN_ROLES,
            VersionUtils.randomCompatibleVersion(random(), Version.CURRENT)
        );
        return new WorkloadGroupStats(stats);
    }
}
