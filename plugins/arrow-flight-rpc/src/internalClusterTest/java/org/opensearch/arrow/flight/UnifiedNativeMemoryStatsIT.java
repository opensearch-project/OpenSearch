/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.arrow.flight;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

import org.opensearch.action.admin.cluster.node.stats.NodesStatsRequest;
import org.opensearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.opensearch.arrow.allocator.ArrowBasePlugin;
import org.opensearch.common.settings.Settings;
import org.opensearch.plugin.stats.NativeAllocatorPoolStats;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.Collection;
import java.util.List;
import java.util.Set;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Integration test verifying the unified native memory stats endpoint.
 * Boots a single-node cluster with ArrowBasePlugin and confirms that
 * all registered pools (Arrow + virtual) appear in _nodes/stats/native_memory.
 */
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, minNumDataNodes = 1, maxNumDataNodes = 1)
public class UnifiedNativeMemoryStatsIT extends OpenSearchIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return List.of(ArrowBasePlugin.class);
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder().put(super.nodeSettings(nodeOrdinal)).put("node.native_memory.limit", "1gb").build();
    }

    /**
     * Verifies that the Arrow pools (flight, ingest, query) are visible in
     * _nodes/stats/native_memory with correct structure.
     */
    public void testArrowPoolsVisibleInStats() {
        NodesStatsResponse response = client().admin()
            .cluster()
            .prepareNodesStats()
            .addMetric(NodesStatsRequest.Metric.NATIVE_MEMORY.metricName())
            .get();

        assertThat(response.getNodes().isEmpty(), is(false));
        NativeAllocatorPoolStats stats = response.getNodes().get(0).getNativeAllocatorStats();
        assertThat("native_memory stats should be present", stats, notNullValue());

        // Dump the stats for debugging
        StringBuilder sb = new StringBuilder();
        sb.append("nativeAllocated=").append(stats.getNativeAllocatedBytes());
        sb.append(", nativeResident=").append(stats.getNativeResidentBytes());
        sb.append(", pools=[");
        for (NativeAllocatorPoolStats.PoolStats p : stats.getPools()) {
            sb.append(p.getName())
                .append("(alloc=")
                .append(p.getAllocatedBytes())
                .append(",peak=")
                .append(p.getPeakBytes())
                .append(",limit=")
                .append(p.getLimitBytes())
                .append(") ");
        }
        sb.append("]");
        logger.info("=== NATIVE_MEMORY STATS: {} ===", sb);

        // All Arrow pools should be present
        Set<String> poolNames = stats.getPools().stream().map(NativeAllocatorPoolStats.PoolStats::getName).collect(Collectors.toSet());
        assertThat(poolNames, hasItems("flight", "ingest", "query"));

        // Each pool should have limit > 0 (derived from 1gb native_memory.limit)
        for (NativeAllocatorPoolStats.PoolStats pool : stats.getPools()) {
            assertThat("Pool '" + pool.getName() + "' should have limit > 0", pool.getLimitBytes(), greaterThan(0L));
            assertThat("Pool '" + pool.getName() + "' allocated should be >= 0", pool.getAllocatedBytes(), greaterThanOrEqualTo(0L));
        }
    }

}
