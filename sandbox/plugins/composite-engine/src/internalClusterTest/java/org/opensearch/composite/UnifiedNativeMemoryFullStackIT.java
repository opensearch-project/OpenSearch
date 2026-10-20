/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.composite;

import com.carrotsearch.randomizedtesting.annotations.ThreadLeakScope;

import org.opensearch.action.admin.cluster.node.stats.NodesStatsRequest;
import org.opensearch.action.admin.cluster.node.stats.NodesStatsResponse;
import org.opensearch.arrow.allocator.ArrowBasePlugin;
import org.opensearch.be.datafusion.DataFusionPlugin;
import org.opensearch.be.lucene.LucenePlugin;
import org.opensearch.common.settings.Settings;
import org.opensearch.parquet.ParquetDataFormatPlugin;
import org.opensearch.plugin.stats.NativeAllocatorPoolStats;
import org.opensearch.plugins.Plugin;
import org.opensearch.test.OpenSearchIntegTestCase;

import java.util.Arrays;
import java.util.Collection;
import java.util.Set;
import java.util.stream.Collectors;

import static org.hamcrest.Matchers.greaterThan;
import static org.hamcrest.Matchers.greaterThanOrEqualTo;
import static org.hamcrest.Matchers.hasItems;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.notNullValue;

/**
 * Full-stack IT verifying all 6 pools appear in _nodes/stats/native_memory.
 */
@ThreadLeakScope(ThreadLeakScope.Scope.NONE)
@OpenSearchIntegTestCase.ClusterScope(scope = OpenSearchIntegTestCase.Scope.TEST, minNumDataNodes = 1, maxNumDataNodes = 1)
public class UnifiedNativeMemoryFullStackIT extends OpenSearchIntegTestCase {

    @Override
    protected Collection<Class<? extends Plugin>> nodePlugins() {
        return Arrays.asList(
            ArrowBasePlugin.class,
            ParquetDataFormatPlugin.class,
            CompositeDataFormatPlugin.class,
            LucenePlugin.class,
            DataFusionPlugin.class
        );
    }

    @Override
    protected Settings nodeSettings(int nodeOrdinal) {
        return Settings.builder().put(super.nodeSettings(nodeOrdinal)).put("node.native_memory.limit", "2gb").build();
    }

    public void testAllSixPoolsVisibleInStats() {
        NodesStatsResponse response = client().admin()
            .cluster()
            .prepareNodesStats()
            .addMetric(NodesStatsRequest.Metric.NATIVE_MEMORY.metricName())
            .get();

        assertThat(response.getNodes().isEmpty(), is(false));
        NativeAllocatorPoolStats stats = response.getNodes().get(0).getNativeAllocatorStats();
        assertThat("native_memory stats should be present", stats, notNullValue());

        // All 6 pools should be present
        Set<String> poolNames = stats.getPools().stream().map(NativeAllocatorPoolStats.PoolStats::getName).collect(Collectors.toSet());
        assertThat(poolNames, hasItems("flight", "ingest", "query", "datafusion", "write", "merge"));

        // Each pool should have limit > 0
        for (NativeAllocatorPoolStats.PoolStats pool : stats.getPools()) {
            assertThat("Pool '" + pool.getName() + "' limit should be > 0", pool.getLimitBytes(), greaterThan(0L));
            assertThat("Pool '" + pool.getName() + "' allocated should be >= 0", pool.getAllocatedBytes(), greaterThanOrEqualTo(0L));
        }

        // Native memory stats (jemalloc) should be available since DataFusion plugin sets the supplier
        assertThat("native allocated_bytes should be > 0", stats.getNativeAllocatedBytes(), greaterThan(0L));
        assertThat("native resident_bytes should be > 0", stats.getNativeResidentBytes(), greaterThan(0L));
    }

}
