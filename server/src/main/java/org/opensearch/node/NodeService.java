/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/*
 * Licensed to Elasticsearch under one or more contributor
 * license agreements. See the NOTICE file distributed with
 * this work for additional information regarding copyright
 * ownership. Elasticsearch licenses this file to you under
 * the Apache License, Version 2.0 (the "License"); you may
 * not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */

/*
 * Modifications Copyright OpenSearch Contributors. See
 * GitHub history for details.
 */

package org.opensearch.node;

import org.opensearch.cluster.routing.WeightedRoutingStats;
import org.opensearch.core.internal.io.IOUtils;
import org.opensearch.Build;
import org.opensearch.Version;
import org.opensearch.action.admin.cluster.node.info.NodeInfo;
import org.opensearch.action.admin.cluster.node.stats.NodeStats;
import org.opensearch.action.admin.indices.stats.CommonStatsFlags;
import org.opensearch.action.search.SearchTransportService;
import org.opensearch.cluster.service.ClusterService;
import org.opensearch.common.Nullable;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.settings.SettingsFilter;
import org.opensearch.discovery.Discovery;
import org.opensearch.env.NodeEnvironment;
import org.opensearch.http.HttpServerTransport;
import org.opensearch.index.IndexingPressureService;
import org.opensearch.indices.IndicesService;
import org.opensearch.indices.breaker.CircuitBreakerService;
import org.opensearch.ingest.IngestService;
import org.opensearch.monitor.MonitorService;
import org.opensearch.plugins.PluginsService;
import org.opensearch.script.ScriptService;
import org.opensearch.search.aggregations.support.AggregationUsageService;
import org.opensearch.search.backpressure.SearchBackpressureService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.TransportService;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Services exposed to nodes
 *
 * @opensearch.internal
 */
public class NodeService implements Closeable {
    private final Settings settings;
    private final ThreadPool threadPool;
    private final MonitorService monitorService;
    private final TransportService transportService;
    private final IndicesService indicesService;
    private final PluginsService pluginService;
    private final CircuitBreakerService circuitBreakerService;
    private final IngestService ingestService;
    private final SettingsFilter settingsFilter;
    private final ScriptService scriptService;
    private final HttpServerTransport httpServerTransport;
    private final ResponseCollectorService responseCollectorService;
    private final SearchTransportService searchTransportService;
    private final IndexingPressureService indexingPressureService;
    private final AggregationUsageService aggregationUsageService;
    private final SearchBackpressureService searchBackpressureService;
    private final ClusterService clusterService;
    private final Discovery discovery;
    private final NodeEnvironment nodeEnvironment;

    NodeService(
        Settings settings,
        ThreadPool threadPool,
        MonitorService monitorService,
        Discovery discovery,
        TransportService transportService,
        IndicesService indicesService,
        PluginsService pluginService,
        CircuitBreakerService circuitBreakerService,
        ScriptService scriptService,
        @Nullable HttpServerTransport httpServerTransport,
        IngestService ingestService,
        ClusterService clusterService,
        SettingsFilter settingsFilter,
        ResponseCollectorService responseCollectorService,
        SearchTransportService searchTransportService,
        IndexingPressureService indexingPressureService,
        AggregationUsageService aggregationUsageService,
        SearchBackpressureService searchBackpressureService,
        NodeEnvironment nodeEnvironment
    ) {
        this.settings = settings;
        this.threadPool = threadPool;
        this.monitorService = monitorService;
        this.transportService = transportService;
        this.indicesService = indicesService;
        this.discovery = discovery;
        this.pluginService = pluginService;
        this.circuitBreakerService = circuitBreakerService;
        this.httpServerTransport = httpServerTransport;
        this.ingestService = ingestService;
        this.settingsFilter = settingsFilter;
        this.scriptService = scriptService;
        this.responseCollectorService = responseCollectorService;
        this.searchTransportService = searchTransportService;
        this.indexingPressureService = indexingPressureService;
        this.aggregationUsageService = aggregationUsageService;
        this.searchBackpressureService = searchBackpressureService;
        this.clusterService = clusterService;
        this.nodeEnvironment = nodeEnvironment;
        clusterService.addStateApplier(ingestService);
    }

    public NodeInfo info(
        boolean settings,
        boolean os,
        boolean process,
        boolean jvm,
        boolean threadPool,
        boolean transport,
        boolean http,
        boolean plugin,
        boolean ingest,
        boolean aggs,
        boolean indices
    ) {
        return new NodeInfo(
            Version.CURRENT,
            Build.CURRENT,
            transportService.getLocalNode(),
            settings ? settingsFilter.filter(this.settings) : null,
            os ? monitorService.osService().info() : null,
            process ? monitorService.processService().info() : null,
            jvm ? monitorService.jvmService().info() : null,
            threadPool ? this.threadPool.info() : null,
            transport ? transportService.info() : null,
            http ? (httpServerTransport == null ? null : httpServerTransport.info()) : null,
            plugin ? (pluginService == null ? null : pluginService.info()) : null,
            ingest ? (ingestService == null ? null : ingestService.info()) : null,
            aggs ? (aggregationUsageService == null ? null : aggregationUsageService.info()) : null,
            indices ? indicesService.getTotalIndexingBufferBytes() : null
        );
    }

    public NodeStats stats(
        CommonStatsFlags indices,
        boolean os,
        boolean process,
        boolean jvm,
        boolean threadPool,
        boolean fs,
        boolean transport,
        boolean http,
        boolean circuitBreaker,
        boolean script,
        boolean discoveryStats,
        boolean ingest,
        boolean adaptiveSelection,
        boolean scriptCache,
        boolean indexingPressure,
        boolean shardIndexingPressure,
        boolean searchBackpressure,
        boolean clusterManagerThrottling,
        boolean weightedRoutingStats,
        boolean fileCacheStats
    ) {
        // for indices stats we want to include previous allocated shards stats as well (it will
        // only be applied to the sensible ones to use, like refresh/merge/flush/indexing stats)
        return new NodeStats(
            transportService.getLocalNode(),
            System.currentTimeMillis(),
            indices.anySet() ? indicesService.stats(indices) : null,
            os ? monitorService.osService().stats() : null,
            process ? monitorService.processService().stats() : null,
            jvm ? monitorService.jvmService().stats() : null,
            threadPool ? this.threadPool.stats() : null,
            fs ? monitorService.fsService().stats() : null,
            transport ? transportService.stats() : null,
            http ? (httpServerTransport == null ? null : httpServerTransport.stats()) : null,
            circuitBreaker ? circuitBreakerService.stats() : null,
            script ? scriptService.stats() : null,
            discoveryStats ? discovery.stats() : null,
            ingest ? ingestService.stats() : null,
            adaptiveSelection ? responseCollectorService.getAdaptiveStats(searchTransportService.getPendingSearchRequests()) : null,
            scriptCache ? scriptService.cacheStats() : null,
            indexingPressure ? this.indexingPressureService.nodeStats() : null,
            shardIndexingPressure ? this.indexingPressureService.shardStats(indices) : null,
            searchBackpressure ? this.searchBackpressureService.nodeStats() : null,
            clusterManagerThrottling ? this.clusterService.getClusterManagerService().getThrottlingStats() : null,
            weightedRoutingStats ? WeightedRoutingStats.getInstance() : null,
            fileCacheStats ? nodeEnvironment.fileCacheStats() : null
        );
    }

    public IngestService getIngestService() {
        return ingestService;
    }

    public MonitorService getMonitorService() {
        return monitorService;
    }

    public SearchBackpressureService getSearchBackpressureService() {
        return searchBackpressureService;
    }

    @Override
    public void close() throws IOException {
        IOUtils.close(indicesService);
    }

    /**
     * Wait for the node to be effectively closed.
     * @see IndicesService#awaitClose(long, TimeUnit)
     */
    public boolean awaitClose(long timeout, TimeUnit timeUnit) throws InterruptedException {
        return indicesService.awaitClose(timeout, timeUnit);
    }

}
