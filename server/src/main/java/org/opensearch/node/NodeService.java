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
import org.opensearch.common.util.io.IOUtils;
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
import org.opensearch.http.HttpServerTransport;
import org.opensearch.index.IndexingPressureService;
import org.opensearch.index.store.remote.filecache.FileCache;
import org.opensearch.indices.IndicesService;
import org.opensearch.core.indices.breaker.CircuitBreakerService;
import org.opensearch.ingest.IngestService;
import org.opensearch.monitor.MonitorService;
import org.opensearch.plugins.PluginsService;
import org.opensearch.script.ScriptService;
import org.opensearch.search.aggregations.support.AggregationUsageService;
import org.opensearch.search.backpressure.SearchBackpressureService;
import org.opensearch.search.pipeline.SearchPipelineService;
import org.opensearch.tasks.TaskCancellationMonitoringService;
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
    private final SearchPipelineService searchPipelineService;
    private final ClusterService clusterService;
    private final Discovery discovery;
    private final FileCache fileCache;
    private final TaskCancellationMonitoringService taskCancellationMonitoringService;

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
        SearchPipelineService searchPipelineService,
        FileCache fileCache,
        TaskCancellationMonitoringService taskCancellationMonitoringService
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
        this.searchPipelineService = searchPipelineService;
        this.clusterService = clusterService;
        this.fileCache = fileCache;
        this.taskCancellationMonitoringService = taskCancellationMonitoringService;
        clusterService.addStateApplier(ingestService);
        clusterService.addStateApplier(searchPipelineService);
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
        boolean indices,
        boolean searchPipeline
    ) {
        NodeInfo.Builder builder = NodeInfo.builder(Version.CURRENT, Build.CURRENT, transportService.getLocalNode());
        if (settings) {
            builder.setSettings(settingsFilter.filter(this.settings));
        }
        if (os) {
            builder.setOs(monitorService.osService().info());
        }
        if (process) {
            builder.setProcess(monitorService.processService().info());
        }
        if (jvm) {
            builder.setJvm(monitorService.jvmService().info());
        }
        if (threadPool) {
            builder.setThreadPool(this.threadPool.info());
        }
        if (transport) {
            builder.setTransport(transportService.info());
        }
        if (http && httpServerTransport != null) {
            builder.setHttp(httpServerTransport.info());
        }
        if (plugin && pluginService != null) {
            builder.setPlugins(pluginService.info());
        }
        if (ingest && ingestService != null) {
            builder.setIngest(ingestService.info());
        }
        if (aggs && aggregationUsageService != null) {
            builder.setAggsInfo(aggregationUsageService.info());
        }
        if (indices) {
            builder.setTotalIndexingBuffer(indicesService.getTotalIndexingBufferBytes());
        }
        if (searchPipeline && searchPipelineService != null) {
            builder.setSearchPipelineInfo(searchPipelineService.info());
        }
        return builder.build();
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
        boolean fileCacheStats,
        boolean taskCancellation,
        boolean searchPipelineStats
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
            fileCacheStats && fileCache != null ? fileCache.fileCacheStats() : null,
            taskCancellation ? this.taskCancellationMonitoringService.stats() : null,
            searchPipelineStats ? this.searchPipelineService.stats() : null
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

    public TaskCancellationMonitoringService getTaskCancellationMonitoringService() {
        return taskCancellationMonitoringService;
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
