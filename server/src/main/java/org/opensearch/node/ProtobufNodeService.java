/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

package org.opensearch.node;

import org.opensearch.common.util.io.IOUtils;
import org.opensearch.Build;
import org.opensearch.Version;
import org.opensearch.action.admin.cluster.node.info.ProtobufNodeInfo;
import org.opensearch.action.admin.cluster.node.stats.ProtobufNodeStats;
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
import org.opensearch.indices.breaker.CircuitBreakerService;
import org.opensearch.ingest.IngestService;
import org.opensearch.monitor.ProtobufMonitorService;
import org.opensearch.plugins.PluginsService;
import org.opensearch.script.ScriptService;
import org.opensearch.search.aggregations.support.AggregationUsageService;
import org.opensearch.search.backpressure.SearchBackpressureService;
import org.opensearch.search.pipeline.SearchPipelineService;
import org.opensearch.threadpool.ThreadPool;
import org.opensearch.transport.ProtobufTransportService;

import java.io.Closeable;
import java.io.IOException;
import java.util.concurrent.TimeUnit;

/**
 * Services exposed to nodes
*
* @opensearch.internal
*/
public class ProtobufNodeService implements Closeable {
    private final Settings settings;
    private final ThreadPool threadPool;
    private final ProtobufMonitorService monitorService;
    private final ProtobufTransportService transportService;
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

    ProtobufNodeService(
        Settings settings,
        ThreadPool threadPool,
        ProtobufMonitorService monitorService,
        Discovery discovery,
        ProtobufTransportService transportService,
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
        FileCache fileCache
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
        clusterService.addStateApplier(ingestService);
        clusterService.addStateApplier(searchPipelineService);
    }

    public ProtobufNodeInfo info(
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
        ProtobufNodeInfo.Builder builder = ProtobufNodeInfo.builder(Version.CURRENT, Build.CURRENT, transportService.getLocalNode());
        if (settings) {
            builder.setSettings(settingsFilter.filter(this.settings));
        }
        if (os) {
            builder.setOs(monitorService.osService().protobufInfo());
        }
        if (process) {
            builder.setProcess(monitorService.processService().protobufInfo());
        }
        if (jvm) {
            builder.setJvm(monitorService.jvmService().protobufInfo());
        }
        if (threadPool) {
            builder.setThreadPool(this.threadPool.protobufInfo());
        }
        if (transport) {
            builder.setTransport(transportService.protobufInfo());
        }
        if (http && httpServerTransport != null) {
            builder.setHttp(httpServerTransport.protobufInfo());
        }
        // if (plugin && pluginService != null) {
        // builder.setPlugins(pluginService.info());
        // }
        if (ingest && ingestService != null) {
            builder.setIngest(ingestService.protobufInfo());
        }
        if (aggs && aggregationUsageService != null) {
            builder.setAggsInfo(aggregationUsageService.protobufInfo());
        }
        if (indices) {
            builder.setTotalIndexingBuffer(indicesService.getTotalIndexingBufferBytes());
        }
        if (searchPipeline && searchPipelineService != null) {
            builder.setProtobufSearchPipelineInfo(searchPipelineService.protobufInfo());
        }
        return builder.build();
    }

    public ProtobufNodeStats stats(
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
        return new ProtobufNodeStats(
            transportService.getLocalNode(),
            System.currentTimeMillis(),
            indices.anySet() ? indicesService.protobufStats(indices) : null,
            os ? monitorService.osService().stats() : null,
            process ? monitorService.processService().stats() : null,
            jvm ? monitorService.jvmService().stats() : null,
            threadPool ? this.threadPool.protobufStats() : null,
            fs ? monitorService.fsService().stats() : null,
            transport ? transportService.stats() : null,
            http ? (httpServerTransport == null ? null : httpServerTransport.protobufStats()) : null,
            circuitBreaker ? circuitBreakerService.protobufStats() : null,
            script ? scriptService.protobufStats() : null,
            discoveryStats ? discovery.protobufStats() : null,
            ingest ? ingestService.protobufStats() : null,
            adaptiveSelection ? responseCollectorService.getProtobufAdaptiveStats(searchTransportService.getPendingSearchRequests()) : null
            // scriptCache ? scriptService.cacheStats() : null,
            // indexingPressure ? this.indexingPressureService.nodeStats() : null,
            // shardIndexingPressure ? this.indexingPressureService.shardStats(indices) : null,
            // searchBackpressure ? this.searchBackpressureService.nodeStats() : null,
            // clusterManagerThrottling ? this.clusterService.getClusterManagerService().getThrottlingStats() : null,
            // weightedRoutingStats ? WeightedRoutingStats.getInstance() : null,
            // fileCacheStats && fileCache != null ? fileCache.fileCacheStats() : null
        );
    }

    public IngestService getIngestService() {
        return ingestService;
    }

    public ProtobufMonitorService getMonitorService() {
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
