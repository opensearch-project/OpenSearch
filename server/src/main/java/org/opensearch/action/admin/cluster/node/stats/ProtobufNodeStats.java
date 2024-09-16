/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

package org.opensearch.action.admin.cluster.node.stats;

import com.google.protobuf.InvalidProtocolBufferException;

import org.opensearch.action.support.nodes.ProtobufBaseNodeResponse;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.common.Nullable;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.discovery.DiscoveryStats;
import org.opensearch.http.HttpStats;
import org.opensearch.indices.NodeIndicesStats;
import org.opensearch.core.indices.breaker.AllCircuitBreakerStats;
import org.opensearch.ingest.IngestStats;
import org.opensearch.monitor.fs.FsInfo;
import org.opensearch.monitor.jvm.JvmStats;
import org.opensearch.monitor.os.OsStats;
import org.opensearch.monitor.process.ProcessStats;
import org.opensearch.node.AdaptiveSelectionStats;
import org.opensearch.script.ScriptStats;
import org.opensearch.server.proto.NodesStatsProto;
import org.opensearch.threadpool.ThreadPoolStats;
import org.opensearch.transport.TransportStats;

import java.io.IOException;
import java.io.OutputStream;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Node statistics (dynamic, changes depending on when created).
*
* @opensearch.internal
*/
public class ProtobufNodeStats extends ProtobufBaseNodeResponse implements ToXContentFragment {

    private long timestamp;

    private NodesStatsProto.NodesStats nodesStatsResponse;

    public ProtobufNodeStats(byte[] data) throws InvalidProtocolBufferException {
        super(data);
        this.nodesStatsResponse = NodesStatsProto.NodesStats.parseFrom(data);
    }

    public ProtobufNodeStats(NodesStatsProto.NodesStats nodesStats) throws InvalidProtocolBufferException {
        super(nodesStats.toByteArray());
        this.nodesStatsResponse = nodesStats;
    }

    public ProtobufNodeStats(
        DiscoveryNode node,
        long timestamp,
        @Nullable NodeIndicesStats indices,
        @Nullable OsStats os,
        @Nullable ProcessStats process,
        @Nullable JvmStats jvm,
        @Nullable ThreadPoolStats threadPool,
        @Nullable FsInfo fs,
        @Nullable TransportStats transport,
        @Nullable HttpStats http,
        @Nullable AllCircuitBreakerStats breaker,
        @Nullable ScriptStats scriptStats,
        @Nullable DiscoveryStats discoveryStats,
        @Nullable IngestStats ingestStats,
        @Nullable AdaptiveSelectionStats adaptiveSelectionStats
    ) {
        super(node);
        this.timestamp = timestamp;
        NodesStatsProto.NodesStats.CompletionStats completionStats = NodesStatsProto.NodesStats.CompletionStats.newBuilder()
            .setSize(indices.getCompletion().getSizeInBytes())
            .build();
        NodesStatsProto.NodesStats.FieldDataStats fieldDataStats = NodesStatsProto.NodesStats.FieldDataStats.newBuilder()
            .setMemSize(indices.getFieldData().getMemorySizeInBytes())
            .setEvictions(indices.getFieldData().getEvictions())
            .build();
        NodesStatsProto.NodesStats.QueryCacheStats queryCacheStats = NodesStatsProto.NodesStats.QueryCacheStats.newBuilder()
            .setRamBytesUsed(indices.getQueryCache().getMemorySizeInBytes())
            .setHitCount(indices.getQueryCache().getEvictions())
            .setMissCount(indices.getQueryCache().getHitCount())
            .setCacheCount(indices.getQueryCache().getMissCount())
            .setCacheSize(indices.getQueryCache().getCacheSize())
            .build();
        NodesStatsProto.NodesStats.RequestCacheStats requestCacheStats = NodesStatsProto.NodesStats.RequestCacheStats.newBuilder()
            .setMemorySize(indices.getRequestCache().getMemorySizeInBytes())
            .setEvictions(indices.getRequestCache().getEvictions())
            .setHitCount(indices.getRequestCache().getHitCount())
            .setMissCount(indices.getRequestCache().getMissCount())
            .build();
        NodesStatsProto.NodesStats.FlushStats flushStats = NodesStatsProto.NodesStats.FlushStats.newBuilder()
            .setTotal(indices.getFlush().getTotal())
            .setPeriodic(indices.getFlush().getPeriodic())
            .setTotalTimeInMillis(indices.getFlush().getTotalTimeInMillis())
            .build();
        NodesStatsProto.NodesStats.GetStats getStats = NodesStatsProto.NodesStats.GetStats.newBuilder()
            .setExistsCount(indices.getGet().getExistsCount())
            .setExistsTimeInMillis(indices.getGet().getExistsTimeInMillis())
            .setMissingCount(indices.getGet().getMissingCount())
            .setMissingTimeInMillis(indices.getGet().getMissingTimeInMillis())
            .setCurrent(indices.getGet().current())
            .setCount(indices.getGet().getCount())
            .setTime(indices.getGet().getTimeInMillis())
            .build();
        NodesStatsProto.NodesStats.IndexingStats indexingStats = NodesStatsProto.NodesStats.IndexingStats.newBuilder()
            .setIndexCount(indices.getIndexing().getTotal().getIndexCount())
            .setIndexTimeInMillis(indices.getIndexing().getTotal().getIndexTime().getMillis())
            .setIndexCurrent(indices.getIndexing().getTotal().getIndexCurrent())
            .setIndexFailedCount(indices.getIndexing().getTotal().getIndexFailedCount())
            .setDeleteCount(indices.getIndexing().getTotal().getDeleteCount())
            .setDeleteTimeInMillis(indices.getIndexing().getTotal().getDeleteTime().getMillis())
            .setDeleteCurrent(indices.getIndexing().getTotal().getDeleteCurrent())
            .setNoopUpdateCount(indices.getIndexing().getTotal().getNoopUpdateCount())
            .setIsThrottled(indices.getIndexing().getTotal().isThrottled())
            .setThrottleTimeInMillis(indices.getIndexing().getTotal().getThrottleTime().getMillis())
            .build();
        NodesStatsProto.NodesStats.MergeStats mergeStats = NodesStatsProto.NodesStats.MergeStats.newBuilder()
            .setTotal(indices.getMerge().getTotal())
            .setTotalTimeInMillis(indices.getMerge().getTotalTimeInMillis())
            .setTotalNumDocs(indices.getMerge().getTotalNumDocs())
            .setTotalSizeInBytes(indices.getMerge().getTotalSizeInBytes())
            .setCurrent(indices.getMerge().getCurrent())
            .setCurrentNumDocs(indices.getMerge().getCurrentNumDocs())
            .setCurrentSizeInBytes(indices.getMerge().getCurrentSizeInBytes())
            .build();
        NodesStatsProto.NodesStats.RefreshStats refreshStats = NodesStatsProto.NodesStats.RefreshStats.newBuilder()
            .setTotal(indices.getRefresh().getTotal())
            .setTotalTimeInMillis(indices.getRefresh().getTotalTimeInMillis())
            .setExternalTotal(indices.getRefresh().getExternalTotal())
            .setExternalTotalTimeInMillis(indices.getRefresh().getExternalTotalTimeInMillis())
            .setListeners(indices.getRefresh().getListeners())
            .build();
        NodesStatsProto.NodesStats.ScriptStats scStats = NodesStatsProto.NodesStats.ScriptStats.newBuilder()
            .setCompilations(scriptStats.getCompilations())
            .setCacheEvictions(scriptStats.getCacheEvictions())
            .setCompilationLimitTriggered(scriptStats.getCompilationLimitTriggered())
            .build();
        NodesStatsProto.NodesStats.SearchStats searchStats = NodesStatsProto.NodesStats.SearchStats.newBuilder()
            .setQueryCount(indices.getSearch().getTotal().getQueryCount())
            .setQueryTimeInMillis(indices.getSearch().getTotal().getQueryTimeInMillis())
            .setQueryCurrent(indices.getSearch().getTotal().getQueryCurrent())
            .setFetchCount(indices.getSearch().getTotal().getFetchCount())
            .setFetchTimeInMillis(indices.getSearch().getTotal().getFetchTimeInMillis())
            .setFetchCurrent(indices.getSearch().getTotal().getFetchCurrent())
            .setScrollCount(indices.getSearch().getTotal().getScrollCount())
            .setScrollTimeInMillis(indices.getSearch().getTotal().getScrollTimeInMillis())
            .setScrollCurrent(indices.getSearch().getTotal().getScrollCurrent())
            .setSuggestCount(indices.getSearch().getTotal().getSuggestCount())
            .setSuggestTimeInMillis(indices.getSearch().getTotal().getSuggestTimeInMillis())
            .setSuggestCurrent(indices.getSearch().getTotal().getSuggestCurrent())
            .setPitCount(indices.getSearch().getTotal().getPitCount())
            .setPitTimeInMillis(indices.getSearch().getTotal().getPitTimeInMillis())
            .setPitCurrent(indices.getSearch().getTotal().getPitCurrent())
            .setOpenContexts(indices.getSearch().getOpenContexts())
            .build();
        NodesStatsProto.NodesStats.SegmentStats segmentStats = NodesStatsProto.NodesStats.SegmentStats.newBuilder()
            .setCount(indices.getSegments().getCount())
            .setIndexWriterMemoryInBytes(indices.getSegments().getIndexWriterMemoryInBytes())
            .setVersionMapMemoryInBytes(indices.getSegments().getVersionMapMemoryInBytes())
            .setMaxUnsafeAutoIdTimestamp(indices.getSegments().getMaxUnsafeAutoIdTimestamp())
            .setBitsetMemoryInBytes(indices.getSegments().getBitsetMemoryInBytes())
            .build();
        List<Double> list = Arrays.stream(os.getCpu().getLoadAverage()).boxed().collect(Collectors.toList());
        this.nodesStatsResponse = NodesStatsProto.NodesStats.newBuilder()
            .setNodeId(node.getId())
            .setJvmHeapUsed(jvm.getMem().getHeapUsed().getBytes())
            .setJvmHeapUsedPercent(Short.toString(jvm.getMem().getHeapUsedPercent()))
            .setJvmUpTime(jvm.getUptime().getMillis())
            .setDiskTotal(fs.getTotal().getTotal().getBytes())
            .setDiskAvailable(fs.getTotal().getAvailable().getBytes())
            .setOsMemUsed(os.getMem().getUsed().getBytes())
            .setOsMemUsedPercent(Short.toString(os.getMem().getUsedPercent()))
            .setOsMemTotal(os.getMem().getTotal().getBytes())
            .setOsCpuPercent(Short.toString(os.getCpu().getPercent()))
            .addAllOsCpuLoadAverage(list)
            .setProcessOpenFileDescriptors(process.getOpenFileDescriptors())
            .setProcessMaxFileDescriptors(process.getMaxFileDescriptors())
            .setCompletionStats(completionStats)
            .setFieldDataStats(fieldDataStats)
            .setQueryCacheStats(queryCacheStats)
            .setRequestCacheStats(requestCacheStats)
            .setFlushStats(flushStats)
            .setGetStats(getStats)
            .setIndexingStats(indexingStats)
            .setMergeStats(mergeStats)
            .setRefreshStats(refreshStats)
            .setScriptStats(scStats)
            .setSearchStats(searchStats)
            .setSegmentStats(segmentStats)
            .build();
    }

    public long getTimestamp() {
        return this.timestamp;
    }

    @Nullable
    public String getHostname() {
        return getNode().getHostName();
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {

        builder.field("name", getNode().getName());
        builder.field("transport_address", getNode().getAddress().toString());
        builder.field("host", getNode().getHostName());
        builder.field("ip", getNode().getAddress());

        builder.startArray("roles");
        for (DiscoveryNodeRole role : getNode().getRoles()) {
            builder.value(role.roleName());
        }
        builder.endArray();

        if (!getNode().getAttributes().isEmpty()) {
            builder.startObject("attributes");
            for (Map.Entry<String, String> attrEntry : getNode().getAttributes().entrySet()) {
                builder.field(attrEntry.getKey(), attrEntry.getValue());
            }
            builder.endObject();
        }

        return builder;
    }

    @Override
    public void writeTo(OutputStream out) throws IOException {
        out.write(this.nodesStatsResponse.toByteArray());
    }

    public NodesStatsProto.NodesStats response() {
        return this.nodesStatsResponse;
    }
}
