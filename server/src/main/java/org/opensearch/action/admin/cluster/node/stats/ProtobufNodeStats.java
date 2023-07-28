/*
* SPDX-License-Identifier: Apache-2.0
*
* The OpenSearch Contributors require contributions made to
* this file be licensed under the Apache-2.0 license or a
* compatible open source license.
*/

package org.opensearch.action.admin.cluster.node.stats;

import com.google.protobuf.CodedInputStream;
import com.google.protobuf.CodedOutputStream;
import com.google.protobuf.InvalidProtocolBufferException;

import org.opensearch.action.support.nodes.ProtobufBaseNodeResponse;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.common.Nullable;
import org.opensearch.common.io.stream.ProtobufStreamInput;
import org.opensearch.common.io.stream.ProtobufStreamOutput;
import org.opensearch.common.io.stream.TryWriteable;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.discovery.ProtobufDiscoveryStats;
import org.opensearch.http.ProtobufHttpStats;
import org.opensearch.indices.ProtobufNodeIndicesStats;
import org.opensearch.indices.breaker.ProtobufAllCircuitBreakerStats;
import org.opensearch.ingest.ProtobufIngestStats;
import org.opensearch.monitor.fs.ProtobufFsInfo;
import org.opensearch.monitor.jvm.ProtobufJvmStats;
import org.opensearch.monitor.os.ProtobufOsStats;
import org.opensearch.monitor.process.ProtobufProcessStats;
import org.opensearch.node.ProtobufAdaptiveSelectionStats;
import org.opensearch.script.ProtobufScriptStats;
import org.opensearch.server.proto.NodesStatsProto;
import org.opensearch.server.proto.NodesStatsProto.NodesStats;
import org.opensearch.threadpool.ProtobufThreadPoolStats;
import org.opensearch.transport.ProtobufTransportStats;

import java.io.IOException;
import java.io.OutputStream;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * Node statistics (dynamic, changes depending on when created).
*
* @opensearch.internal
*/
public class ProtobufNodeStats extends ProtobufBaseNodeResponse implements ToXContentFragment, TryWriteable {

    private long timestamp;

    @Nullable
    private ProtobufNodeIndicesStats indices;

    @Nullable
    private ProtobufOsStats os;

    @Nullable
    private ProtobufProcessStats process;

    @Nullable
    private ProtobufJvmStats jvm;

    @Nullable
    private ProtobufThreadPoolStats threadPool;

    @Nullable
    private ProtobufFsInfo fs;

    @Nullable
    private ProtobufTransportStats transport;

    @Nullable
    private ProtobufHttpStats http;

    @Nullable
    private ProtobufAllCircuitBreakerStats breaker;

    @Nullable
    private ProtobufScriptStats scriptStats;

    @Nullable
    private ProtobufDiscoveryStats discoveryStats;

    @Nullable
    private ProtobufIngestStats ingestStats;

    @Nullable
    private ProtobufAdaptiveSelectionStats adaptiveSelectionStats;

    private NodesStatsProto.NodesStats nodesStatsResponse;

    public ProtobufNodeStats(CodedInputStream in) throws IOException {
        super(in);
        ProtobufStreamInput protobufStreamInput = new ProtobufStreamInput(in);
        timestamp = in.readInt64();
        if (in.readBool()) {
            indices = new ProtobufNodeIndicesStats(in);
        }
        os = protobufStreamInput.readOptionalWriteable(ProtobufOsStats::new);
        process = protobufStreamInput.readOptionalWriteable(ProtobufProcessStats::new);
        jvm = protobufStreamInput.readOptionalWriteable(ProtobufJvmStats::new);
        threadPool = protobufStreamInput.readOptionalWriteable(ProtobufThreadPoolStats::new);
        fs = protobufStreamInput.readOptionalWriteable(ProtobufFsInfo::new);
        transport = protobufStreamInput.readOptionalWriteable(ProtobufTransportStats::new);
        http = protobufStreamInput.readOptionalWriteable(ProtobufHttpStats::new);
        breaker = protobufStreamInput.readOptionalWriteable(ProtobufAllCircuitBreakerStats::new);
        scriptStats = protobufStreamInput.readOptionalWriteable(ProtobufScriptStats::new);
        discoveryStats = protobufStreamInput.readOptionalWriteable(ProtobufDiscoveryStats::new);
        ingestStats = protobufStreamInput.readOptionalWriteable(ProtobufIngestStats::new);
        adaptiveSelectionStats = protobufStreamInput.readOptionalWriteable(ProtobufAdaptiveSelectionStats::new);
    }

    public ProtobufNodeStats(byte[] data) throws InvalidProtocolBufferException {
        super(data);
        this.nodesStatsResponse = NodesStatsProto.NodesStats.parseFrom(data);
    }

    public ProtobufNodeStats(
        DiscoveryNode node,
        long timestamp,
        @Nullable ProtobufNodeIndicesStats indices,
        @Nullable ProtobufOsStats os,
        @Nullable ProtobufProcessStats process,
        @Nullable ProtobufJvmStats jvm,
        @Nullable ProtobufThreadPoolStats threadPool,
        @Nullable ProtobufFsInfo fs,
        @Nullable ProtobufTransportStats transport,
        @Nullable ProtobufHttpStats http,
        @Nullable ProtobufAllCircuitBreakerStats breaker,
        @Nullable ProtobufScriptStats scriptStats,
        @Nullable ProtobufDiscoveryStats discoveryStats,
        @Nullable ProtobufIngestStats ingestStats,
        @Nullable ProtobufAdaptiveSelectionStats adaptiveSelectionStats
    ) {
        super(node);
        this.timestamp = timestamp;
        this.indices = indices;
        this.os = os;
        this.process = process;
        this.jvm = jvm;
        this.threadPool = threadPool;
        this.fs = fs;
        this.transport = transport;
        this.http = http;
        this.breaker = breaker;
        this.scriptStats = scriptStats;
        this.discoveryStats = discoveryStats;
        this.ingestStats = ingestStats;
        this.adaptiveSelectionStats = adaptiveSelectionStats;
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

    /**
     * Indices level stats.
    */
    @Nullable
    public ProtobufNodeIndicesStats getIndices() {
        return this.indices;
    }

    /**
     * Operating System level statistics.
    */
    @Nullable
    public ProtobufOsStats getOs() {
        return this.os;
    }

    /**
     * Process level statistics.
    */
    @Nullable
    public ProtobufProcessStats getProcess() {
        return process;
    }

    /**
     * JVM level statistics.
    */
    @Nullable
    public ProtobufJvmStats getJvm() {
        return jvm;
    }

    /**
     * Thread Pool level statistics.
    */
    @Nullable
    public ProtobufThreadPoolStats getThreadPool() {
        return this.threadPool;
    }

    /**
     * File system level stats.
    */
    @Nullable
    public ProtobufFsInfo getFs() {
        return fs;
    }

    @Nullable
    public ProtobufTransportStats getTransport() {
        return this.transport;
    }

    @Nullable
    public ProtobufHttpStats getHttp() {
        return this.http;
    }

    @Nullable
    public ProtobufAllCircuitBreakerStats getBreaker() {
        return this.breaker;
    }

    @Nullable
    public ProtobufScriptStats getScriptStats() {
        return this.scriptStats;
    }

    @Nullable
    public ProtobufDiscoveryStats getDiscoveryStats() {
        return this.discoveryStats;
    }

    @Nullable
    public ProtobufIngestStats getIngestStats() {
        return ingestStats;
    }

    @Nullable
    public ProtobufAdaptiveSelectionStats getAdaptiveSelectionStats() {
        return adaptiveSelectionStats;
    }

    @Override
    public void writeTo(CodedOutputStream out) throws IOException {
        super.writeTo(out);
        ProtobufStreamOutput protobufStreamOutput = new ProtobufStreamOutput(out);
        out.writeInt64NoTag(timestamp);
        if (indices == null) {
            out.writeBoolNoTag(false);
        } else {
            out.writeBoolNoTag(true);
            indices.writeTo(out);
        }
        protobufStreamOutput.writeOptionalWriteable(os);
        protobufStreamOutput.writeOptionalWriteable(process);
        protobufStreamOutput.writeOptionalWriteable(jvm);
        protobufStreamOutput.writeOptionalWriteable(threadPool);
        protobufStreamOutput.writeOptionalWriteable(fs);
        protobufStreamOutput.writeOptionalWriteable(transport);
        protobufStreamOutput.writeOptionalWriteable(http);
        protobufStreamOutput.writeOptionalWriteable(breaker);
        protobufStreamOutput.writeOptionalWriteable(scriptStats);
        protobufStreamOutput.writeOptionalWriteable(discoveryStats);
        protobufStreamOutput.writeOptionalWriteable(ingestStats);
        protobufStreamOutput.writeOptionalWriteable(adaptiveSelectionStats);
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

        if (getIndices() != null) {
            getIndices().toXContent(builder, params);
        }
        if (getOs() != null) {
            getOs().toXContent(builder, params);
        }
        if (getProcess() != null) {
            getProcess().toXContent(builder, params);
        }
        if (getJvm() != null) {
            getJvm().toXContent(builder, params);
        }
        if (getThreadPool() != null) {
            getThreadPool().toXContent(builder, params);
        }
        if (getFs() != null) {
            getFs().toXContent(builder, params);
        }
        if (getTransport() != null) {
            getTransport().toXContent(builder, params);
        }
        if (getHttp() != null) {
            getHttp().toXContent(builder, params);
        }
        if (getBreaker() != null) {
            getBreaker().toXContent(builder, params);
        }
        if (getScriptStats() != null) {
            getScriptStats().toXContent(builder, params);
        }
        if (getDiscoveryStats() != null) {
            getDiscoveryStats().toXContent(builder, params);
        }
        if (getIngestStats() != null) {
            getIngestStats().toXContent(builder, params);
        }
        if (getAdaptiveSelectionStats() != null) {
            getAdaptiveSelectionStats().toXContent(builder, params);
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
