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
 *     http://www.apache.org/licenses/LICENSE-2.0
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

package org.opensearch.action.admin.cluster.node.stats;

import org.opensearch.Version;
import org.opensearch.action.support.nodes.BaseNodeResponse;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.node.DiscoveryNodeRole;
import org.opensearch.cluster.routing.WeightedRoutingStats;
import org.opensearch.cluster.service.ClusterManagerThrottlingStats;
import org.opensearch.common.Nullable;
import org.opensearch.common.cache.service.NodeCacheStats;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.indices.breaker.AllCircuitBreakerStats;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.discovery.DiscoveryStats;
import org.opensearch.http.HttpStats;
import org.opensearch.index.SegmentReplicationRejectionStats;
import org.opensearch.index.stats.IndexingPressureStats;
import org.opensearch.index.stats.ShardIndexingPressureStats;
import org.opensearch.index.store.remote.filecache.AggregateFileCacheStats;
import org.opensearch.indices.NodeIndicesStats;
import org.opensearch.ingest.IngestStats;
import org.opensearch.monitor.fs.FsInfo;
import org.opensearch.monitor.jvm.JvmStats;
import org.opensearch.monitor.os.OsStats;
import org.opensearch.monitor.process.ProcessStats;
import org.opensearch.node.AdaptiveSelectionStats;
import org.opensearch.node.NodesResourceUsageStats;
import org.opensearch.node.remotestore.RemoteStoreNodeStats;
import org.opensearch.plugin.stats.AnalyticsBackendNativeMemoryStats;
import org.opensearch.plugin.stats.NativeAllocatorPoolStats;
import org.opensearch.plugins.BlockCacheStats;
import org.opensearch.ratelimitting.admissioncontrol.stats.AdmissionControlStats;
import org.opensearch.repositories.RepositoriesStats;
import org.opensearch.script.ScriptCacheStats;
import org.opensearch.script.ScriptStats;
import org.opensearch.search.backpressure.stats.SearchBackpressureStats;
import org.opensearch.search.pipeline.SearchPipelineStats;
import org.opensearch.tasks.TaskCancellationStats;
import org.opensearch.threadpool.ThreadPoolStats;
import org.opensearch.transport.TransportStats;

import java.io.IOException;
import java.util.Map;

/**
 * Node statistics (dynamic, changes depending on when created).
 *
 * @opensearch.internal
 */
public class NodeStats extends BaseNodeResponse implements ToXContentFragment {

    private long timestamp;

    @Nullable
    private NodeIndicesStats indices;

    @Nullable
    private OsStats os;

    @Nullable
    private ProcessStats process;

    @Nullable
    private JvmStats jvm;

    @Nullable
    private ThreadPoolStats threadPool;

    @Nullable
    private FsInfo fs;

    @Nullable
    private TransportStats transport;

    @Nullable
    private HttpStats http;

    @Nullable
    private AllCircuitBreakerStats breaker;

    @Nullable
    private ScriptStats scriptStats;

    @Nullable
    private ScriptCacheStats scriptCacheStats;

    @Nullable
    private DiscoveryStats discoveryStats;

    @Nullable
    private IngestStats ingestStats;

    @Nullable
    private AdaptiveSelectionStats adaptiveSelectionStats;

    @Nullable
    private IndexingPressureStats indexingPressureStats;

    @Nullable
    private ShardIndexingPressureStats shardIndexingPressureStats;

    @Nullable
    private SearchBackpressureStats searchBackpressureStats;

    @Nullable
    private SegmentReplicationRejectionStats segmentReplicationRejectionStats;

    @Nullable
    private ClusterManagerThrottlingStats clusterManagerThrottlingStats;

    @Nullable
    private WeightedRoutingStats weightedRoutingStats;

    @Nullable
    private AggregateFileCacheStats fileCacheStats;

    /** Populated only when ?detailed is requested: FileCache-only stats (no block cache contribution). */
    @Nullable
    private AggregateFileCacheStats fileCacheOnlyStats;

    /** Populated only when ?detailed is requested: combined rollup across all BlockCache implementations. */
    @Nullable
    private BlockCacheStats blockCacheOnlyStats;

    @Nullable
    private TaskCancellationStats taskCancellationStats;

    @Nullable
    private SearchPipelineStats searchPipelineStats;

    @Nullable
    private NodesResourceUsageStats resourceUsageStats;

    @Nullable
    private RepositoriesStats repositoriesStats;

    @Nullable
    private AdmissionControlStats admissionControlStats;

    @Nullable
    private NodeCacheStats nodeCacheStats;

    @Nullable
    private RemoteStoreNodeStats remoteStoreNodeStats;

    @Nullable
    private NativeAllocatorPoolStats nativeAllocatorStats;

    /**
     * Process-level native-memory estimate captured on the data node hosting this {@code NodeStats}.
     * Computed once in {@link org.opensearch.node.NodeService#stats} via
     * {@code OsProbe.getProcessNativeMemoryBytes()} and serialized over the wire so the coordinator
     * renders the source node's value, not its own. {@code -1} when the probe could not read
     * {@code /proc/self/status} (non-Linux platforms or restricted environments).
     */
    private long totalEstimatedNativeBytes;

    public NodeStats(StreamInput in) throws IOException {
        super(in);
        timestamp = in.readVLong();
        if (in.readBoolean()) {
            indices = new NodeIndicesStats(in);
        }
        os = in.readOptionalWriteable(OsStats::new);
        process = in.readOptionalWriteable(ProcessStats::new);
        jvm = in.readOptionalWriteable(JvmStats::new);
        threadPool = in.readOptionalWriteable(ThreadPoolStats::new);
        fs = in.readOptionalWriteable(FsInfo::new);
        transport = in.readOptionalWriteable(TransportStats::new);
        http = in.readOptionalWriteable(HttpStats::new);
        breaker = in.readOptionalWriteable(AllCircuitBreakerStats::new);
        scriptStats = in.readOptionalWriteable(ScriptStats::new);
        discoveryStats = in.readOptionalWriteable(DiscoveryStats::new);
        ingestStats = in.readOptionalWriteable(IngestStats::new);
        adaptiveSelectionStats = in.readOptionalWriteable(AdaptiveSelectionStats::new);
        scriptCacheStats = null;
        if (scriptStats != null) {
            scriptCacheStats = scriptStats.toScriptCacheStats();
        }
        indexingPressureStats = in.readOptionalWriteable(IndexingPressureStats::new);
        shardIndexingPressureStats = in.readOptionalWriteable(ShardIndexingPressureStats::new);

        if (in.getVersion().onOrAfter(Version.V_2_4_0)) {
            searchBackpressureStats = in.readOptionalWriteable(SearchBackpressureStats::new);
        } else {
            searchBackpressureStats = null;
        }

        if (in.getVersion().onOrAfter(Version.V_2_6_0)) {
            clusterManagerThrottlingStats = in.readOptionalWriteable(ClusterManagerThrottlingStats::new);
        } else {
            clusterManagerThrottlingStats = null;
        }
        if (in.getVersion().onOrAfter(Version.V_2_6_0)) {
            weightedRoutingStats = in.readOptionalWriteable(WeightedRoutingStats::new);
        } else {
            weightedRoutingStats = null;
        }
        if (in.getVersion().onOrAfter(Version.V_2_7_0)) {
            fileCacheStats = in.readOptionalWriteable(AggregateFileCacheStats::new);
        } else {
            fileCacheStats = null;
        }
        if (in.getVersion().onOrAfter(Version.V_3_7_0)) {
            fileCacheOnlyStats = in.readOptionalWriteable(AggregateFileCacheStats::new);
            blockCacheOnlyStats = in.readOptionalWriteable(BlockCacheStats::new);
        } else {
            fileCacheOnlyStats = null;
            blockCacheOnlyStats = null;
        }
        if (in.getVersion().onOrAfter(Version.V_2_9_0)) {
            taskCancellationStats = in.readOptionalWriteable(TaskCancellationStats::new);
        } else {
            taskCancellationStats = null;
        }
        if (in.getVersion().onOrAfter(Version.V_2_9_0)) {
            searchPipelineStats = in.readOptionalWriteable(SearchPipelineStats::new);
        } else {
            searchPipelineStats = null;
        }
        if (in.getVersion().onOrAfter(Version.V_2_12_0)) {
            resourceUsageStats = in.readOptionalWriteable(NodesResourceUsageStats::new);
        } else {
            resourceUsageStats = null;
        }
        if (in.getVersion().onOrAfter(Version.V_2_12_0)) {
            segmentReplicationRejectionStats = in.readOptionalWriteable(SegmentReplicationRejectionStats::new);
        } else {
            segmentReplicationRejectionStats = null;
        }
        if (in.getVersion().onOrAfter(Version.V_2_12_0)) {
            repositoriesStats = in.readOptionalWriteable(RepositoriesStats::new);
        } else {
            repositoriesStats = null;
        }
        if (in.getVersion().onOrAfter(Version.V_2_12_0)) {
            admissionControlStats = in.readOptionalWriteable(AdmissionControlStats::new);
        } else {
            admissionControlStats = null;
        }
        if (in.getVersion().onOrAfter(Version.V_2_14_0)) {
            nodeCacheStats = in.readOptionalWriteable(NodeCacheStats::new);
        } else {
            nodeCacheStats = null;
        }
        if (in.getVersion().onOrAfter(Version.V_2_18_0)) {
            remoteStoreNodeStats = in.readOptionalWriteable(RemoteStoreNodeStats::new);
        } else {
            remoteStoreNodeStats = null;
        }
        if (in.getVersion().onOrAfter(Version.V_3_8_0)) {
            nativeAllocatorStats = in.readOptionalWriteable(NativeAllocatorPoolStats::new);
        } else if (in.getVersion().onOrAfter(Version.V_3_7_0)) {
            // BWC: V_3_7_0 wrote old-format NativeAllocatorPoolStats (3 VLongs + pools with 4 fields); read and discard.
            in.readOptionalWriteable(NativeAllocatorPoolStats::readAndDiscardV3_7);
            nativeAllocatorStats = null;
        } else {
            nativeAllocatorStats = null;
        }
        if (in.getVersion().onOrAfter(Version.V_3_7_0)) {
            // BWC: V_3_7_0 wrote AnalyticsBackendNativeMemoryStats here; read and discard.
            in.readOptionalWriteable(AnalyticsBackendNativeMemoryStats::new);
        }
        if (in.getVersion().onOrAfter(Version.V_3_7_0)) {
            totalEstimatedNativeBytes = in.readLong();
        } else {
            totalEstimatedNativeBytes = -1L;
        }
    }

    public NodeStats(
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
        @Nullable AdaptiveSelectionStats adaptiveSelectionStats,
        @Nullable NodesResourceUsageStats resourceUsageStats,
        @Nullable ScriptCacheStats scriptCacheStats,
        @Nullable IndexingPressureStats indexingPressureStats,
        @Nullable ShardIndexingPressureStats shardIndexingPressureStats,
        @Nullable SearchBackpressureStats searchBackpressureStats,
        @Nullable ClusterManagerThrottlingStats clusterManagerThrottlingStats,
        @Nullable WeightedRoutingStats weightedRoutingStats,
        @Nullable AggregateFileCacheStats fileCacheStats,
        @Nullable AggregateFileCacheStats fileCacheOnlyStats,
        @Nullable BlockCacheStats blockCacheOnlyStats,
        @Nullable TaskCancellationStats taskCancellationStats,
        @Nullable SearchPipelineStats searchPipelineStats,
        @Nullable SegmentReplicationRejectionStats segmentReplicationRejectionStats,
        @Nullable RepositoriesStats repositoriesStats,
        @Nullable AdmissionControlStats admissionControlStats,
        @Nullable NodeCacheStats nodeCacheStats,
        @Nullable RemoteStoreNodeStats remoteStoreNodeStats,
        @Nullable NativeAllocatorPoolStats nativeAllocatorStats,
        long totalEstimatedNativeBytes
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
        this.resourceUsageStats = resourceUsageStats;
        this.scriptCacheStats = scriptCacheStats;
        this.indexingPressureStats = indexingPressureStats;
        this.shardIndexingPressureStats = shardIndexingPressureStats;
        this.searchBackpressureStats = searchBackpressureStats;
        this.clusterManagerThrottlingStats = clusterManagerThrottlingStats;
        this.weightedRoutingStats = weightedRoutingStats;
        this.fileCacheStats = fileCacheStats;
        this.fileCacheOnlyStats = fileCacheOnlyStats;
        this.blockCacheOnlyStats = blockCacheOnlyStats;
        this.taskCancellationStats = taskCancellationStats;
        this.searchPipelineStats = searchPipelineStats;
        this.segmentReplicationRejectionStats = segmentReplicationRejectionStats;
        this.repositoriesStats = repositoriesStats;
        this.admissionControlStats = admissionControlStats;
        this.nodeCacheStats = nodeCacheStats;
        this.remoteStoreNodeStats = remoteStoreNodeStats;
        this.nativeAllocatorStats = nativeAllocatorStats;
        this.totalEstimatedNativeBytes = totalEstimatedNativeBytes;
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
    public NodeIndicesStats getIndices() {
        return this.indices;
    }

    /**
     * Operating System level statistics.
     */
    @Nullable
    public OsStats getOs() {
        return this.os;
    }

    /**
     * Process level statistics.
     */
    @Nullable
    public ProcessStats getProcess() {
        return process;
    }

    /**
     * JVM level statistics.
     */
    @Nullable
    public JvmStats getJvm() {
        return jvm;
    }

    /**
     * Thread Pool level statistics.
     */
    @Nullable
    public ThreadPoolStats getThreadPool() {
        return this.threadPool;
    }

    /**
     * File system level stats.
     */
    @Nullable
    public FsInfo getFs() {
        return fs;
    }

    @Nullable
    public TransportStats getTransport() {
        return this.transport;
    }

    @Nullable
    public HttpStats getHttp() {
        return this.http;
    }

    @Nullable
    public AllCircuitBreakerStats getBreaker() {
        return this.breaker;
    }

    @Nullable
    public ScriptStats getScriptStats() {
        return this.scriptStats;
    }

    @Nullable
    public DiscoveryStats getDiscoveryStats() {
        return this.discoveryStats;
    }

    @Nullable
    public IngestStats getIngestStats() {
        return ingestStats;
    }

    @Nullable
    public AdaptiveSelectionStats getAdaptiveSelectionStats() {
        return adaptiveSelectionStats;
    }

    @Nullable
    public NodesResourceUsageStats getResourceUsageStats() {
        return resourceUsageStats;
    }

    @Nullable
    public ScriptCacheStats getScriptCacheStats() {
        return scriptCacheStats;
    }

    @Nullable
    public IndexingPressureStats getIndexingPressureStats() {
        return indexingPressureStats;
    }

    @Nullable
    public ShardIndexingPressureStats getShardIndexingPressureStats() {
        return shardIndexingPressureStats;
    }

    @Nullable
    public SearchBackpressureStats getSearchBackpressureStats() {
        return searchBackpressureStats;
    }

    @Nullable
    public ClusterManagerThrottlingStats getClusterManagerThrottlingStats() {
        return clusterManagerThrottlingStats;
    }

    public WeightedRoutingStats getWeightedRoutingStats() {
        return weightedRoutingStats;
    }

    public AggregateFileCacheStats getFileCacheStats() {
        return fileCacheStats;
    }

    @Nullable
    public AggregateFileCacheStats getFileCacheOnlyStats() {
        return fileCacheOnlyStats;
    }

    @Nullable
    public BlockCacheStats getBlockCacheOnlyStats() {
        return blockCacheOnlyStats;
    }

    @Nullable
    public TaskCancellationStats getTaskCancellationStats() {
        return taskCancellationStats;
    }

    @Nullable
    public SearchPipelineStats getSearchPipelineStats() {
        return searchPipelineStats;
    }

    @Nullable
    public SegmentReplicationRejectionStats getSegmentReplicationRejectionStats() {
        return segmentReplicationRejectionStats;
    }

    @Nullable
    public RepositoriesStats getRepositoriesStats() {
        return repositoriesStats;
    }

    @Nullable
    public AdmissionControlStats getAdmissionControlStats() {
        return admissionControlStats;
    }

    @Nullable
    public NodeCacheStats getNodeCacheStats() {
        return nodeCacheStats;
    }

    @Nullable
    public RemoteStoreNodeStats getRemoteStoreNodeStats() {
        return remoteStoreNodeStats;
    }

    /**
     * Returns the native allocator pool stats (Arrow allocator), or {@code null} if not available.
     */
    @Nullable
    public NativeAllocatorPoolStats getNativeAllocatorStats() {
        return nativeAllocatorStats;
    }

    /**
     * Returns the process-level native-memory estimate captured on this node
     * (RssAnon - JVM heap committed - JVM non-heap committed), or {@code -1} when the probe
     * could not read {@code /proc/self/status}.
     */
    public long getTotalEstimatedNativeBytes() {
        return totalEstimatedNativeBytes;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
        out.writeVLong(timestamp);
        if (indices == null) {
            out.writeBoolean(false);
        } else {
            out.writeBoolean(true);
            indices.writeTo(out);
        }
        out.writeOptionalWriteable(os);
        out.writeOptionalWriteable(process);
        out.writeOptionalWriteable(jvm);
        out.writeOptionalWriteable(threadPool);
        out.writeOptionalWriteable(fs);
        out.writeOptionalWriteable(transport);
        out.writeOptionalWriteable(http);
        out.writeOptionalWriteable(breaker);
        out.writeOptionalWriteable(scriptStats);
        out.writeOptionalWriteable(discoveryStats);
        out.writeOptionalWriteable(ingestStats);
        out.writeOptionalWriteable(adaptiveSelectionStats);
        out.writeOptionalWriteable(indexingPressureStats);
        out.writeOptionalWriteable(shardIndexingPressureStats);

        if (out.getVersion().onOrAfter(Version.V_2_4_0)) {
            out.writeOptionalWriteable(searchBackpressureStats);
        }
        if (out.getVersion().onOrAfter(Version.V_2_6_0)) {
            out.writeOptionalWriteable(clusterManagerThrottlingStats);
        }
        if (out.getVersion().onOrAfter(Version.V_2_6_0)) {
            out.writeOptionalWriteable(weightedRoutingStats);
        }
        if (out.getVersion().onOrAfter(Version.V_2_7_0)) {
            out.writeOptionalWriteable(fileCacheStats);
        }
        if (out.getVersion().onOrAfter(Version.V_3_7_0)) {
            out.writeOptionalWriteable(fileCacheOnlyStats);
            out.writeOptionalWriteable(blockCacheOnlyStats);
        }
        if (out.getVersion().onOrAfter(Version.V_2_9_0)) {
            out.writeOptionalWriteable(taskCancellationStats);
        }
        if (out.getVersion().onOrAfter(Version.V_2_9_0)) {
            out.writeOptionalWriteable(searchPipelineStats);
        }
        if (out.getVersion().onOrAfter(Version.V_2_12_0)) {
            out.writeOptionalWriteable(resourceUsageStats);
        }
        if (out.getVersion().onOrAfter(Version.V_2_12_0)) {
            out.writeOptionalWriteable(segmentReplicationRejectionStats);
        }
        if (out.getVersion().onOrAfter(Version.V_2_12_0)) {
            out.writeOptionalWriteable(repositoriesStats);
        }
        if (out.getVersion().onOrAfter(Version.V_2_12_0)) {
            out.writeOptionalWriteable(admissionControlStats);
        }
        if (out.getVersion().onOrAfter(Version.V_2_14_0)) {
            out.writeOptionalWriteable(nodeCacheStats);
        }
        if (out.getVersion().onOrAfter(Version.V_2_18_0)) {
            out.writeOptionalWriteable(remoteStoreNodeStats);
        }
        if (out.getVersion().onOrAfter(Version.V_3_8_0)) {
            out.writeOptionalWriteable(nativeAllocatorStats);
        } else if (out.getVersion().onOrAfter(Version.V_3_7_0)) {
            // BWC: write old-format NativeAllocatorPoolStats for V_3_7_0 nodes
            NativeAllocatorPoolStats.writeV3_7(out, nativeAllocatorStats);
        }
        if (out.getVersion().onOrAfter(Version.V_3_7_0)) {
            // BWC: V_3_7_0 expects AnalyticsBackendNativeMemoryStats here; write null.
            out.writeOptionalWriteable(null);
        }
        if (out.getVersion().onOrAfter(Version.V_3_7_0)) {
            out.writeLong(totalEstimatedNativeBytes);
        }
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
        if (getScriptCacheStats() != null) {
            getScriptCacheStats().toXContent(builder, params);
        }
        if (getIndexingPressureStats() != null) {
            getIndexingPressureStats().toXContent(builder, params);
        }
        if (getShardIndexingPressureStats() != null) {
            getShardIndexingPressureStats().toXContent(builder, params);
        }
        if (getSearchBackpressureStats() != null) {
            getSearchBackpressureStats().toXContent(builder, params);
        }
        if (getClusterManagerThrottlingStats() != null) {
            getClusterManagerThrottlingStats().toXContent(builder, params);
        }
        if (getWeightedRoutingStats() != null) {
            getWeightedRoutingStats().toXContent(builder, params);
        }
        if (getFileCacheStats() != null) {
            getFileCacheStats().toXContent(builder, params);
        }
        if (getFileCacheOnlyStats() != null) {
            builder.startObject("file_cache");
            getFileCacheOnlyStats().getOverallFileCacheStats().toXContent(builder, params);
            getFileCacheOnlyStats().getFullFileCacheStats().toXContent(builder, params);
            getFileCacheOnlyStats().getBlockFileCacheStats().toXContent(builder, params);
            getFileCacheOnlyStats().getPinnedFileCacheStats().toXContent(builder, params);
            builder.endObject();
        }
        if (getBlockCacheOnlyStats() != null) {
            getBlockCacheOnlyStats().toXContent(builder, params);
        }
        if (getTaskCancellationStats() != null) {
            getTaskCancellationStats().toXContent(builder, params);
        }
        if (getSearchPipelineStats() != null) {
            getSearchPipelineStats().toXContent(builder, params);
        }
        if (getResourceUsageStats() != null) {
            getResourceUsageStats().toXContent(builder, params);
        }
        if (getSegmentReplicationRejectionStats() != null) {
            getSegmentReplicationRejectionStats().toXContent(builder, params);
        }

        if (getRepositoriesStats() != null) {
            getRepositoriesStats().toXContent(builder, params);
        }
        if (getAdmissionControlStats() != null) {
            getAdmissionControlStats().toXContent(builder, params);
        }
        if (getNodeCacheStats() != null) {
            getNodeCacheStats().toXContent(builder, params);
        }
        if (getRemoteStoreNodeStats() != null) {
            getRemoteStoreNodeStats().toXContent(builder, params);
        }
        // total_estimated_bytes ≈ RssAnon - JVM heap committed - JVM non-heap committed.
        // native_memory: unified view of all native memory pools and jemalloc stats.
        // NativeAllocatorPoolStats now includes jemalloc allocated/resident + all pools.
        builder.startObject("native_memory");
        builder.field("total_estimated_bytes", totalEstimatedNativeBytes);
        if (getNativeAllocatorStats() != null) {
            NativeAllocatorPoolStats stats = getNativeAllocatorStats();
            builder.startObject("runtime");
            builder.field("allocated_bytes", stats.getNativeAllocatedBytes());
            builder.field("resident_bytes", stats.getNativeResidentBytes());
            builder.endObject();
            builder.startObject("memory_pools");
            for (var entry : stats.getGroupedStats().entrySet()) {
                entry.getValue().toXContent(builder, params);
            }
            builder.endObject();
        }
        builder.endObject();
        return builder;
    }
}
