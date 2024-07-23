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

package org.opensearch.action.admin.cluster.node.stats;

import org.opensearch.Version;
import org.opensearch.action.admin.indices.stats.CommonStats;
import org.opensearch.action.admin.indices.stats.CommonStatsFlags;
import org.opensearch.action.admin.indices.stats.IndexShardStats;
import org.opensearch.action.admin.indices.stats.ShardStats;
import org.opensearch.action.search.SearchRequestStats;
import org.opensearch.cluster.coordination.PendingClusterStateStats;
import org.opensearch.cluster.coordination.PersistedStateStats;
import org.opensearch.cluster.coordination.PublishClusterStateStats;
import org.opensearch.cluster.node.DiscoveryNode;
import org.opensearch.cluster.routing.ShardRouting;
import org.opensearch.cluster.routing.ShardRoutingState;
import org.opensearch.cluster.routing.TestShardRouting;
import org.opensearch.cluster.routing.WeightedRoutingStats;
import org.opensearch.cluster.service.ClusterManagerThrottlingStats;
import org.opensearch.cluster.service.ClusterStateStats;
import org.opensearch.common.cache.CacheType;
import org.opensearch.common.cache.service.NodeCacheStats;
import org.opensearch.common.cache.stats.CacheStats;
import org.opensearch.common.cache.stats.DefaultCacheStatsHolder;
import org.opensearch.common.cache.stats.DefaultCacheStatsHolderTests;
import org.opensearch.common.cache.stats.ImmutableCacheStatsHolder;
import org.opensearch.common.io.stream.BytesStreamOutput;
import org.opensearch.common.metrics.OperationStats;
import org.opensearch.common.settings.ClusterSettings;
import org.opensearch.common.settings.Settings;
import org.opensearch.common.xcontent.XContentFactory;
import org.opensearch.common.xcontent.XContentHelper;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.index.Index;
import org.opensearch.core.index.shard.ShardId;
import org.opensearch.core.indices.breaker.AllCircuitBreakerStats;
import org.opensearch.core.indices.breaker.CircuitBreakerStats;
import org.opensearch.core.xcontent.ToXContent;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.discovery.DiscoveryStats;
import org.opensearch.gateway.remote.RemotePersistenceStats;
import org.opensearch.http.HttpStats;
import org.opensearch.index.ReplicationStats;
import org.opensearch.index.SegmentReplicationRejectionStats;
import org.opensearch.index.cache.query.QueryCacheStats;
import org.opensearch.index.engine.SegmentsStats;
import org.opensearch.index.fielddata.FieldDataStats;
import org.opensearch.index.flush.FlushStats;
import org.opensearch.index.remote.RemoteSegmentStats;
import org.opensearch.index.remote.RemoteTranslogTransferTracker;
import org.opensearch.index.shard.DocsStats;
import org.opensearch.index.shard.IndexingStats;
import org.opensearch.index.shard.ShardPath;
import org.opensearch.index.store.StoreStats;
import org.opensearch.index.translog.RemoteTranslogStats;
import org.opensearch.indices.NodeIndicesStats;
import org.opensearch.ingest.IngestStats;
import org.opensearch.monitor.fs.FsInfo;
import org.opensearch.monitor.jvm.JvmStats;
import org.opensearch.monitor.os.OsStats;
import org.opensearch.monitor.process.ProcessStats;
import org.opensearch.node.AdaptiveSelectionStats;
import org.opensearch.node.IoUsageStats;
import org.opensearch.node.NodeResourceUsageStats;
import org.opensearch.node.NodesResourceUsageStats;
import org.opensearch.node.ResponseCollectorService;
import org.opensearch.ratelimitting.admissioncontrol.controllers.AdmissionController;
import org.opensearch.ratelimitting.admissioncontrol.controllers.CpuBasedAdmissionController;
import org.opensearch.ratelimitting.admissioncontrol.enums.AdmissionControlActionType;
import org.opensearch.ratelimitting.admissioncontrol.stats.AdmissionControlStats;
import org.opensearch.ratelimitting.admissioncontrol.stats.AdmissionControllerStats;
import org.opensearch.script.ScriptCacheStats;
import org.opensearch.script.ScriptStats;
import org.opensearch.search.suggest.completion.CompletionStats;
import org.opensearch.test.OpenSearchTestCase;
import org.opensearch.test.VersionUtils;
import org.opensearch.threadpool.ThreadPoolStats;
import org.opensearch.transport.TransportStats;

import java.io.IOException;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.TreeMap;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.StreamSupport;

import static java.util.Collections.emptyMap;
import static java.util.Collections.emptySet;
import static org.hamcrest.CoreMatchers.is;
import static org.hamcrest.collection.IsCollectionWithSize.hasSize;

public class NodeStatsTests extends OpenSearchTestCase {
    public void testSerialization() throws IOException {
        NodeStats nodeStats = createNodeStats(true);
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            nodeStats.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                NodeStats deserializedNodeStats = new NodeStats(in);
                assertEquals(nodeStats.getNode(), deserializedNodeStats.getNode());
                assertEquals(nodeStats.getTimestamp(), deserializedNodeStats.getTimestamp());
                if (nodeStats.getOs() == null) {
                    assertNull(deserializedNodeStats.getOs());
                } else {
                    assertEquals(nodeStats.getOs().getTimestamp(), deserializedNodeStats.getOs().getTimestamp());
                    assertEquals(nodeStats.getOs().getSwap().getFree(), deserializedNodeStats.getOs().getSwap().getFree());
                    assertEquals(nodeStats.getOs().getSwap().getTotal(), deserializedNodeStats.getOs().getSwap().getTotal());
                    assertEquals(nodeStats.getOs().getSwap().getUsed(), deserializedNodeStats.getOs().getSwap().getUsed());
                    assertEquals(nodeStats.getOs().getMem().getFree(), deserializedNodeStats.getOs().getMem().getFree());
                    assertEquals(nodeStats.getOs().getMem().getTotal(), deserializedNodeStats.getOs().getMem().getTotal());
                    assertEquals(nodeStats.getOs().getMem().getUsed(), deserializedNodeStats.getOs().getMem().getUsed());
                    assertEquals(nodeStats.getOs().getMem().getFreePercent(), deserializedNodeStats.getOs().getMem().getFreePercent());
                    assertEquals(nodeStats.getOs().getMem().getUsedPercent(), deserializedNodeStats.getOs().getMem().getUsedPercent());
                    assertEquals(nodeStats.getOs().getCpu().getPercent(), deserializedNodeStats.getOs().getCpu().getPercent());
                    assertEquals(
                        nodeStats.getOs().getCgroup().getCpuAcctControlGroup(),
                        deserializedNodeStats.getOs().getCgroup().getCpuAcctControlGroup()
                    );
                    assertEquals(
                        nodeStats.getOs().getCgroup().getCpuAcctUsageNanos(),
                        deserializedNodeStats.getOs().getCgroup().getCpuAcctUsageNanos()
                    );
                    assertEquals(
                        nodeStats.getOs().getCgroup().getCpuControlGroup(),
                        deserializedNodeStats.getOs().getCgroup().getCpuControlGroup()
                    );
                    assertEquals(
                        nodeStats.getOs().getCgroup().getCpuCfsPeriodMicros(),
                        deserializedNodeStats.getOs().getCgroup().getCpuCfsPeriodMicros()
                    );
                    assertEquals(
                        nodeStats.getOs().getCgroup().getCpuCfsQuotaMicros(),
                        deserializedNodeStats.getOs().getCgroup().getCpuCfsQuotaMicros()
                    );
                    assertEquals(
                        nodeStats.getOs().getCgroup().getCpuStat().getNumberOfElapsedPeriods(),
                        deserializedNodeStats.getOs().getCgroup().getCpuStat().getNumberOfElapsedPeriods()
                    );
                    assertEquals(
                        nodeStats.getOs().getCgroup().getCpuStat().getNumberOfTimesThrottled(),
                        deserializedNodeStats.getOs().getCgroup().getCpuStat().getNumberOfTimesThrottled()
                    );
                    assertEquals(
                        nodeStats.getOs().getCgroup().getCpuStat().getTimeThrottledNanos(),
                        deserializedNodeStats.getOs().getCgroup().getCpuStat().getTimeThrottledNanos()
                    );
                    assertEquals(
                        nodeStats.getOs().getCgroup().getMemoryLimitInBytes(),
                        deserializedNodeStats.getOs().getCgroup().getMemoryLimitInBytes()
                    );
                    assertEquals(
                        nodeStats.getOs().getCgroup().getMemoryUsageInBytes(),
                        deserializedNodeStats.getOs().getCgroup().getMemoryUsageInBytes()
                    );
                    assertArrayEquals(
                        nodeStats.getOs().getCpu().getLoadAverage(),
                        deserializedNodeStats.getOs().getCpu().getLoadAverage(),
                        0
                    );
                }
                if (nodeStats.getProcess() == null) {
                    assertNull(deserializedNodeStats.getProcess());
                } else {
                    assertEquals(nodeStats.getProcess().getTimestamp(), deserializedNodeStats.getProcess().getTimestamp());
                    assertEquals(nodeStats.getProcess().getCpu().getTotal(), deserializedNodeStats.getProcess().getCpu().getTotal());
                    assertEquals(nodeStats.getProcess().getCpu().getPercent(), deserializedNodeStats.getProcess().getCpu().getPercent());
                    assertEquals(
                        nodeStats.getProcess().getMem().getTotalVirtual(),
                        deserializedNodeStats.getProcess().getMem().getTotalVirtual()
                    );
                    assertEquals(
                        nodeStats.getProcess().getMaxFileDescriptors(),
                        deserializedNodeStats.getProcess().getMaxFileDescriptors()
                    );
                    assertEquals(
                        nodeStats.getProcess().getOpenFileDescriptors(),
                        deserializedNodeStats.getProcess().getOpenFileDescriptors()
                    );
                }
                JvmStats jvm = nodeStats.getJvm();
                JvmStats deserializedJvm = deserializedNodeStats.getJvm();
                if (jvm == null) {
                    assertNull(deserializedJvm);
                } else {
                    JvmStats.Mem mem = jvm.getMem();
                    JvmStats.Mem deserializedMem = deserializedJvm.getMem();
                    assertEquals(jvm.getTimestamp(), deserializedJvm.getTimestamp());
                    assertEquals(mem.getHeapUsedPercent(), deserializedMem.getHeapUsedPercent());
                    assertEquals(mem.getHeapUsed(), deserializedMem.getHeapUsed());
                    assertEquals(mem.getHeapCommitted(), deserializedMem.getHeapCommitted());
                    assertEquals(mem.getNonHeapCommitted(), deserializedMem.getNonHeapCommitted());
                    assertEquals(mem.getNonHeapUsed(), deserializedMem.getNonHeapUsed());
                    assertEquals(mem.getHeapMax(), deserializedMem.getHeapMax());

                    final Map<String, JvmStats.MemoryPool> pools = StreamSupport.stream(mem.spliterator(), false)
                        .collect(Collectors.toMap(JvmStats.MemoryPool::getName, Function.identity()));

                    final Map<String, JvmStats.MemoryPool> deserializedPools = StreamSupport.stream(deserializedMem.spliterator(), false)
                        .collect(Collectors.toMap(JvmStats.MemoryPool::getName, Function.identity()));

                    final int poolsCount = (int) StreamSupport.stream(nodeStats.getJvm().getMem().spliterator(), false).count();
                    assertThat(pools.keySet(), hasSize(poolsCount));
                    assertThat(deserializedPools.keySet(), hasSize(poolsCount));

                    for (final Map.Entry<String, JvmStats.MemoryPool> entry : pools.entrySet()) {
                        assertThat(deserializedPools.containsKey(entry.getKey()), is(true));
                        assertEquals(entry.getValue().getName(), deserializedPools.get(entry.getKey()).getName());
                        assertEquals(entry.getValue().getMax(), deserializedPools.get(entry.getKey()).getMax());
                        assertEquals(entry.getValue().getPeakMax(), deserializedPools.get(entry.getKey()).getPeakMax());
                        assertEquals(entry.getValue().getPeakUsed(), deserializedPools.get(entry.getKey()).getPeakUsed());
                        assertEquals(entry.getValue().getUsed(), deserializedPools.get(entry.getKey()).getUsed());

                        assertEquals(
                            entry.getValue().getLastGcStats().getUsed(),
                            deserializedPools.get(entry.getKey()).getLastGcStats().getUsed()
                        );
                        assertEquals(
                            entry.getValue().getLastGcStats().getMax(),
                            deserializedPools.get(entry.getKey()).getLastGcStats().getMax()
                        );
                        assertEquals(
                            entry.getValue().getLastGcStats().getUsagePercent(),
                            deserializedPools.get(entry.getKey()).getLastGcStats().getUsagePercent()
                        );
                    }

                    JvmStats.Classes classes = jvm.getClasses();
                    assertEquals(classes.getLoadedClassCount(), deserializedJvm.getClasses().getLoadedClassCount());
                    assertEquals(classes.getTotalLoadedClassCount(), deserializedJvm.getClasses().getTotalLoadedClassCount());
                    assertEquals(classes.getUnloadedClassCount(), deserializedJvm.getClasses().getUnloadedClassCount());
                    assertEquals(jvm.getGc().getCollectors().length, deserializedJvm.getGc().getCollectors().length);
                    for (int i = 0; i < jvm.getGc().getCollectors().length; i++) {
                        JvmStats.GarbageCollector garbageCollector = jvm.getGc().getCollectors()[i];
                        JvmStats.GarbageCollector deserializedGarbageCollector = deserializedJvm.getGc().getCollectors()[i];
                        assertEquals(garbageCollector.getName(), deserializedGarbageCollector.getName());
                        assertEquals(garbageCollector.getCollectionCount(), deserializedGarbageCollector.getCollectionCount());
                        assertEquals(garbageCollector.getCollectionTime(), deserializedGarbageCollector.getCollectionTime());
                    }
                    assertEquals(jvm.getThreads().getCount(), deserializedJvm.getThreads().getCount());
                    assertEquals(jvm.getThreads().getPeakCount(), deserializedJvm.getThreads().getPeakCount());
                    assertEquals(jvm.getUptime(), deserializedJvm.getUptime());
                    if (jvm.getBufferPools() == null) {
                        assertNull(deserializedJvm.getBufferPools());
                    } else {
                        assertEquals(jvm.getBufferPools().size(), deserializedJvm.getBufferPools().size());
                        for (int i = 0; i < jvm.getBufferPools().size(); i++) {
                            JvmStats.BufferPool bufferPool = jvm.getBufferPools().get(i);
                            JvmStats.BufferPool deserializedBufferPool = deserializedJvm.getBufferPools().get(i);
                            assertEquals(bufferPool.getName(), deserializedBufferPool.getName());
                            assertEquals(bufferPool.getCount(), deserializedBufferPool.getCount());
                            assertEquals(bufferPool.getTotalCapacity(), deserializedBufferPool.getTotalCapacity());
                            assertEquals(bufferPool.getUsed(), deserializedBufferPool.getUsed());
                        }
                    }
                }
                if (nodeStats.getThreadPool() == null) {
                    assertNull(deserializedNodeStats.getThreadPool());
                } else {
                    Iterator<ThreadPoolStats.Stats> threadPoolIterator = nodeStats.getThreadPool().iterator();
                    Iterator<ThreadPoolStats.Stats> deserializedThreadPoolIterator = deserializedNodeStats.getThreadPool().iterator();
                    while (threadPoolIterator.hasNext()) {
                        ThreadPoolStats.Stats stats = threadPoolIterator.next();
                        ThreadPoolStats.Stats deserializedStats = deserializedThreadPoolIterator.next();
                        assertEquals(stats.getName(), deserializedStats.getName());
                        assertEquals(stats.getThreads(), deserializedStats.getThreads());
                        assertEquals(stats.getActive(), deserializedStats.getActive());
                        assertEquals(stats.getLargest(), deserializedStats.getLargest());
                        assertEquals(stats.getCompleted(), deserializedStats.getCompleted());
                        assertEquals(stats.getQueue(), deserializedStats.getQueue());
                        assertEquals(stats.getRejected(), deserializedStats.getRejected());
                    }
                }
                FsInfo fs = nodeStats.getFs();
                FsInfo deserializedFs = deserializedNodeStats.getFs();
                if (fs == null) {
                    assertNull(deserializedFs);
                } else {
                    assertEquals(fs.getTimestamp(), deserializedFs.getTimestamp());
                    assertEquals(fs.getTotal().getAvailable(), deserializedFs.getTotal().getAvailable());
                    assertEquals(fs.getTotal().getTotal(), deserializedFs.getTotal().getTotal());
                    assertEquals(fs.getTotal().getFree(), deserializedFs.getTotal().getFree());
                    assertEquals(fs.getTotal().getMount(), deserializedFs.getTotal().getMount());
                    assertEquals(fs.getTotal().getPath(), deserializedFs.getTotal().getPath());
                    assertEquals(fs.getTotal().getType(), deserializedFs.getTotal().getType());
                    FsInfo.IoStats ioStats = fs.getIoStats();
                    FsInfo.IoStats deserializedIoStats = deserializedFs.getIoStats();
                    assertEquals(ioStats.getTotalOperations(), deserializedIoStats.getTotalOperations());
                    assertEquals(ioStats.getTotalReadKilobytes(), deserializedIoStats.getTotalReadKilobytes());
                    assertEquals(ioStats.getTotalReadOperations(), deserializedIoStats.getTotalReadOperations());
                    assertEquals(ioStats.getTotalWriteKilobytes(), deserializedIoStats.getTotalWriteKilobytes());
                    assertEquals(ioStats.getTotalWriteOperations(), deserializedIoStats.getTotalWriteOperations());
                    assertEquals(ioStats.getTotalReadTime(), deserializedIoStats.getTotalReadTime());
                    assertEquals(ioStats.getTotalWriteTime(), deserializedIoStats.getTotalWriteTime());
                    assertEquals(ioStats.getTotalQueueSize(), deserializedIoStats.getTotalQueueSize());
                    assertEquals(ioStats.getTotalIOTimeMillis(), deserializedIoStats.getTotalIOTimeMillis());
                    assertEquals(ioStats.getDevicesStats().length, deserializedIoStats.getDevicesStats().length);
                    for (int i = 0; i < ioStats.getDevicesStats().length; i++) {
                        FsInfo.DeviceStats deviceStats = ioStats.getDevicesStats()[i];
                        FsInfo.DeviceStats deserializedDeviceStats = deserializedIoStats.getDevicesStats()[i];
                        assertEquals(deviceStats.operations(), deserializedDeviceStats.operations());
                        assertEquals(deviceStats.readKilobytes(), deserializedDeviceStats.readKilobytes());
                        assertEquals(deviceStats.readOperations(), deserializedDeviceStats.readOperations());
                        assertEquals(deviceStats.writeKilobytes(), deserializedDeviceStats.writeKilobytes());
                        assertEquals(deviceStats.writeOperations(), deserializedDeviceStats.writeOperations());
                    }
                }
                if (nodeStats.getTransport() == null) {
                    assertNull(deserializedNodeStats.getTransport());
                } else {
                    assertEquals(nodeStats.getTransport().getRxCount(), deserializedNodeStats.getTransport().getRxCount());
                    assertEquals(nodeStats.getTransport().getRxSize(), deserializedNodeStats.getTransport().getRxSize());
                    assertEquals(nodeStats.getTransport().getServerOpen(), deserializedNodeStats.getTransport().getServerOpen());
                    assertEquals(nodeStats.getTransport().getTxCount(), deserializedNodeStats.getTransport().getTxCount());
                    assertEquals(nodeStats.getTransport().getTxSize(), deserializedNodeStats.getTransport().getTxSize());
                }
                if (nodeStats.getHttp() == null) {
                    assertNull(deserializedNodeStats.getHttp());
                } else {
                    assertEquals(nodeStats.getHttp().getServerOpen(), deserializedNodeStats.getHttp().getServerOpen());
                    assertEquals(nodeStats.getHttp().getTotalOpen(), deserializedNodeStats.getHttp().getTotalOpen());
                }
                if (nodeStats.getBreaker() == null) {
                    assertNull(deserializedNodeStats.getBreaker());
                } else {
                    assertEquals(nodeStats.getBreaker().getAllStats().length, deserializedNodeStats.getBreaker().getAllStats().length);
                    for (int i = 0; i < nodeStats.getBreaker().getAllStats().length; i++) {
                        CircuitBreakerStats circuitBreakerStats = nodeStats.getBreaker().getAllStats()[i];
                        CircuitBreakerStats deserializedCircuitBreakerStats = deserializedNodeStats.getBreaker().getAllStats()[i];
                        assertEquals(circuitBreakerStats.getEstimated(), deserializedCircuitBreakerStats.getEstimated());
                        assertEquals(circuitBreakerStats.getLimit(), deserializedCircuitBreakerStats.getLimit());
                        assertEquals(circuitBreakerStats.getName(), deserializedCircuitBreakerStats.getName());
                        assertEquals(circuitBreakerStats.getOverhead(), deserializedCircuitBreakerStats.getOverhead(), 0);
                        assertEquals(circuitBreakerStats.getTrippedCount(), deserializedCircuitBreakerStats.getTrippedCount(), 0);
                    }
                }
                ScriptStats scriptStats = nodeStats.getScriptStats();
                if (scriptStats == null) {
                    assertNull(deserializedNodeStats.getScriptStats());
                } else {
                    assertEquals(scriptStats.getCacheEvictions(), deserializedNodeStats.getScriptStats().getCacheEvictions());
                    assertEquals(scriptStats.getCompilations(), deserializedNodeStats.getScriptStats().getCompilations());
                }
                DiscoveryStats discoveryStats = nodeStats.getDiscoveryStats();
                DiscoveryStats deserializedDiscoveryStats = deserializedNodeStats.getDiscoveryStats();
                if (discoveryStats == null) {
                    assertNull(deserializedDiscoveryStats);
                } else {
                    PendingClusterStateStats queueStats = discoveryStats.getQueueStats();
                    if (queueStats == null) {
                        assertNull(deserializedDiscoveryStats.getQueueStats());
                    } else {
                        assertEquals(queueStats.getCommitted(), deserializedDiscoveryStats.getQueueStats().getCommitted());
                        assertEquals(queueStats.getTotal(), deserializedDiscoveryStats.getQueueStats().getTotal());
                        assertEquals(queueStats.getPending(), deserializedDiscoveryStats.getQueueStats().getPending());
                    }
                    ClusterStateStats stateStats = discoveryStats.getClusterStateStats();
                    if (stateStats == null) {
                        assertNull(deserializedDiscoveryStats.getClusterStateStats());
                    } else {
                        assertEquals(stateStats.getUpdateFailed(), deserializedDiscoveryStats.getClusterStateStats().getUpdateFailed());
                        assertEquals(stateStats.getUpdateSuccess(), deserializedDiscoveryStats.getClusterStateStats().getUpdateSuccess());
                        assertEquals(
                            stateStats.getUpdateTotalTimeInMillis(),
                            deserializedDiscoveryStats.getClusterStateStats().getUpdateTotalTimeInMillis()
                        );
                        assertEquals(1, deserializedDiscoveryStats.getClusterStateStats().getPersistenceStats().size());
                        PersistedStateStats deserializedRemoteStateStats = deserializedDiscoveryStats.getClusterStateStats()
                            .getPersistenceStats()
                            .get(0);
                        PersistedStateStats remoteStateStats = stateStats.getPersistenceStats().get(0);
                        assertEquals(remoteStateStats.getStatsName(), deserializedRemoteStateStats.getStatsName());
                        assertEquals(remoteStateStats.getFailedCount(), deserializedRemoteStateStats.getFailedCount());
                        assertEquals(remoteStateStats.getSuccessCount(), deserializedRemoteStateStats.getSuccessCount());
                        assertEquals(remoteStateStats.getTotalTimeInMillis(), deserializedRemoteStateStats.getTotalTimeInMillis());
                    }
                }
                IngestStats ingestStats = nodeStats.getIngestStats();
                IngestStats deserializedIngestStats = deserializedNodeStats.getIngestStats();
                if (ingestStats == null) {
                    assertNull(deserializedIngestStats);
                } else {
                    OperationStats totalStats = ingestStats.getTotalStats();
                    assertEquals(totalStats.getCount(), deserializedIngestStats.getTotalStats().getCount());
                    assertEquals(totalStats.getCurrent(), deserializedIngestStats.getTotalStats().getCurrent());
                    assertEquals(totalStats.getFailedCount(), deserializedIngestStats.getTotalStats().getFailedCount());
                    assertEquals(totalStats.getTotalTimeInMillis(), deserializedIngestStats.getTotalStats().getTotalTimeInMillis());
                    assertEquals(ingestStats.getPipelineStats().size(), deserializedIngestStats.getPipelineStats().size());
                    for (IngestStats.PipelineStat pipelineStat : ingestStats.getPipelineStats()) {
                        String pipelineId = pipelineStat.getPipelineId();
                        OperationStats deserializedPipelineStats = getPipelineStats(deserializedIngestStats.getPipelineStats(), pipelineId);
                        assertEquals(pipelineStat.getStats().getFailedCount(), deserializedPipelineStats.getFailedCount());
                        assertEquals(pipelineStat.getStats().getTotalTimeInMillis(), deserializedPipelineStats.getTotalTimeInMillis());
                        assertEquals(pipelineStat.getStats().getCurrent(), deserializedPipelineStats.getCurrent());
                        assertEquals(pipelineStat.getStats().getCount(), deserializedPipelineStats.getCount());
                        List<IngestStats.ProcessorStat> processorStats = ingestStats.getProcessorStats().get(pipelineId);
                        // intentionally validating identical order
                        Iterator<IngestStats.ProcessorStat> it = deserializedIngestStats.getProcessorStats().get(pipelineId).iterator();
                        for (IngestStats.ProcessorStat processorStat : processorStats) {
                            IngestStats.ProcessorStat deserializedProcessorStat = it.next();
                            assertEquals(processorStat.getStats().getFailedCount(), deserializedProcessorStat.getStats().getFailedCount());
                            assertEquals(
                                processorStat.getStats().getTotalTimeInMillis(),
                                deserializedProcessorStat.getStats().getTotalTimeInMillis()
                            );
                            assertEquals(processorStat.getStats().getCurrent(), deserializedProcessorStat.getStats().getCurrent());
                            assertEquals(processorStat.getStats().getCount(), deserializedProcessorStat.getStats().getCount());
                        }
                        assertFalse(it.hasNext());
                    }
                }
                AdaptiveSelectionStats adaptiveStats = nodeStats.getAdaptiveSelectionStats();
                AdaptiveSelectionStats deserializedAdaptiveStats = deserializedNodeStats.getAdaptiveSelectionStats();
                if (adaptiveStats == null) {
                    assertNull(deserializedAdaptiveStats);
                } else {
                    assertEquals(adaptiveStats.getOutgoingConnections(), deserializedAdaptiveStats.getOutgoingConnections());
                    assertEquals(adaptiveStats.getRanks(), deserializedAdaptiveStats.getRanks());
                    adaptiveStats.getComputedStats().forEach((k, v) -> {
                        ResponseCollectorService.ComputedNodeStats aStats = adaptiveStats.getComputedStats().get(k);
                        ResponseCollectorService.ComputedNodeStats bStats = deserializedAdaptiveStats.getComputedStats().get(k);
                        assertEquals(aStats.nodeId, bStats.nodeId);
                        assertEquals(aStats.queueSize, bStats.queueSize, 0.01);
                        assertEquals(aStats.serviceTime, bStats.serviceTime, 0.01);
                        assertEquals(aStats.responseTime, bStats.responseTime, 0.01);
                    });
                }
                NodesResourceUsageStats resourceUsageStats = nodeStats.getResourceUsageStats();
                NodesResourceUsageStats deserializedResourceUsageStats = deserializedNodeStats.getResourceUsageStats();
                if (resourceUsageStats == null) {
                    assertNull(deserializedResourceUsageStats);
                } else {
                    resourceUsageStats.getNodeIdToResourceUsageStatsMap().forEach((k, v) -> {
                        NodeResourceUsageStats aResourceUsageStats = resourceUsageStats.getNodeIdToResourceUsageStatsMap().get(k);
                        NodeResourceUsageStats bResourceUsageStats = deserializedResourceUsageStats.getNodeIdToResourceUsageStatsMap()
                            .get(k);
                        assertEquals(
                            aResourceUsageStats.getMemoryUtilizationPercent(),
                            bResourceUsageStats.getMemoryUtilizationPercent(),
                            0.0
                        );
                        assertEquals(aResourceUsageStats.getCpuUtilizationPercent(), bResourceUsageStats.getCpuUtilizationPercent(), 0.0);
                        assertEquals(aResourceUsageStats.getTimestamp(), bResourceUsageStats.getTimestamp());
                    });
                }
                SegmentReplicationRejectionStats segmentReplicationRejectionStats = nodeStats.getSegmentReplicationRejectionStats();
                SegmentReplicationRejectionStats deserializedSegmentReplicationRejectionStats = deserializedNodeStats
                    .getSegmentReplicationRejectionStats();
                if (segmentReplicationRejectionStats == null) {
                    assertNull(deserializedSegmentReplicationRejectionStats);
                } else {
                    assertEquals(
                        segmentReplicationRejectionStats.getTotalRejectionCount(),
                        deserializedSegmentReplicationRejectionStats.getTotalRejectionCount()
                    );
                }
                ScriptCacheStats scriptCacheStats = nodeStats.getScriptCacheStats();
                ScriptCacheStats deserializedScriptCacheStats = deserializedNodeStats.getScriptCacheStats();
                if (scriptCacheStats == null) {
                    assertNull(deserializedScriptCacheStats);
                } else if (deserializedScriptCacheStats.getContextStats() != null) {
                    Map<String, ScriptStats> deserialized = deserializedScriptCacheStats.getContextStats();
                    long evictions = 0;
                    long limited = 0;
                    long compilations = 0;
                    Map<String, ScriptStats> stats = scriptCacheStats.getContextStats();
                    for (String context : stats.keySet()) {
                        ScriptStats deserStats = deserialized.get(context);
                        ScriptStats generatedStats = stats.get(context);

                        evictions += generatedStats.getCacheEvictions();
                        assertEquals(generatedStats.getCacheEvictions(), deserStats.getCacheEvictions());

                        limited += generatedStats.getCompilationLimitTriggered();
                        assertEquals(generatedStats.getCompilationLimitTriggered(), deserStats.getCompilationLimitTriggered());

                        compilations += generatedStats.getCompilations();
                        assertEquals(generatedStats.getCompilations(), deserStats.getCompilations());
                    }
                    ScriptStats sum = deserializedScriptCacheStats.sum();
                    assertEquals(evictions, sum.getCacheEvictions());
                    assertEquals(limited, sum.getCompilationLimitTriggered());
                    assertEquals(compilations, sum.getCompilations());
                }
                ClusterManagerThrottlingStats clusterManagerThrottlingStats = nodeStats.getClusterManagerThrottlingStats();
                ClusterManagerThrottlingStats deserializedClusterManagerThrottlingStats = deserializedNodeStats
                    .getClusterManagerThrottlingStats();
                if (clusterManagerThrottlingStats == null) {
                    assertNull(deserializedClusterManagerThrottlingStats);
                } else {
                    assertEquals(
                        clusterManagerThrottlingStats.getTotalThrottledTaskCount(),
                        deserializedClusterManagerThrottlingStats.getTotalThrottledTaskCount()
                    );
                    assertEquals(
                        clusterManagerThrottlingStats.getThrottlingCount("test-task"),
                        deserializedClusterManagerThrottlingStats.getThrottlingCount("test-task")
                    );
                }

                WeightedRoutingStats weightedRoutingStats = nodeStats.getWeightedRoutingStats();
                WeightedRoutingStats deserializedWeightedRoutingStats = deserializedNodeStats.getWeightedRoutingStats();
                if (weightedRoutingStats == null) {
                    assertNull(deserializedWeightedRoutingStats);
                } else {
                    assertEquals(weightedRoutingStats.getFailOpenCount(), deserializedWeightedRoutingStats.getFailOpenCount());

                }

                NodeIndicesStats nodeIndicesStats = nodeStats.getIndices();
                NodeIndicesStats deserializedNodeIndicesStats = deserializedNodeStats.getIndices();
                if (nodeIndicesStats == null) {
                    assertNull(deserializedNodeIndicesStats);
                } else {
                    RemoteSegmentStats remoteSegmentStats = nodeIndicesStats.getSegments().getRemoteSegmentStats();
                    RemoteSegmentStats deserializedRemoteSegmentStats = deserializedNodeIndicesStats.getSegments().getRemoteSegmentStats();
                    assertEquals(remoteSegmentStats.getDownloadBytesStarted(), deserializedRemoteSegmentStats.getDownloadBytesStarted());
                    assertEquals(
                        remoteSegmentStats.getDownloadBytesSucceeded(),
                        deserializedRemoteSegmentStats.getDownloadBytesSucceeded()
                    );
                    assertEquals(remoteSegmentStats.getDownloadBytesFailed(), deserializedRemoteSegmentStats.getDownloadBytesFailed());
                    assertEquals(remoteSegmentStats.getUploadBytesStarted(), deserializedRemoteSegmentStats.getUploadBytesStarted());
                    assertEquals(remoteSegmentStats.getUploadBytesSucceeded(), deserializedRemoteSegmentStats.getUploadBytesSucceeded());
                    assertEquals(remoteSegmentStats.getUploadBytesFailed(), deserializedRemoteSegmentStats.getUploadBytesFailed());
                    assertEquals(remoteSegmentStats.getMaxRefreshTimeLag(), deserializedRemoteSegmentStats.getMaxRefreshTimeLag());
                    assertEquals(remoteSegmentStats.getMaxRefreshBytesLag(), deserializedRemoteSegmentStats.getMaxRefreshBytesLag());
                    assertEquals(remoteSegmentStats.getTotalRefreshBytesLag(), deserializedRemoteSegmentStats.getTotalRefreshBytesLag());
                    assertEquals(remoteSegmentStats.getTotalUploadTime(), deserializedRemoteSegmentStats.getTotalUploadTime());
                    assertEquals(remoteSegmentStats.getTotalDownloadTime(), deserializedRemoteSegmentStats.getTotalDownloadTime());

                    RemoteTranslogStats remoteTranslogStats = nodeIndicesStats.getTranslog().getRemoteTranslogStats();
                    RemoteTranslogStats deserializedRemoteTranslogStats = deserializedNodeIndicesStats.getTranslog()
                        .getRemoteTranslogStats();
                    assertEquals(remoteTranslogStats, deserializedRemoteTranslogStats);

                    ReplicationStats replicationStats = nodeIndicesStats.getSegments().getReplicationStats();

                    ReplicationStats deserializedReplicationStats = deserializedNodeIndicesStats.getSegments().getReplicationStats();
                    assertEquals(replicationStats.getMaxBytesBehind(), deserializedReplicationStats.getMaxBytesBehind());
                    assertEquals(replicationStats.getTotalBytesBehind(), deserializedReplicationStats.getTotalBytesBehind());
                    assertEquals(replicationStats.getMaxReplicationLag(), deserializedReplicationStats.getMaxReplicationLag());
                }
                AdmissionControlStats admissionControlStats = nodeStats.getAdmissionControlStats();
                AdmissionControlStats deserializedAdmissionControlStats = deserializedNodeStats.getAdmissionControlStats();
                if (admissionControlStats == null) {
                    assertNull(deserializedAdmissionControlStats);
                } else {
                    assertEquals(
                        admissionControlStats.getAdmissionControllerStatsList().size(),
                        deserializedAdmissionControlStats.getAdmissionControllerStatsList().size()
                    );
                    AdmissionControllerStats admissionControllerStats = admissionControlStats.getAdmissionControllerStatsList().get(0);
                    AdmissionControllerStats deserializedAdmissionControllerStats = deserializedAdmissionControlStats
                        .getAdmissionControllerStatsList()
                        .get(0);
                    assertEquals(
                        admissionControllerStats.getAdmissionControllerName(),
                        deserializedAdmissionControllerStats.getAdmissionControllerName()
                    );
                    assertEquals(1, (long) admissionControllerStats.getRejectionCount().get(AdmissionControlActionType.SEARCH.getType()));
                    assertEquals(
                        admissionControllerStats.getRejectionCount().get(AdmissionControlActionType.SEARCH.getType()),
                        deserializedAdmissionControllerStats.getRejectionCount().get(AdmissionControlActionType.SEARCH.getType())
                    );

                    assertEquals(2, (long) admissionControllerStats.getRejectionCount().get(AdmissionControlActionType.INDEXING.getType()));
                    assertEquals(
                        admissionControllerStats.getRejectionCount().get(AdmissionControlActionType.INDEXING.getType()),
                        deserializedAdmissionControllerStats.getRejectionCount().get(AdmissionControlActionType.INDEXING.getType())
                    );
                }
                NodeCacheStats nodeCacheStats = nodeStats.getNodeCacheStats();
                NodeCacheStats deserializedNodeCacheStats = deserializedNodeStats.getNodeCacheStats();
                if (nodeCacheStats == null) {
                    assertNull(deserializedNodeCacheStats);
                } else {
                    assertEquals(nodeCacheStats, deserializedNodeCacheStats);
                }
            }
        }
    }

    public static NodeStats createNodeStats() throws IOException {
        return createNodeStats(false);
    }

    public static NodeStats createNodeStats(boolean remoteStoreStats) throws IOException {
        DiscoveryNode node = new DiscoveryNode(
            "test_node",
            buildNewFakeTransportAddress(),
            emptyMap(),
            emptySet(),
            VersionUtils.randomVersion(random())
        );
        OsStats osStats = null;
        if (frequently()) {
            double loadAverages[] = new double[3];
            for (int i = 0; i < 3; i++) {
                loadAverages[i] = randomBoolean() ? randomDouble() : -1;
            }
            long memTotal = randomNonNegativeLong();
            long swapTotal = randomNonNegativeLong();
            osStats = new OsStats(
                System.currentTimeMillis(),
                new OsStats.Cpu(randomShort(), loadAverages),
                new OsStats.Mem(memTotal, randomLongBetween(0, memTotal)),
                new OsStats.Swap(swapTotal, randomLongBetween(0, swapTotal)),
                new OsStats.Cgroup(
                    randomAlphaOfLength(8),
                    randomNonNegativeLong(),
                    randomAlphaOfLength(8),
                    randomNonNegativeLong(),
                    randomNonNegativeLong(),
                    new OsStats.Cgroup.CpuStat(randomNonNegativeLong(), randomNonNegativeLong(), randomNonNegativeLong()),
                    randomAlphaOfLength(8),
                    Long.toString(randomNonNegativeLong()),
                    Long.toString(randomNonNegativeLong())
                )
            );
        }
        ProcessStats processStats = frequently()
            ? new ProcessStats(
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                new ProcessStats.Cpu(randomShort(), randomNonNegativeLong()),
                new ProcessStats.Mem(randomNonNegativeLong())
            )
            : null;
        JvmStats jvmStats = null;
        if (frequently()) {
            int numMemoryPools = randomIntBetween(0, 10);
            List<JvmStats.MemoryPool> memoryPools = new ArrayList<>(numMemoryPools);
            for (int i = 0; i < numMemoryPools; i++) {
                memoryPools.add(
                    new JvmStats.MemoryPool(
                        randomAlphaOfLengthBetween(3, 10),
                        randomNonNegativeLong(),
                        randomNonNegativeLong(),
                        randomNonNegativeLong(),
                        randomNonNegativeLong(),
                        new JvmStats.MemoryPoolGcStats(randomNonNegativeLong(), randomNonNegativeLong())
                    )
                );
            }
            JvmStats.Threads threads = new JvmStats.Threads(randomIntBetween(1, 1000), randomIntBetween(1, 1000));
            int numGarbageCollectors = randomIntBetween(0, 10);
            JvmStats.GarbageCollector[] garbageCollectorsArray = new JvmStats.GarbageCollector[numGarbageCollectors];
            for (int i = 0; i < numGarbageCollectors; i++) {
                garbageCollectorsArray[i] = new JvmStats.GarbageCollector(
                    randomAlphaOfLengthBetween(3, 10),
                    randomNonNegativeLong(),
                    randomNonNegativeLong()
                );
            }
            JvmStats.GarbageCollectors garbageCollectors = new JvmStats.GarbageCollectors(garbageCollectorsArray);
            int numBufferPools = randomIntBetween(0, 10);
            List<JvmStats.BufferPool> bufferPoolList = new ArrayList<>();
            for (int i = 0; i < numBufferPools; i++) {
                bufferPoolList.add(
                    new JvmStats.BufferPool(
                        randomAlphaOfLengthBetween(3, 10),
                        randomNonNegativeLong(),
                        randomNonNegativeLong(),
                        randomNonNegativeLong()
                    )
                );
            }
            JvmStats.Classes classes = new JvmStats.Classes(randomNonNegativeLong(), randomNonNegativeLong(), randomNonNegativeLong());
            jvmStats = new JvmStats(
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                new JvmStats.Mem(
                    randomNonNegativeLong(),
                    randomNonNegativeLong(),
                    randomNonNegativeLong(),
                    randomNonNegativeLong(),
                    randomNonNegativeLong(),
                    memoryPools
                ),
                threads,
                garbageCollectors,
                randomBoolean() ? Collections.emptyList() : bufferPoolList,
                classes
            );
        }
        ThreadPoolStats threadPoolStats = null;
        if (frequently()) {
            int numThreadPoolStats = randomIntBetween(0, 10);
            List<ThreadPoolStats.Stats> threadPoolStatsList = new ArrayList<>();
            for (int i = 0; i < numThreadPoolStats; i++) {
                threadPoolStatsList.add(
                    new ThreadPoolStats.Stats(
                        randomAlphaOfLengthBetween(3, 10),
                        randomIntBetween(1, 1000),
                        randomIntBetween(1, 1000),
                        randomIntBetween(1, 1000),
                        randomNonNegativeLong(),
                        randomIntBetween(1, 1000),
                        randomIntBetween(1, 1000),
                        randomIntBetween(-1, 10)
                    )
                );
            }
            threadPoolStats = new ThreadPoolStats(threadPoolStatsList);
        }
        FsInfo fsInfo = null;
        if (frequently()) {
            int numDeviceStats = randomIntBetween(0, 10);
            FsInfo.DeviceStats[] deviceStatsArray = new FsInfo.DeviceStats[numDeviceStats];
            for (int i = 0; i < numDeviceStats; i++) {
                FsInfo.DeviceStats previousDeviceStats = randomBoolean()
                    ? null
                    : new FsInfo.DeviceStats(
                        randomInt(),
                        randomInt(),
                        randomAlphaOfLengthBetween(3, 10),
                        randomNonNegativeLong(),
                        randomNonNegativeLong(),
                        randomNonNegativeLong(),
                        randomNonNegativeLong(),
                        randomNonNegativeLong(),
                        randomNonNegativeLong(),
                        randomNonNegativeLong(),
                        randomNonNegativeLong(),
                        null
                    );
                deviceStatsArray[i] = new FsInfo.DeviceStats(
                    randomInt(),
                    randomInt(),
                    randomAlphaOfLengthBetween(3, 10),
                    randomNonNegativeLong(),
                    randomNonNegativeLong(),
                    randomNonNegativeLong(),
                    randomNonNegativeLong(),
                    randomNonNegativeLong(),
                    randomNonNegativeLong(),
                    randomNonNegativeLong(),
                    randomNonNegativeLong(),
                    previousDeviceStats
                );
            }
            FsInfo.IoStats ioStats = new FsInfo.IoStats(deviceStatsArray);
            int numPaths = randomIntBetween(0, 10);
            FsInfo.Path[] paths = new FsInfo.Path[numPaths];
            for (int i = 0; i < numPaths; i++) {
                paths[i] = new FsInfo.Path(
                    randomAlphaOfLengthBetween(3, 10),
                    randomBoolean() ? randomAlphaOfLengthBetween(3, 10) : null,
                    randomNonNegativeLong(),
                    randomNonNegativeLong(),
                    randomNonNegativeLong()
                );
            }
            fsInfo = new FsInfo(randomNonNegativeLong(), ioStats, paths);
        }
        TransportStats transportStats = frequently()
            ? new TransportStats(
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                randomNonNegativeLong()
            )
            : null;
        HttpStats httpStats = frequently() ? new HttpStats(randomNonNegativeLong(), randomNonNegativeLong()) : null;
        AllCircuitBreakerStats allCircuitBreakerStats = null;
        if (frequently()) {
            int numCircuitBreakerStats = randomIntBetween(0, 10);
            CircuitBreakerStats[] circuitBreakerStatsArray = new CircuitBreakerStats[numCircuitBreakerStats];
            for (int i = 0; i < numCircuitBreakerStats; i++) {
                circuitBreakerStatsArray[i] = new CircuitBreakerStats(
                    randomAlphaOfLengthBetween(3, 10),
                    randomNonNegativeLong(),
                    randomNonNegativeLong(),
                    randomDouble(),
                    randomNonNegativeLong()
                );
            }
            allCircuitBreakerStats = new AllCircuitBreakerStats(circuitBreakerStatsArray);
        }
        ScriptStats scriptStats = frequently()
            ? new ScriptStats(randomNonNegativeLong(), randomNonNegativeLong(), randomNonNegativeLong())
            : null;
        ClusterStateStats stateStats = new ClusterStateStats();
        RemotePersistenceStats remoteStateStats = new RemotePersistenceStats();
        stateStats.setPersistenceStats(Arrays.asList(remoteStateStats));
        DiscoveryStats discoveryStats = frequently()
            ? new DiscoveryStats(
                randomBoolean() ? new PendingClusterStateStats(randomInt(), randomInt(), randomInt()) : null,
                randomBoolean()
                    ? new PublishClusterStateStats(randomNonNegativeLong(), randomNonNegativeLong(), randomNonNegativeLong())
                    : null,
                randomBoolean() ? stateStats : null
            )
            : null;
        IngestStats ingestStats = null;
        if (frequently()) {
            OperationStats totalStats = new OperationStats(
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                randomNonNegativeLong(),
                randomNonNegativeLong()
            );
            int numPipelines = randomIntBetween(0, 10);
            int numProcessors = randomIntBetween(0, 10);
            List<IngestStats.PipelineStat> ingestPipelineStats = new ArrayList<>(numPipelines);
            Map<String, List<IngestStats.ProcessorStat>> ingestProcessorStats = new HashMap<>(numPipelines);
            for (int i = 0; i < numPipelines; i++) {
                String pipelineId = randomAlphaOfLengthBetween(3, 10);
                ingestPipelineStats.add(
                    new IngestStats.PipelineStat(
                        pipelineId,
                        new OperationStats(
                            randomNonNegativeLong(),
                            randomNonNegativeLong(),
                            randomNonNegativeLong(),
                            randomNonNegativeLong()
                        )
                    )
                );

                List<IngestStats.ProcessorStat> processorPerPipeline = new ArrayList<>(numProcessors);
                for (int j = 0; j < numProcessors; j++) {
                    OperationStats processorStats = new OperationStats(
                        randomNonNegativeLong(),
                        randomNonNegativeLong(),
                        randomNonNegativeLong(),
                        randomNonNegativeLong()
                    );
                    processorPerPipeline.add(
                        new IngestStats.ProcessorStat(randomAlphaOfLengthBetween(3, 10), randomAlphaOfLengthBetween(3, 10), processorStats)
                    );
                }
                ingestProcessorStats.put(pipelineId, processorPerPipeline);
            }
            ingestStats = new IngestStats(totalStats, ingestPipelineStats, ingestProcessorStats);
        }
        AdaptiveSelectionStats adaptiveSelectionStats = null;
        if (frequently()) {
            int numNodes = randomIntBetween(0, 10);
            Map<String, Long> nodeConnections = new HashMap<>();
            Map<String, ResponseCollectorService.ComputedNodeStats> nodeStats = new HashMap<>();
            for (int i = 0; i < numNodes; i++) {
                String nodeId = randomAlphaOfLengthBetween(3, 10);
                // add outgoing connection info
                if (frequently()) {
                    nodeConnections.put(nodeId, randomLongBetween(0, 100));
                }
                // add node calculations
                if (frequently()) {
                    ResponseCollectorService.ComputedNodeStats stats = new ResponseCollectorService.ComputedNodeStats(
                        nodeId,
                        randomIntBetween(1, 10),
                        randomIntBetween(0, 2000),
                        randomDoubleBetween(1.0, 10000000.0, true),
                        randomDoubleBetween(1.0, 10000000.0, true)
                    );
                    nodeStats.put(nodeId, stats);
                }
            }
            adaptiveSelectionStats = new AdaptiveSelectionStats(nodeConnections, nodeStats);
        }
        NodesResourceUsageStats nodesResourceUsageStats = null;
        if (frequently()) {
            int numNodes = randomIntBetween(0, 10);
            Map<String, Long> nodeConnections = new HashMap<>();
            Map<String, NodeResourceUsageStats> resourceUsageStatsMap = new HashMap<>();
            for (int i = 0; i < numNodes; i++) {
                String nodeId = randomAlphaOfLengthBetween(3, 10);
                // add outgoing connection info
                if (frequently()) {
                    nodeConnections.put(nodeId, randomLongBetween(0, 100));
                }
                // add node calculations
                if (frequently()) {
                    NodeResourceUsageStats stats = new NodeResourceUsageStats(
                        nodeId,
                        System.currentTimeMillis(),
                        randomDoubleBetween(1.0, 100.0, true),
                        randomDoubleBetween(1.0, 100.0, true),
                        new IoUsageStats(100.0)
                    );
                    resourceUsageStatsMap.put(nodeId, stats);
                }
            }
            nodesResourceUsageStats = new NodesResourceUsageStats(resourceUsageStatsMap);
        }
        SegmentReplicationRejectionStats segmentReplicationRejectionStats = null;
        if (frequently()) {
            segmentReplicationRejectionStats = new SegmentReplicationRejectionStats(randomNonNegativeLong());
        }

        ClusterManagerThrottlingStats clusterManagerThrottlingStats = null;
        if (frequently()) {
            clusterManagerThrottlingStats = new ClusterManagerThrottlingStats();
            clusterManagerThrottlingStats.onThrottle("test-task", randomInt());
        }

        AdmissionControlStats admissionControlStats = null;
        if (frequently()) {
            AdmissionController admissionController = new AdmissionController(
                CpuBasedAdmissionController.CPU_BASED_ADMISSION_CONTROLLER,
                null,
                null
            ) {
                @Override
                public void apply(String action, AdmissionControlActionType admissionControlActionType) {
                    return;
                }
            };
            admissionController.addRejectionCount(AdmissionControlActionType.SEARCH.getType(), 1);
            admissionController.addRejectionCount(AdmissionControlActionType.INDEXING.getType(), 2);
            AdmissionControllerStats stats = new AdmissionControllerStats(admissionController);
            List<AdmissionControllerStats> statsList = new ArrayList();
            statsList.add(stats);
            admissionControlStats = new AdmissionControlStats(statsList);
        }
        ScriptCacheStats scriptCacheStats = scriptStats != null ? scriptStats.toScriptCacheStats() : null;

        WeightedRoutingStats weightedRoutingStats = null;
        weightedRoutingStats = WeightedRoutingStats.getInstance();
        weightedRoutingStats.updateFailOpenCount();

        NodeIndicesStats indicesStats = getNodeIndicesStats(remoteStoreStats);

        NodeCacheStats nodeCacheStats = null;
        if (frequently()) {
            int numIndices = randomIntBetween(1, 10);
            int numShardsPerIndex = randomIntBetween(1, 50);

            List<String> dimensionNames = List.of("index", "shard", "tier");
            DefaultCacheStatsHolder statsHolder = new DefaultCacheStatsHolder(dimensionNames, "dummyStoreName");
            for (int indexNum = 0; indexNum < numIndices; indexNum++) {
                String indexName = "index" + indexNum;
                for (int shardNum = 0; shardNum < numShardsPerIndex; shardNum++) {
                    String shardName = "[" + indexName + "][" + shardNum + "]";
                    for (String tierName : new String[] { "dummy_tier_1", "dummy_tier_2" }) {
                        List<String> dimensionValues = List.of(indexName, shardName, tierName);
                        CacheStats toIncrement = new CacheStats(randomInt(20), randomInt(20), randomInt(20), randomInt(20), randomInt(20));
                        DefaultCacheStatsHolderTests.populateStatsHolderFromStatsValueMap(
                            statsHolder,
                            Map.of(dimensionValues, toIncrement)
                        );
                    }
                }
            }
            CommonStatsFlags flags = new CommonStatsFlags();
            for (CacheType cacheType : CacheType.values()) {
                if (frequently()) {
                    flags.includeCacheType(cacheType);
                }
            }
            ImmutableCacheStatsHolder cacheStats = statsHolder.getImmutableCacheStatsHolder(dimensionNames.toArray(new String[0]));
            TreeMap<CacheType, ImmutableCacheStatsHolder> cacheStatsMap = new TreeMap<>();
            cacheStatsMap.put(CacheType.INDICES_REQUEST_CACHE, cacheStats);
            nodeCacheStats = new NodeCacheStats(cacheStatsMap, flags);
        }

        // TODO: Only remote_store based aspects of NodeIndicesStats are being tested here.
        // It is possible to test other metrics in NodeIndicesStats as well since it extends Writeable now
        return new NodeStats(
            node,
            randomNonNegativeLong(),
            indicesStats,
            osStats,
            processStats,
            jvmStats,
            threadPoolStats,
            fsInfo,
            transportStats,
            httpStats,
            allCircuitBreakerStats,
            scriptStats,
            discoveryStats,
            ingestStats,
            adaptiveSelectionStats,
            nodesResourceUsageStats,
            scriptCacheStats,
            null,
            null,
            null,
            clusterManagerThrottlingStats,
            weightedRoutingStats,
            null,
            null,
            null,
            segmentReplicationRejectionStats,
            null,
            admissionControlStats,
            nodeCacheStats
        );
    }

    private static NodeIndicesStats getNodeIndicesStats(boolean remoteStoreStats) {
        NodeIndicesStats indicesStats = null;
        if (remoteStoreStats) {
            ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
            indicesStats = new NodeIndicesStats(
                new CommonStats(CommonStatsFlags.ALL),
                new HashMap<>(),
                new SearchRequestStats(clusterSettings)
            );
            RemoteSegmentStats remoteSegmentStats = indicesStats.getSegments().getRemoteSegmentStats();
            remoteSegmentStats.addUploadBytesStarted(10L);
            remoteSegmentStats.addUploadBytesSucceeded(10L);
            remoteSegmentStats.addUploadBytesFailed(1L);
            remoteSegmentStats.addDownloadBytesStarted(10L);
            remoteSegmentStats.addDownloadBytesSucceeded(10L);
            remoteSegmentStats.addDownloadBytesFailed(1L);
            remoteSegmentStats.addTotalRefreshBytesLag(5L);
            remoteSegmentStats.addMaxRefreshBytesLag(2L);
            remoteSegmentStats.setMaxRefreshTimeLag(2L);
            remoteSegmentStats.addTotalUploadTime(20L);
            remoteSegmentStats.addTotalDownloadTime(20L);
            remoteSegmentStats.addTotalRejections(5L);

            RemoteTranslogStats remoteTranslogStats = indicesStats.getTranslog().getRemoteTranslogStats();
            RemoteTranslogStats otherRemoteTranslogStats = new RemoteTranslogStats(getRandomRemoteTranslogTransferTrackerStats());
            remoteTranslogStats.add(otherRemoteTranslogStats);
        }
        return indicesStats;
    }

    private static RemoteTranslogTransferTracker.Stats getRandomRemoteTranslogTransferTrackerStats() {
        return new RemoteTranslogTransferTracker.Stats(
            new ShardId("test-idx", "test-idx", randomIntBetween(1, 10)),
            0L,
            randomLongBetween(100, 500),
            randomLongBetween(50, 100),
            randomLongBetween(100, 200),
            randomLongBetween(10000, 50000),
            randomLongBetween(5000, 10000),
            randomLongBetween(10000, 20000),
            0L,
            0D,
            0D,
            0D,
            0L,
            0L,
            0L,
            0L,
            0D,
            0D,
            0D
        );
    }

    private OperationStats getPipelineStats(List<IngestStats.PipelineStat> pipelineStats, String id) {
        return pipelineStats.stream().filter(p1 -> p1.getPipelineId().equals(id)).findFirst().map(p2 -> p2.getStats()).orElse(null);
    }

    public static class MockNodeIndicesStats extends NodeIndicesStats {

        public MockNodeIndicesStats(StreamInput in) throws IOException {
            super(in);
        }

        public MockNodeIndicesStats(
            CommonStats oldStats,
            Map<Index, List<IndexShardStats>> statsByShard,
            SearchRequestStats searchRequestStats
        ) {
            super(oldStats, statsByShard, searchRequestStats);
        }

        public MockNodeIndicesStats(
            CommonStats oldStats,
            Map<Index, List<IndexShardStats>> statsByShard,
            SearchRequestStats searchRequestStats,
            String[] levels
        ) {
            super(oldStats, statsByShard, searchRequestStats, levels);
        }

        public CommonStats getStats() {
            return this.stats;
        }

        public Map<Index, CommonStats> getStatsByIndex() {
            return this.statsByIndex;
        }

        public Map<Index, List<IndexShardStats>> getStatsByShard() {
            return this.statsByShard;
        }
    }

    public void testOldVersionNodes() throws IOException {
        long numDocs = randomLongBetween(0, 10000);
        long numDeletedDocs = randomLongBetween(0, 100);
        CommonStats commonStats = new CommonStats(CommonStatsFlags.NONE);

        commonStats.docs = new DocsStats(numDocs, numDeletedDocs, 0);
        commonStats.store = new StoreStats(100, 0L);
        commonStats.indexing = new IndexingStats();
        DocsStats hostDocStats = new DocsStats(numDocs, numDeletedDocs, 0);

        CommonStatsFlags commonStatsFlags = new CommonStatsFlags();
        commonStatsFlags.clear();
        commonStatsFlags.set(CommonStatsFlags.Flag.Docs, true);
        commonStatsFlags.set(CommonStatsFlags.Flag.Store, true);
        commonStatsFlags.set(CommonStatsFlags.Flag.Indexing, true);

        Index newIndex = new Index("index", "_na_");

        MockNodeIndicesStats mockNodeIndicesStats = generateMockNodeIndicesStats(commonStats, newIndex, commonStatsFlags);

        // To test out scenario when the incoming node stats response is from a node with an older ES Version.
        try (BytesStreamOutput out = new BytesStreamOutput()) {
            out.setVersion(Version.V_2_13_0);
            mockNodeIndicesStats.writeTo(out);
            try (StreamInput in = out.bytes().streamInput()) {
                in.setVersion(Version.V_2_13_0);
                MockNodeIndicesStats newNodeIndicesStats = new MockNodeIndicesStats(in);

                List<IndexShardStats> incomingIndexStats = newNodeIndicesStats.getStatsByShard().get(newIndex);
                incomingIndexStats.forEach(indexShardStats -> {
                    ShardStats shardStats = Arrays.stream(indexShardStats.getShards()).findFirst().get();
                    DocsStats incomingDocStats = shardStats.getStats().docs;

                    assertEquals(incomingDocStats.getCount(), hostDocStats.getCount());
                    assertEquals(incomingDocStats.getTotalSizeInBytes(), hostDocStats.getTotalSizeInBytes());
                    assertEquals(incomingDocStats.getAverageSizeInBytes(), hostDocStats.getAverageSizeInBytes());
                    assertEquals(incomingDocStats.getDeleted(), hostDocStats.getDeleted());
                });
            }
        }
    }

    public void testNodeIndicesStatsSerialization() throws IOException {
        long numDocs = randomLongBetween(0, 10000);
        long numDeletedDocs = randomLongBetween(0, 100);
        List<NodeIndicesStats.StatsLevel> levelParams = new ArrayList<>();
        levelParams.add(NodeIndicesStats.StatsLevel.INDICES);
        levelParams.add(NodeIndicesStats.StatsLevel.SHARDS);
        levelParams.add(NodeIndicesStats.StatsLevel.NODE);
        CommonStats commonStats = new CommonStats(CommonStatsFlags.NONE);

        commonStats.docs = new DocsStats(numDocs, numDeletedDocs, 0);
        commonStats.store = new StoreStats(100, 0L);
        commonStats.indexing = new IndexingStats();

        CommonStatsFlags commonStatsFlags = new CommonStatsFlags();
        commonStatsFlags.clear();
        commonStatsFlags.set(CommonStatsFlags.Flag.Docs, true);
        commonStatsFlags.set(CommonStatsFlags.Flag.Store, true);
        commonStatsFlags.set(CommonStatsFlags.Flag.Indexing, true);
        commonStatsFlags.setIncludeIndicesStatsByLevel(true);

        levelParams.forEach(levelParam -> {
            ArrayList<String> level_arg = new ArrayList<>();
            level_arg.add(levelParam.getRestName());

            commonStatsFlags.setLevels(level_arg.toArray(new String[0]));

            Index newIndex = new Index("index", "_na_");

            MockNodeIndicesStats mockNodeIndicesStats = generateMockNodeIndicesStats(commonStats, newIndex, commonStatsFlags);

            // To test out scenario when the incoming node stats response is from a node with an older ES Version.
            try (BytesStreamOutput out = new BytesStreamOutput()) {
                mockNodeIndicesStats.writeTo(out);
                try (StreamInput in = out.bytes().streamInput()) {
                    MockNodeIndicesStats newNodeIndicesStats = new MockNodeIndicesStats(in);
                    switch (levelParam) {
                        case NODE:
                            assertNull(newNodeIndicesStats.getStatsByIndex());
                            assertNull(newNodeIndicesStats.getStatsByShard());
                            break;
                        case INDICES:
                            assertNull(newNodeIndicesStats.getStatsByShard());
                            assertNotNull(newNodeIndicesStats.getStatsByIndex());
                            break;
                        case SHARDS:
                            assertNull(newNodeIndicesStats.getStatsByIndex());
                            assertNotNull(newNodeIndicesStats.getStatsByShard());
                            break;
                    }
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    public void testNodeIndicesStatsToXContent() {
        long numDocs = randomLongBetween(0, 10000);
        long numDeletedDocs = randomLongBetween(0, 100);
        List<NodeIndicesStats.StatsLevel> levelParams = new ArrayList<>();
        levelParams.add(NodeIndicesStats.StatsLevel.INDICES);
        levelParams.add(NodeIndicesStats.StatsLevel.SHARDS);
        levelParams.add(NodeIndicesStats.StatsLevel.NODE);
        CommonStats commonStats = new CommonStats(CommonStatsFlags.NONE);

        commonStats.docs = new DocsStats(numDocs, numDeletedDocs, 0);
        commonStats.store = new StoreStats(100, 0L);
        commonStats.indexing = new IndexingStats();

        CommonStatsFlags commonStatsFlags = new CommonStatsFlags();
        commonStatsFlags.clear();
        commonStatsFlags.set(CommonStatsFlags.Flag.Docs, true);
        commonStatsFlags.set(CommonStatsFlags.Flag.Store, true);
        commonStatsFlags.set(CommonStatsFlags.Flag.Indexing, true);
        commonStatsFlags.setIncludeIndicesStatsByLevel(true);

        levelParams.forEach(levelParam -> {
            ArrayList<String> level_arg = new ArrayList<>();
            level_arg.add(levelParam.getRestName());

            commonStatsFlags.setLevels(level_arg.toArray(new String[0]));

            Index newIndex = new Index("index", "_na_");

            MockNodeIndicesStats mockNodeIndicesStats = generateMockNodeIndicesStats(commonStats, newIndex, commonStatsFlags);

            XContentBuilder builder = null;
            try {
                builder = XContentFactory.jsonBuilder();
                builder.startObject();
                builder = mockNodeIndicesStats.toXContent(
                    builder,
                    new ToXContent.MapParams(Collections.singletonMap("level", levelParam.getRestName()))
                );
                builder.endObject();

                Map<String, Object> xContentMap = xContentBuilderToMap(builder);
                LinkedHashMap indicesStatsMap = (LinkedHashMap) xContentMap.get(NodeIndicesStats.StatsLevel.INDICES.getRestName());

                switch (levelParam) {
                    case NODE:
                        assertFalse(indicesStatsMap.containsKey(NodeIndicesStats.StatsLevel.INDICES.getRestName()));
                        assertFalse(indicesStatsMap.containsKey(NodeIndicesStats.StatsLevel.SHARDS.getRestName()));
                        break;
                    case INDICES:
                        assertTrue(indicesStatsMap.containsKey(NodeIndicesStats.StatsLevel.INDICES.getRestName()));
                        assertFalse(indicesStatsMap.containsKey(NodeIndicesStats.StatsLevel.SHARDS.getRestName()));
                        break;
                    case SHARDS:
                        assertFalse(indicesStatsMap.containsKey(NodeIndicesStats.StatsLevel.INDICES.getRestName()));
                        assertTrue(indicesStatsMap.containsKey(NodeIndicesStats.StatsLevel.SHARDS.getRestName()));
                        break;
                }
            } catch (IOException e) {
                throw new RuntimeException(e);
            }

        });
    }

    public void testNodeIndicesStatsWithAndWithoutAggregations() throws IOException {

        CommonStatsFlags commonStatsFlags = new CommonStatsFlags(
            CommonStatsFlags.Flag.Docs,
            CommonStatsFlags.Flag.Store,
            CommonStatsFlags.Flag.Indexing,
            CommonStatsFlags.Flag.Completion,
            CommonStatsFlags.Flag.Flush,
            CommonStatsFlags.Flag.FieldData,
            CommonStatsFlags.Flag.QueryCache,
            CommonStatsFlags.Flag.Segments
        );

        int numberOfIndexes = randomIntBetween(1, 3);
        List<Index> indexList = new ArrayList<>();
        for (int i = 0; i < numberOfIndexes; i++) {
            Index index = new Index("test-index-" + i, "_na_");
            indexList.add(index);
        }

        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);
        HashMap<Index, List<IndexShardStats>> statsByShards = createRandomShardByStats(indexList);

        final MockNodeIndicesStats nonAggregatedNodeIndicesStats = new MockNodeIndicesStats(
            new CommonStats(commonStatsFlags),
            statsByShards,
            new SearchRequestStats(clusterSettings)
        );

        commonStatsFlags.setIncludeIndicesStatsByLevel(true);

        Arrays.stream(NodeIndicesStats.StatsLevel.values()).forEach(enumLevel -> {
            String level = enumLevel.getRestName();
            List<String> levelList = new ArrayList<>();
            levelList.add(level);

            MockNodeIndicesStats aggregatedNodeIndicesStats = new MockNodeIndicesStats(
                new CommonStats(commonStatsFlags),
                statsByShards,
                new SearchRequestStats(clusterSettings),
                levelList.toArray(new String[0])
            );

            XContentBuilder nonAggregatedBuilder = null;
            XContentBuilder aggregatedBuilder = null;
            try {
                nonAggregatedBuilder = XContentFactory.jsonBuilder();
                nonAggregatedBuilder.startObject();
                nonAggregatedBuilder = nonAggregatedNodeIndicesStats.toXContent(
                    nonAggregatedBuilder,
                    new ToXContent.MapParams(Collections.singletonMap("level", level))
                );
                nonAggregatedBuilder.endObject();
                Map<String, Object> nonAggregatedContentMap = xContentBuilderToMap(nonAggregatedBuilder);

                aggregatedBuilder = XContentFactory.jsonBuilder();
                aggregatedBuilder.startObject();
                aggregatedBuilder = aggregatedNodeIndicesStats.toXContent(
                    aggregatedBuilder,
                    new ToXContent.MapParams(Collections.singletonMap("level", level))
                );
                aggregatedBuilder.endObject();
                Map<String, Object> aggregatedContentMap = xContentBuilderToMap(aggregatedBuilder);

                assertEquals(aggregatedContentMap, nonAggregatedContentMap);
            } catch (IOException e) {
                throw new RuntimeException(e);
            }
        });
    }

    private CommonStats createRandomCommonStats() {
        CommonStats commonStats = new CommonStats(CommonStatsFlags.NONE);
        commonStats.docs = new DocsStats(randomLongBetween(0, 10000), randomLongBetween(0, 100), randomLongBetween(0, 1000));
        commonStats.store = new StoreStats(randomLongBetween(0, 100), randomLongBetween(0, 1000));
        commonStats.indexing = new IndexingStats();
        commonStats.completion = new CompletionStats();
        commonStats.flush = new FlushStats(randomLongBetween(0, 100), randomLongBetween(0, 100), randomLongBetween(0, 100));
        commonStats.fieldData = new FieldDataStats(randomLongBetween(0, 100), randomLongBetween(0, 100), null);
        commonStats.queryCache = new QueryCacheStats(
            randomLongBetween(0, 100),
            randomLongBetween(0, 100),
            randomLongBetween(0, 100),
            randomLongBetween(0, 100),
            randomLongBetween(0, 100)
        );
        commonStats.segments = new SegmentsStats();

        return commonStats;
    }

    private HashMap<Index, List<IndexShardStats>> createRandomShardByStats(List<Index> indexes) {
        DiscoveryNode localNode = new DiscoveryNode("node", buildNewFakeTransportAddress(), Version.CURRENT);
        HashMap<Index, List<IndexShardStats>> statsByShards = new HashMap<>();
        indexes.forEach(index -> {
            List<IndexShardStats> indexShardStatsList = new ArrayList<>();

            int numberOfShards = randomIntBetween(1, 4);
            for (int i = 0; i < numberOfShards; i++) {
                ShardRoutingState shardRoutingState = ShardRoutingState.fromValue((byte) randomIntBetween(2, 3));

                ShardRouting shardRouting = TestShardRouting.newShardRouting(
                    index.getName(),
                    i,
                    localNode.getId(),
                    randomBoolean(),
                    shardRoutingState
                );

                Path path = createTempDir().resolve("indices")
                    .resolve(shardRouting.shardId().getIndex().getUUID())
                    .resolve(String.valueOf(shardRouting.shardId().id()));

                ShardStats shardStats = new ShardStats(
                    shardRouting,
                    new ShardPath(false, path, path, shardRouting.shardId()),
                    createRandomCommonStats(),
                    null,
                    null,
                    null
                );
                List<ShardStats> shardStatsList = new ArrayList<>();
                shardStatsList.add(shardStats);
                IndexShardStats indexShardStats = new IndexShardStats(shardRouting.shardId(), shardStatsList.toArray(new ShardStats[0]));
                indexShardStatsList.add(indexShardStats);
            }
            statsByShards.put(index, indexShardStatsList);
        });

        return statsByShards;
    }

    private Map<String, Object> xContentBuilderToMap(XContentBuilder xContentBuilder) {
        return XContentHelper.convertToMap(BytesReference.bytes(xContentBuilder), true, xContentBuilder.contentType()).v2();
    }

    public MockNodeIndicesStats generateMockNodeIndicesStats(CommonStats commonStats, Index index, CommonStatsFlags commonStatsFlags) {
        DiscoveryNode localNode = new DiscoveryNode("local", buildNewFakeTransportAddress(), Version.CURRENT);
        Map<Index, List<IndexShardStats>> statsByShard = new HashMap<>();
        List<IndexShardStats> indexShardStatsList = new ArrayList<>();
        Index statsIndex = null;
        for (int i = 0; i < 2; i++) {
            ShardRoutingState shardRoutingState = ShardRoutingState.fromValue((byte) randomIntBetween(2, 3));
            ShardRouting shardRouting = TestShardRouting.newShardRouting(
                index.getName(),
                i,
                localNode.getId(),
                randomBoolean(),
                shardRoutingState
            );

            if (statsIndex == null) {
                statsIndex = shardRouting.shardId().getIndex();
            }

            Path path = createTempDir().resolve("indices")
                .resolve(shardRouting.shardId().getIndex().getUUID())
                .resolve(String.valueOf(shardRouting.shardId().id()));

            ShardStats shardStats = new ShardStats(
                shardRouting,
                new ShardPath(false, path, path, shardRouting.shardId()),
                commonStats,
                null,
                null,
                null
            );
            IndexShardStats indexShardStats = new IndexShardStats(shardRouting.shardId(), new ShardStats[] { shardStats });
            indexShardStatsList.add(indexShardStats);
        }

        statsByShard.put(statsIndex, indexShardStatsList);

        ClusterSettings clusterSettings = new ClusterSettings(Settings.EMPTY, ClusterSettings.BUILT_IN_CLUSTER_SETTINGS);

        if (commonStatsFlags.getIncludeIndicesStatsByLevel()) {
            return new MockNodeIndicesStats(
                new CommonStats(commonStatsFlags),
                statsByShard,
                new SearchRequestStats(clusterSettings),
                commonStatsFlags.getLevels()
            );
        } else {
            return new MockNodeIndicesStats(new CommonStats(commonStatsFlags), statsByShard, new SearchRequestStats(clusterSettings));
        }
    }
}
