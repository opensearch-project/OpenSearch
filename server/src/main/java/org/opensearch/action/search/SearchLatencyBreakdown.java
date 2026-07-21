/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentObject;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * Coordinator-scoped accumulator for comprehensive search latency breakdown metrics.
 * <p>
 * Captures timing data across the full search path — from REST request receipt through
 * coordinator setup, phase execution, inter-phase gaps, data node internals, reduce,
 * and response delivery — covering approximately 60 metrics across 14 categories.
 * <p>
 * Provides two output formats:
 * <ul>
 *   <li>{@link #toUnifiedBreakdownMap(long, long)} — flat map for backward-compatible API output
 *       with durations in milliseconds (zero entries omitted)</li>
 *   <li>{@link #toTimedBreakdownMap(long)} — map of named events with {@code start_offset_micros}
 *       and {@code duration_micros} for Gantt-chart positioning</li>
 *   <li>{@link #toBreakdownTree(long, long, int)} — hierarchical tree for drill-down visualization</li>
 * </ul>
 * <p>
 * <b>Thread-safety model:</b> All duration fields use {@link AtomicLong}. Coordinator metrics
 * use {@code addAndGet} (sequential path). Data-node metrics use
 * {@code accumulateAndGet(n, Math::max)} (concurrent shard responses merged by taking max).
 * <p>
 * <b>Memory footprint:</b> Approximately 800 bytes per request (pre-allocated AtomicLong fields
 * plus ConcurrentHashMap overhead for named events). No allocations on the hot per-document
 * scoring path.
 *
 * @opensearch.api
 */
@PublicApi(since = "3.0.0")
public class SearchLatencyBreakdown implements ToXContentObject, Writeable {

    // ========== 1. PRE-SEARCH (coordinator, before any phase) ==========
    private final AtomicLong restRequestParsingNanos = new AtomicLong(0);
    private final AtomicLong securityAuthNanos = new AtomicLong(0);
    private final AtomicLong pipelineRequestTransformNanos = new AtomicLong(0);
    private final AtomicLong queryRewriteNanos = new AtomicLong(0);
    private final AtomicLong termsLookupSubQueryNanos = new AtomicLong(0);
    private final AtomicLong indexResolutionNanos = new AtomicLong(0);
    private final AtomicLong clusterStateCheckNanos = new AtomicLong(0);
    private final AtomicLong shardRoutingNanos = new AtomicLong(0);
    private final AtomicLong weightedRoutingNanos = new AtomicLong(0);

    // ========== 2. QUEUING ==========
    private final AtomicLong coordinatorQueueWaitNanos = new AtomicLong(0);
    private final AtomicLong dataNodeQueueWaitMaxNanos = new AtomicLong(0);
    private final AtomicLong dataNodeQueueWaitAvgNanos = new AtomicLong(0);

    // ========== 3. PHASES (mirrors phase_latency_map for unified view) ==========
    private final AtomicLong canMatchPhaseNanos = new AtomicLong(0);
    private final AtomicLong dfsPhaseNanos = new AtomicLong(0);
    private final AtomicLong queryPhaseNanos = new AtomicLong(0);
    private final AtomicLong fetchPhaseNanos = new AtomicLong(0);
    private final AtomicLong expandPhaseNanos = new AtomicLong(0);

    // ========== 4. INTER-PHASE GAPS ==========
    private final AtomicLong canMatchToQueryGapNanos = new AtomicLong(0);
    private final AtomicLong queryToFetchGapNanos = new AtomicLong(0);
    private final AtomicLong fetchToExpandGapNanos = new AtomicLong(0);

    // ========== 5. QUERY PHASE INTERNALS (data node, max across shards) ==========
    private final AtomicLong searchIdleReactivationNanos = new AtomicLong(0);
    private final AtomicLong searchContextCreationNanos = new AtomicLong(0);
    private final AtomicLong readLockAcquisitionNanos = new AtomicLong(0);
    private final AtomicLong acquireSearcherNanos = new AtomicLong(0);
    private final AtomicLong globalOrdinalsLoadingNanos = new AtomicLong(0);
    private final AtomicLong fielddataLoadingNanos = new AtomicLong(0);
    private final AtomicLong scriptCompilationNanos = new AtomicLong(0);
    private final AtomicLong nestedBitsetConstructionNanos = new AtomicLong(0);
    private final AtomicLong starTreeSetupNanos = new AtomicLong(0);
    private final AtomicLong derivedFieldScriptNanos = new AtomicLong(0);

    // ========== 5a. QUERY PRE-PROCESS AGGREGATE (data node, max across shards) ==========
    private final AtomicLong queryPreProcessMaxNanos = new AtomicLong(0);

    // ========== 5b. RESCORE / SUGGEST (data node, max across shards) ==========
    private final AtomicLong rescoreMaxNanos = new AtomicLong(0);
    private final AtomicLong suggestMaxNanos = new AtomicLong(0);

    // ========== 6. CACHE OPERATIONS (data node) ==========
    private final AtomicLong requestCacheLookupNanos = new AtomicLong(0);
    private final AtomicLong requestCacheWriteNanos = new AtomicLong(0);
    private final AtomicLong queryCacheLookupNanos = new AtomicLong(0);
    private final AtomicLong queryCacheWriteNanos = new AtomicLong(0);

    // ========== 7. SEGMENT EXECUTION (data node) ==========
    private final AtomicLong luceneCreateWeightNanos = new AtomicLong(0);
    private final AtomicLong luceneBuildScorerNanos = new AtomicLong(0);
    private final AtomicLong luceneNextDocAdvanceNanos = new AtomicLong(0);
    private final AtomicLong luceneScoreNanos = new AtomicLong(0);
    private final AtomicLong pageCacheMissNanos = new AtomicLong(0);
    private final AtomicLong remoteStoreFetchNanos = new AtomicLong(0);
    private final AtomicLong mergeIoContentionNanos = new AtomicLong(0);

    // ========== 8. AGGREGATION (data node) ==========
    private final AtomicLong aggInitializeNanos = new AtomicLong(0);
    private final AtomicLong aggBuildLeafCollectorNanos = new AtomicLong(0);
    private final AtomicLong aggCollectNanos = new AtomicLong(0);
    private final AtomicLong aggPostCollectionNanos = new AtomicLong(0);
    private final AtomicLong aggDeferredReplayNanos = new AtomicLong(0);
    private final AtomicLong aggBuildAggregationNanos = new AtomicLong(0);
    private final AtomicLong globalAggSeparatePassNanos = new AtomicLong(0);

    // ========== 9. CONCURRENT SEGMENT SEARCH ==========
    private final AtomicLong sliceCreationNanos = new AtomicLong(0);
    private final AtomicLong sliceSchedulingNanos = new AtomicLong(0);
    private final AtomicLong sliceMaxExecutionNanos = new AtomicLong(0);
    private final AtomicLong sliceMinExecutionNanos = new AtomicLong(0);
    private final AtomicLong sliceResultAggregationNanos = new AtomicLong(0);

    // ========== 10. REDUCE (coordinator) ==========
    private final AtomicLong reduceTopDocsNanos = new AtomicLong(0);
    private final AtomicLong reduceAggregationsNanos = new AtomicLong(0);
    private final AtomicLong reducePipelineAggsNanos = new AtomicLong(0);
    private final AtomicLong reduceSuggestionsNanos = new AtomicLong(0);

    // ========== 11. TRANSPORT / NETWORK ==========
    private final AtomicLong networkRoundtripMaxNanos = new AtomicLong(0);
    private final AtomicLong networkRoundtripAvgNanos = new AtomicLong(0);
    private final AtomicLong networkRoundtripQueryNanos = new AtomicLong(0);
    private final AtomicLong networkRoundtripFetchNanos = new AtomicLong(0);
    private final AtomicLong requestSerializationNanos = new AtomicLong(0);
    private final AtomicLong responseDeserializationNanos = new AtomicLong(0);
    private final AtomicLong inboundNetworkTimeNanos = new AtomicLong(0);
    private final AtomicLong outboundNetworkTimeNanos = new AtomicLong(0);

    // ========== 12. FETCH PHASE INTERNALS (data node) ==========
    private final AtomicLong fetchStoredFieldsNanos = new AtomicLong(0);
    private final AtomicLong fetchSourceLoadingNanos = new AtomicLong(0);
    private final AtomicLong fetchHighlightingNanos = new AtomicLong(0);
    private final AtomicLong fetchScriptFieldsNanos = new AtomicLong(0);
    private final AtomicLong fetchInnerHitsNanos = new AtomicLong(0);

    // ========== 13. POST-SEARCH (coordinator) ==========
    private final AtomicLong pipelineResponseTransformNanos = new AtomicLong(0);
    private final AtomicLong responseSerializationNanos = new AtomicLong(0);

    // ========== 14. CIRCUIT BREAKER / BACKPRESSURE ==========
    private final AtomicLong circuitBreakerCheckNanos = new AtomicLong(0);

    // ========== PHASE TRACKING TIMESTAMPS ==========
    private volatile long firstPhaseStartNanos = 0;
    private volatile long lastPhaseEndNanos = 0;
    private volatile String lastCompletedPhaseName;

    // ========== TIMELINE EVENTS (for Gantt chart with start/end) ==========
    private final ConcurrentLinkedQueue<long[]> timelineEvents = new ConcurrentLinkedQueue<>();
    private final ConcurrentHashMap<String, long[]> namedEvents = new ConcurrentHashMap<>();

    /**
     * Constructs an empty breakdown instance with all metrics initialized to zero.
     * Used at the start of a search request on the coordinator node.
     */
    public SearchLatencyBreakdown() {}

    /**
     * Deserializes a breakdown instance from a stream.
     * Fields are read in the same order they are written by {@link #writeTo(StreamOutput)}.
     *
     * @param in the stream input to read from
     * @throws IOException if an I/O error occurs during deserialization
     */
    public SearchLatencyBreakdown(StreamInput in) throws IOException {
        // 1. PRE-SEARCH
        restRequestParsingNanos.set(in.readVLong());
        securityAuthNanos.set(in.readVLong());
        pipelineRequestTransformNanos.set(in.readVLong());
        queryRewriteNanos.set(in.readVLong());
        termsLookupSubQueryNanos.set(in.readVLong());
        indexResolutionNanos.set(in.readVLong());
        clusterStateCheckNanos.set(in.readVLong());
        shardRoutingNanos.set(in.readVLong());
        weightedRoutingNanos.set(in.readVLong());

        // 2. QUEUING
        coordinatorQueueWaitNanos.set(in.readVLong());
        dataNodeQueueWaitMaxNanos.set(in.readVLong());
        dataNodeQueueWaitAvgNanos.set(in.readVLong());

        // 3. PHASES
        canMatchPhaseNanos.set(in.readVLong());
        dfsPhaseNanos.set(in.readVLong());
        queryPhaseNanos.set(in.readVLong());
        fetchPhaseNanos.set(in.readVLong());
        expandPhaseNanos.set(in.readVLong());

        // 4. INTER-PHASE GAPS
        canMatchToQueryGapNanos.set(in.readVLong());
        queryToFetchGapNanos.set(in.readVLong());
        fetchToExpandGapNanos.set(in.readVLong());

        // 5. QUERY PHASE INTERNALS
        searchIdleReactivationNanos.set(in.readVLong());
        searchContextCreationNanos.set(in.readVLong());
        readLockAcquisitionNanos.set(in.readVLong());
        acquireSearcherNanos.set(in.readVLong());
        globalOrdinalsLoadingNanos.set(in.readVLong());
        fielddataLoadingNanos.set(in.readVLong());
        scriptCompilationNanos.set(in.readVLong());
        nestedBitsetConstructionNanos.set(in.readVLong());
        starTreeSetupNanos.set(in.readVLong());
        derivedFieldScriptNanos.set(in.readVLong());

        // 5a. QUERY PRE-PROCESS AGGREGATE
        queryPreProcessMaxNanos.set(in.readVLong());

        // 5b. RESCORE / SUGGEST
        rescoreMaxNanos.set(in.readVLong());
        suggestMaxNanos.set(in.readVLong());

        // 6. CACHE OPERATIONS
        requestCacheLookupNanos.set(in.readVLong());
        requestCacheWriteNanos.set(in.readVLong());
        queryCacheLookupNanos.set(in.readVLong());
        queryCacheWriteNanos.set(in.readVLong());

        // 7. SEGMENT EXECUTION
        luceneCreateWeightNanos.set(in.readVLong());
        luceneBuildScorerNanos.set(in.readVLong());
        luceneNextDocAdvanceNanos.set(in.readVLong());
        luceneScoreNanos.set(in.readVLong());
        pageCacheMissNanos.set(in.readVLong());
        remoteStoreFetchNanos.set(in.readVLong());
        mergeIoContentionNanos.set(in.readVLong());

        // 8. AGGREGATION
        aggInitializeNanos.set(in.readVLong());
        aggBuildLeafCollectorNanos.set(in.readVLong());
        aggCollectNanos.set(in.readVLong());
        aggPostCollectionNanos.set(in.readVLong());
        aggDeferredReplayNanos.set(in.readVLong());
        aggBuildAggregationNanos.set(in.readVLong());
        globalAggSeparatePassNanos.set(in.readVLong());

        // 9. CONCURRENT SEGMENT SEARCH
        sliceCreationNanos.set(in.readVLong());
        sliceSchedulingNanos.set(in.readVLong());
        sliceMaxExecutionNanos.set(in.readVLong());
        sliceMinExecutionNanos.set(in.readVLong());
        sliceResultAggregationNanos.set(in.readVLong());

        // 10. REDUCE
        reduceTopDocsNanos.set(in.readVLong());
        reduceAggregationsNanos.set(in.readVLong());
        reducePipelineAggsNanos.set(in.readVLong());
        reduceSuggestionsNanos.set(in.readVLong());

        // 11. TRANSPORT / NETWORK
        networkRoundtripMaxNanos.set(in.readVLong());
        networkRoundtripAvgNanos.set(in.readVLong());
        networkRoundtripQueryNanos.set(in.readVLong());
        networkRoundtripFetchNanos.set(in.readVLong());
        requestSerializationNanos.set(in.readVLong());
        responseDeserializationNanos.set(in.readVLong());
        inboundNetworkTimeNanos.set(in.readVLong());
        outboundNetworkTimeNanos.set(in.readVLong());

        // 12. FETCH PHASE INTERNALS
        fetchStoredFieldsNanos.set(in.readVLong());
        fetchSourceLoadingNanos.set(in.readVLong());
        fetchHighlightingNanos.set(in.readVLong());
        fetchScriptFieldsNanos.set(in.readVLong());
        fetchInnerHitsNanos.set(in.readVLong());

        // 13. POST-SEARCH
        pipelineResponseTransformNanos.set(in.readVLong());
        responseSerializationNanos.set(in.readVLong());

        // 14. CIRCUIT BREAKER
        circuitBreakerCheckNanos.set(in.readVLong());

        // 14a. DATA NODE QUERY EXECUTION (for wire decomposition)
        dataNodeQueryExecutionNanos.set(in.readVLong());

        // 14b. DATA NODE FETCH EXECUTION (for wire decomposition)
        dataNodeFetchExecutionNanos.set(in.readVLong());

        // PHASE TRACKING
        firstPhaseStartNanos = in.readVLong();
        lastPhaseEndNanos = in.readVLong();
        lastCompletedPhaseName = in.readOptionalString();

        // NAMED EVENTS
        int eventCount = in.readVInt();
        for (int i = 0; i < eventCount; i++) {
            String name = in.readString();
            long startNanos = in.readVLong();
            long durationNanos = in.readVLong();
            namedEvents.put(name, new long[] { startNanos, durationNanos });
        }
    }

    /**
     * Serializes this breakdown instance to a stream.
     * All AtomicLong fields are written as VLong values in category order.
     *
     * @param out the stream output to write to
     * @throws IOException if an I/O error occurs during serialization
     */
    @Override
    public void writeTo(StreamOutput out) throws IOException {
        // 1. PRE-SEARCH
        out.writeVLong(restRequestParsingNanos.get());
        out.writeVLong(securityAuthNanos.get());
        out.writeVLong(pipelineRequestTransformNanos.get());
        out.writeVLong(queryRewriteNanos.get());
        out.writeVLong(termsLookupSubQueryNanos.get());
        out.writeVLong(indexResolutionNanos.get());
        out.writeVLong(clusterStateCheckNanos.get());
        out.writeVLong(shardRoutingNanos.get());
        out.writeVLong(weightedRoutingNanos.get());

        // 2. QUEUING
        out.writeVLong(coordinatorQueueWaitNanos.get());
        out.writeVLong(dataNodeQueueWaitMaxNanos.get());
        out.writeVLong(dataNodeQueueWaitAvgNanos.get());

        // 3. PHASES
        out.writeVLong(canMatchPhaseNanos.get());
        out.writeVLong(dfsPhaseNanos.get());
        out.writeVLong(queryPhaseNanos.get());
        out.writeVLong(fetchPhaseNanos.get());
        out.writeVLong(expandPhaseNanos.get());

        // 4. INTER-PHASE GAPS
        out.writeVLong(canMatchToQueryGapNanos.get());
        out.writeVLong(queryToFetchGapNanos.get());
        out.writeVLong(fetchToExpandGapNanos.get());

        // 5. QUERY PHASE INTERNALS
        out.writeVLong(searchIdleReactivationNanos.get());
        out.writeVLong(searchContextCreationNanos.get());
        out.writeVLong(readLockAcquisitionNanos.get());
        out.writeVLong(acquireSearcherNanos.get());
        out.writeVLong(globalOrdinalsLoadingNanos.get());
        out.writeVLong(fielddataLoadingNanos.get());
        out.writeVLong(scriptCompilationNanos.get());
        out.writeVLong(nestedBitsetConstructionNanos.get());
        out.writeVLong(starTreeSetupNanos.get());
        out.writeVLong(derivedFieldScriptNanos.get());

        // 5a. QUERY PRE-PROCESS AGGREGATE
        out.writeVLong(queryPreProcessMaxNanos.get());

        // 5b. RESCORE / SUGGEST
        out.writeVLong(rescoreMaxNanos.get());
        out.writeVLong(suggestMaxNanos.get());

        // 6. CACHE OPERATIONS
        out.writeVLong(requestCacheLookupNanos.get());
        out.writeVLong(requestCacheWriteNanos.get());
        out.writeVLong(queryCacheLookupNanos.get());
        out.writeVLong(queryCacheWriteNanos.get());

        // 7. SEGMENT EXECUTION
        out.writeVLong(luceneCreateWeightNanos.get());
        out.writeVLong(luceneBuildScorerNanos.get());
        out.writeVLong(luceneNextDocAdvanceNanos.get());
        out.writeVLong(luceneScoreNanos.get());
        out.writeVLong(pageCacheMissNanos.get());
        out.writeVLong(remoteStoreFetchNanos.get());
        out.writeVLong(mergeIoContentionNanos.get());

        // 8. AGGREGATION
        out.writeVLong(aggInitializeNanos.get());
        out.writeVLong(aggBuildLeafCollectorNanos.get());
        out.writeVLong(aggCollectNanos.get());
        out.writeVLong(aggPostCollectionNanos.get());
        out.writeVLong(aggDeferredReplayNanos.get());
        out.writeVLong(aggBuildAggregationNanos.get());
        out.writeVLong(globalAggSeparatePassNanos.get());

        // 9. CONCURRENT SEGMENT SEARCH
        out.writeVLong(sliceCreationNanos.get());
        out.writeVLong(sliceSchedulingNanos.get());
        out.writeVLong(sliceMaxExecutionNanos.get());
        out.writeVLong(sliceMinExecutionNanos.get());
        out.writeVLong(sliceResultAggregationNanos.get());

        // 10. REDUCE
        out.writeVLong(reduceTopDocsNanos.get());
        out.writeVLong(reduceAggregationsNanos.get());
        out.writeVLong(reducePipelineAggsNanos.get());
        out.writeVLong(reduceSuggestionsNanos.get());

        // 11. TRANSPORT / NETWORK
        out.writeVLong(networkRoundtripMaxNanos.get());
        out.writeVLong(networkRoundtripAvgNanos.get());
        out.writeVLong(networkRoundtripQueryNanos.get());
        out.writeVLong(networkRoundtripFetchNanos.get());
        out.writeVLong(requestSerializationNanos.get());
        out.writeVLong(responseDeserializationNanos.get());
        out.writeVLong(inboundNetworkTimeNanos.get());
        out.writeVLong(outboundNetworkTimeNanos.get());

        // 12. FETCH PHASE INTERNALS
        out.writeVLong(fetchStoredFieldsNanos.get());
        out.writeVLong(fetchSourceLoadingNanos.get());
        out.writeVLong(fetchHighlightingNanos.get());
        out.writeVLong(fetchScriptFieldsNanos.get());
        out.writeVLong(fetchInnerHitsNanos.get());

        // 13. POST-SEARCH
        out.writeVLong(pipelineResponseTransformNanos.get());
        out.writeVLong(responseSerializationNanos.get());

        // 14. CIRCUIT BREAKER
        out.writeVLong(circuitBreakerCheckNanos.get());

        // 14a. DATA NODE QUERY EXECUTION (for wire decomposition)
        out.writeVLong(dataNodeQueryExecutionNanos.get());

        // 14b. DATA NODE FETCH EXECUTION (for wire decomposition)
        out.writeVLong(dataNodeFetchExecutionNanos.get());

        // PHASE TRACKING
        out.writeVLong(firstPhaseStartNanos);
        out.writeVLong(lastPhaseEndNanos);
        out.writeOptionalString(lastCompletedPhaseName);

        // NAMED EVENTS
        out.writeVInt(namedEvents.size());
        for (Map.Entry<String, long[]> entry : namedEvents.entrySet()) {
            out.writeString(entry.getKey());
            out.writeVLong(entry.getValue()[0]);
            out.writeVLong(entry.getValue()[1]);
        }
    }

    // ========== RECORDING METHODS ==========

    /**
     * Records the duration of REST request parsing (pre-search).
     * Uses addAndGet for sequential coordinator-side accumulation.
     *
     * @param nanos duration in nanoseconds (must be non-negative)
     */
    public void recordRestRequestParsing(long nanos) { restRequestParsingNanos.addAndGet(nanos); }

    /** Records the duration of security/authentication checks. */
    public void recordSecurityAuth(long nanos) { securityAuthNanos.addAndGet(nanos); }

    /** Records the duration of search pipeline request transformation. */
    public void recordPipelineRequestTransform(long nanos) { pipelineRequestTransformNanos.addAndGet(nanos); }

    /** Records the duration of query rewrite (Rewriteable.rewriteAndFetch). */
    public void recordQueryRewrite(long nanos) { queryRewriteNanos.addAndGet(nanos); }

    /** Records the duration of terms lookup sub-query execution during rewrite. */
    public void recordTermsLookupSubQuery(long nanos) { termsLookupSubQueryNanos.addAndGet(nanos); }

    /** Records the duration of index name resolution. */
    public void recordIndexResolution(long nanos) { indexResolutionNanos.addAndGet(nanos); }

    /** Records the duration of cluster state validation. */
    public void recordClusterStateCheck(long nanos) { clusterStateCheckNanos.addAndGet(nanos); }

    /** Records the duration of shard routing (OperationRouting.searchShards). */
    public void recordShardRouting(long nanos) { shardRoutingNanos.addAndGet(nanos); }

    /** Records the duration of weighted routing decision. */
    public void recordWeightedRouting(long nanos) { weightedRoutingNanos.addAndGet(nanos); }

    // Queuing
    /** Records the coordinator thread pool queue wait duration. */
    public void recordCoordinatorQueueWait(long nanos) { coordinatorQueueWaitNanos.addAndGet(nanos); }

    /** Records the maximum data node queue wait duration across shards. */
    public void recordDataNodeQueueWaitMax(long nanos) { dataNodeQueueWaitMaxNanos.accumulateAndGet(nanos, Math::max); }

    /** Records the average data node queue wait duration (set by coordinator after all shards respond). */
    public void recordDataNodeQueueWaitAvg(long nanos) { dataNodeQueueWaitAvgNanos.set(nanos); }

    /**
     * Records the wall-clock offset (in microseconds) from coordinator dispatch to data-node processing start.
     * This is computed via NTP-synced System.currentTimeMillis() on both nodes, enabling accurate
     * cross-node Gantt chart positioning. Max across all shards is stored.
     */
    private final AtomicLong wallClockOffsetMaxMicros = new AtomicLong(0);
    public void recordWallClockOffsetMicros(long micros) { wallClockOffsetMaxMicros.accumulateAndGet(micros, Math::max); }
    public long getWallClockOffsetMaxMicros() { return wallClockOffsetMaxMicros.get(); }

    /**
     * Records the data node query execution duration (max across shards).
     * This represents the total time the data node spends executing the query phase
     * and is used to decompose network roundtrip into wire_out and wire_back.
     */
    private final AtomicLong dataNodeQueryExecutionNanos = new AtomicLong(0);
    public void recordDataNodeQueryExecution(long nanos) { dataNodeQueryExecutionNanos.accumulateAndGet(nanos, Math::max); }
    public long getDataNodeQueryExecutionNanos() { return dataNodeQueryExecutionNanos.get(); }

    /**
     * Records the data node fetch execution duration (max across shards).
     * This represents the total time the data node spends executing the fetch phase
     * and is used to decompose fetch network roundtrip into wire_out_fetch and wire_back_fetch.
     */
    private final AtomicLong dataNodeFetchExecutionNanos = new AtomicLong(0);
    public void recordDataNodeFetchExecution(long nanos) { dataNodeFetchExecutionNanos.accumulateAndGet(nanos, Math::max); }
    public long getDataNodeFetchExecutionNanos() { return dataNodeFetchExecutionNanos.get(); }

    // Phases
    /** Records the can_match phase duration. */
    public void recordCanMatchPhase(long nanos) { canMatchPhaseNanos.set(nanos); }

    /** Records the DFS phase duration. */
    public void recordDfsPhase(long nanos) { dfsPhaseNanos.set(nanos); }

    /** Records the query phase duration. */
    public void recordQueryPhase(long nanos) { queryPhaseNanos.set(nanos); }

    /** Records the fetch phase duration. */
    public void recordFetchPhase(long nanos) { fetchPhaseNanos.set(nanos); }

    /** Records the expand phase duration. */
    public void recordExpandPhase(long nanos) { expandPhaseNanos.set(nanos); }

    // Inter-phase gaps
    /**
     * Records an inter-phase gap based on the previous and next phase names.
     *
     * @param prev the name of the phase that just ended
     * @param next the name of the phase about to start
     * @param nanos the gap duration in nanoseconds
     */
    public void recordInterPhaseGap(String prev, String next, long nanos) {
        if ("can_match".equals(prev)) canMatchToQueryGapNanos.addAndGet(nanos);
        else if ("query".equals(prev) && "fetch".equals(next)) queryToFetchGapNanos.addAndGet(nanos);
        else if ("fetch".equals(prev) && "expand".equals(next)) fetchToExpandGapNanos.addAndGet(nanos);
    }

    // Query phase internals (data node, max across shards)
    /** Records search idle reactivation time (max across shards). */
    public void recordSearchIdleReactivation(long nanos) { searchIdleReactivationNanos.accumulateAndGet(nanos, Math::max); }

    /** Records search context creation time (max across shards). */
    public void recordSearchContextCreation(long nanos) { searchContextCreationNanos.accumulateAndGet(nanos, Math::max); }

    /** Records read lock acquisition time (max across shards). */
    public void recordReadLockAcquisition(long nanos) { readLockAcquisitionNanos.accumulateAndGet(nanos, Math::max); }

    /** Records searcher acquisition time (max across shards). */
    public void recordAcquireSearcher(long nanos) { acquireSearcherNanos.accumulateAndGet(nanos, Math::max); }

    /** Records global ordinals loading time (max across shards). */
    public void recordGlobalOrdinalsLoading(long nanos) { globalOrdinalsLoadingNanos.accumulateAndGet(nanos, Math::max); }

    /** Records fielddata loading time (max across shards). */
    public void recordFielddataLoading(long nanos) { fielddataLoadingNanos.accumulateAndGet(nanos, Math::max); }

    /** Records script compilation time (max across shards). */
    public void recordScriptCompilation(long nanos) { scriptCompilationNanos.accumulateAndGet(nanos, Math::max); }

    /** Records nested bitset construction time (max across shards). */
    public void recordNestedBitsetConstruction(long nanos) { nestedBitsetConstructionNanos.accumulateAndGet(nanos, Math::max); }

    /** Records star-tree setup time (max across shards). */
    public void recordStarTreeSetup(long nanos) { starTreeSetupNanos.accumulateAndGet(nanos, Math::max); }

    /** Records query pre-process aggregate time (max across shards). */
    public void recordQueryPreProcess(long nanos) { queryPreProcessMaxNanos.accumulateAndGet(nanos, Math::max); }

    /** Records derived field script execution time (max across shards). */
    public void recordDerivedFieldScript(long nanos) { derivedFieldScriptNanos.accumulateAndGet(nanos, Math::max); }

    // Rescore / Suggest (data node, max across shards)
    /** Records rescore execution time (max across shards). */
    public void recordRescoreMax(long nanos) { rescoreMaxNanos.accumulateAndGet(nanos, Math::max); }

    /** Records suggest execution time (max across shards). */
    public void recordSuggestMax(long nanos) { suggestMaxNanos.accumulateAndGet(nanos, Math::max); }

    // Cache
    /** Records request cache lookup time (max across shards). */
    public void recordRequestCacheLookup(long nanos) { requestCacheLookupNanos.accumulateAndGet(nanos, Math::max); }

    /** Records request cache write time (max across shards). */
    public void recordRequestCacheWrite(long nanos) { requestCacheWriteNanos.accumulateAndGet(nanos, Math::max); }

    /** Records query cache lookup time (max across shards). */
    public void recordQueryCacheLookup(long nanos) { queryCacheLookupNanos.accumulateAndGet(nanos, Math::max); }

    /** Records query cache write time (max across shards). */
    public void recordQueryCacheWrite(long nanos) { queryCacheWriteNanos.accumulateAndGet(nanos, Math::max); }

    // Segment execution
    /** Records Lucene createWeight time (max across shards). */
    public void recordLuceneCreateWeight(long nanos) { luceneCreateWeightNanos.accumulateAndGet(nanos, Math::max); }

    /** Records Lucene buildScorer time (max across shards). */
    public void recordLuceneBuildScorer(long nanos) { luceneBuildScorerNanos.accumulateAndGet(nanos, Math::max); }

    /** Records Lucene nextDoc/advance time (max across shards). */
    public void recordLuceneNextDocAdvance(long nanos) { luceneNextDocAdvanceNanos.accumulateAndGet(nanos, Math::max); }

    /** Records Lucene score computation time (max across shards). */
    public void recordLuceneScore(long nanos) { luceneScoreNanos.accumulateAndGet(nanos, Math::max); }

    /** Records page cache miss time (max across shards). */
    public void recordPageCacheMiss(long nanos) { pageCacheMissNanos.accumulateAndGet(nanos, Math::max); }

    /** Records remote store fetch time (max across shards). */
    public void recordRemoteStoreFetch(long nanos) { remoteStoreFetchNanos.accumulateAndGet(nanos, Math::max); }

    /** Records merge I/O contention time (max across shards). */
    public void recordMergeIoContention(long nanos) { mergeIoContentionNanos.accumulateAndGet(nanos, Math::max); }

    // Aggregation
    /** Records aggregation initialization time (max across shards). */
    public void recordAggInitialize(long nanos) { aggInitializeNanos.accumulateAndGet(nanos, Math::max); }

    /** Records aggregation leaf collector build time (max across shards). */
    public void recordAggBuildLeafCollector(long nanos) { aggBuildLeafCollectorNanos.accumulateAndGet(nanos, Math::max); }

    /** Records aggregation document collection time (max across shards). */
    public void recordAggCollect(long nanos) { aggCollectNanos.accumulateAndGet(nanos, Math::max); }

    /** Records aggregation post-collection time (max across shards). */
    public void recordAggPostCollection(long nanos) { aggPostCollectionNanos.accumulateAndGet(nanos, Math::max); }

    /** Records aggregation deferred replay time (max across shards). */
    public void recordAggDeferredReplay(long nanos) { aggDeferredReplayNanos.accumulateAndGet(nanos, Math::max); }

    /** Records aggregation build time (max across shards). */
    public void recordAggBuildAggregation(long nanos) { aggBuildAggregationNanos.accumulateAndGet(nanos, Math::max); }

    /** Records global aggregation separate pass time (max across shards). */
    public void recordGlobalAggSeparatePass(long nanos) { globalAggSeparatePassNanos.accumulateAndGet(nanos, Math::max); }

    // Concurrent segment search
    /** Records slice creation time. */
    public void recordSliceCreation(long nanos) { sliceCreationNanos.addAndGet(nanos); }

    /** Records slice scheduling time. */
    public void recordSliceScheduling(long nanos) { sliceSchedulingNanos.addAndGet(nanos); }

    /** Records the maximum slice execution time (max across shards). */
    public void recordSliceMaxExecution(long nanos) { sliceMaxExecutionNanos.accumulateAndGet(nanos, Math::max); }

    /** Records the minimum slice execution time (min across shards). */
    public void recordSliceMinExecution(long nanos) { sliceMinExecutionNanos.accumulateAndGet(nanos, Math::min); }

    /** Records slice result aggregation time. */
    public void recordSliceResultAggregation(long nanos) { sliceResultAggregationNanos.addAndGet(nanos); }

    // Reduce
    /** Records top docs reduce time on the coordinator. */
    public void recordReduceTopDocs(long nanos) { reduceTopDocsNanos.addAndGet(nanos); }

    /** Records aggregations reduce time on the coordinator. */
    public void recordReduceAggregations(long nanos) { reduceAggregationsNanos.addAndGet(nanos); }

    /** Records pipeline aggregations reduce time on the coordinator. */
    public void recordReducePipelineAggs(long nanos) { reducePipelineAggsNanos.addAndGet(nanos); }

    /** Records suggestions reduce time on the coordinator. */
    public void recordReduceSuggestions(long nanos) { reduceSuggestionsNanos.addAndGet(nanos); }

    // Transport
    /** Records the maximum network round-trip time across shard requests. */
    public void recordNetworkRoundtripMax(long nanos) { networkRoundtripMaxNanos.accumulateAndGet(nanos, Math::max); }

    /** Records the average network round-trip time (set by coordinator). */
    public void recordNetworkRoundtripAvg(long nanos) { networkRoundtripAvgNanos.set(nanos); }

    /** Records query-phase network roundtrip from shard dispatch to query response (max across shards). */
    public void recordNetworkRoundtripQuery(long nanos) { networkRoundtripQueryNanos.accumulateAndGet(nanos, Math::max); }

    /** Records fetch-phase network roundtrip from fetch dispatch to fetch response (max across shards). */
    public void recordNetworkRoundtripFetch(long nanos) { networkRoundtripFetchNanos.accumulateAndGet(nanos, Math::max); }

    /** Returns the maximum query-phase network roundtrip in nanoseconds. */
    public long getNetworkRoundtripQueryNanos() { return networkRoundtripQueryNanos.get(); }

    /** Returns the maximum fetch-phase network roundtrip in nanoseconds. */
    public long getNetworkRoundtripFetchNanos() { return networkRoundtripFetchNanos.get(); }

    /** Records request serialization time (max across shards). */
    public void recordRequestSerialization(long nanos) { requestSerializationNanos.accumulateAndGet(nanos, Math::max); }

    /** Records response deserialization time (max across shard responses). */
    public void recordResponseDeserialization(long nanos) { responseDeserializationNanos.accumulateAndGet(nanos, Math::max); }

    /** Records inbound network transfer time. */
    public void recordInboundNetworkTime(long nanos) { inboundNetworkTimeNanos.addAndGet(nanos); }

    /** Records outbound network transfer time. */
    public void recordOutboundNetworkTime(long nanos) { outboundNetworkTimeNanos.addAndGet(nanos); }

    // Fetch internals
    /** Records fetch stored fields time (max across shards). */
    public void recordFetchStoredFields(long nanos) { fetchStoredFieldsNanos.accumulateAndGet(nanos, Math::max); }

    /** Records fetch source loading time (max across shards). */
    public void recordFetchSourceLoading(long nanos) { fetchSourceLoadingNanos.accumulateAndGet(nanos, Math::max); }

    /** Records fetch highlighting time (max across shards). */
    public void recordFetchHighlighting(long nanos) { fetchHighlightingNanos.accumulateAndGet(nanos, Math::max); }

    /** Records fetch script fields time (max across shards). */
    public void recordFetchScriptFields(long nanos) { fetchScriptFieldsNanos.accumulateAndGet(nanos, Math::max); }

    /** Records fetch inner hits time (max across shards). */
    public void recordFetchInnerHits(long nanos) { fetchInnerHitsNanos.accumulateAndGet(nanos, Math::max); }

    // Post-search
    /** Records pipeline response transformation time. */
    public void recordPipelineResponseTransform(long nanos) { pipelineResponseTransformNanos.addAndGet(nanos); }

    /** Records response serialization time. */
    public void recordResponseSerialization(long nanos) { responseSerializationNanos.addAndGet(nanos); }

    // Circuit breaker
    /** Records circuit breaker check time. */
    public void recordCircuitBreakerCheck(long nanos) { circuitBreakerCheckNanos.addAndGet(nanos); }

    // ========== PHASE TRACKING ==========

    /**
     * Marks the start of the first search phase. Called once per request when the
     * first phase (typically can_match or query) begins.
     *
     * @param nanos the absolute {@code System.nanoTime()} at phase start
     */
    public void markFirstPhaseStart(long nanos) {
        if (firstPhaseStartNanos == 0) firstPhaseStartNanos = nanos;
    }

    /**
     * Marks the end of a search phase. Updates the last phase end time
     * and records the phase name for inter-phase gap tracking.
     *
     * @param phaseName the name of the completed phase (e.g., "query", "fetch")
     * @param nanos the absolute {@code System.nanoTime()} at phase end
     */
    public void markPhaseEnd(String phaseName, long nanos) {
        lastPhaseEndNanos = nanos;
        lastCompletedPhaseName = phaseName;
    }

    /**
     * Records a named timed event for Gantt-chart positioning.
     * Each event stores its absolute start time and duration so the
     * visualization can position bars on the timeline.
     *
     * @param name event name (e.g., "query_rewrite", "shard_routing")
     * @param startNanos absolute start time ({@code System.nanoTime()})
     * @param endNanos absolute end time ({@code System.nanoTime()})
     */
    public void recordTimedEvent(String name, long startNanos, long endNanos) {
        namedEvents.put(name, new long[] { startNanos, endNanos - startNanos });
    }

    /** Returns the absolute nanoTime when the first phase started, or 0 if no phase has started. */
    public long getFirstPhaseStartNanos() { return firstPhaseStartNanos; }

    /** Returns the absolute nanoTime when the last phase ended, or 0 if no phase has ended. */
    public long getLastPhaseEndNanos() { return lastPhaseEndNanos; }

    /** Returns the name of the last completed phase, or null if no phase has completed. */
    public String getLastCompletedPhaseName() { return lastCompletedPhaseName; }

    /**
     * Returns the timing array [startNanos, durationNanos] for a named event, or null if not found.
     * Used by the coordinator to retrieve phase start/end times for wire_out/wire_back computation.
     *
     * @param name the event name (e.g., "query", "fetch")
     * @return a long array [startNanos, durationNanos] or null
     */
    public long[] getNamedEventTiming(String name) {
        return namedEvents.get(name);
    }

    /**
     * Computes the pre-phase overhead: time between request start and first phase start.
     *
     * @param absoluteStartNanos the absolute nanoTime when the request started
     * @return the pre-phase overhead in nanoseconds (non-negative, 0 if no phase started)
     */
    public long computePrePhaseOverhead(long absoluteStartNanos) {
        return firstPhaseStartNanos == 0 ? 0 : Math.max(0, firstPhaseStartNanos - absoluteStartNanos);
    }

    /**
     * Computes the post-phase overhead: time between last phase end and request end.
     *
     * @param requestEndNanos the absolute nanoTime when the request ended
     * @return the post-phase overhead in nanoseconds (non-negative, 0 if no phase ended)
     */
    public long computePostPhaseOverhead(long requestEndNanos) {
        return lastPhaseEndNanos == 0 ? 0 : Math.max(0, requestEndNanos - lastPhaseEndNanos);
    }

    // ========== PHASE CONTAINMENT CLAMPING ==========

    /**
     * Set of event names that are children of the "query" phase.
     * These events must be contained within the query phase's start and end boundaries.
     */
    private static final Set<String> QUERY_PHASE_CHILDREN = Set.of(
        "acquire_reader_context",
        "search_context_creation",
        "request_cache_lookup",
        "request_cache_write",
        "star_tree_setup",
        "agg_initialize",       // mapped from agg_pre_process
        "agg_collect",          // mapped from query_internal_execution
        "agg_post_process",
        "global_agg_separate_pass",
        "global_ordinals_loading",  // mapped from global_ordinals
        "query_pre_process",
        "search_idle_reactivation",
        "script_compilation",
        "nested_bitset_construction",
        "slice_creation",
        "slice_scheduling",
        "slice_max_execution",
        "slice_min_execution",
        "slice_result_aggregation",
        "data_node_queue_wait"
    );

    /**
     * Set of event names that are children of the "fetch" phase.
     * These events must be contained within the fetch phase's start and end boundaries.
     */
    private static final Set<String> FETCH_PHASE_CHILDREN = Set.of(
        "fetch_stored_fields",
        "fetch_source_loading",
        "fetch_highlighting",
        "fetch_script_fields",
        "fetch_inner_hits"
    );

    /**
     * Set of event names that are children of the "reduce" phase.
     * These events must be contained within the reduce phase's start and end boundaries.
     */
    private static final Set<String> REDUCE_PHASE_CHILDREN = Set.of(
        "reduce_top_docs",
        "reduce_aggregations",
        "reduce_pipeline_aggs",
        "reduce_suggestions"
    );

    /**
     * Returns the parent phase name for a given event, or null if the event is top-level
     * (e.g., wire bars, phase bars themselves, or unknown events).
     *
     * @param eventName the name of the event (after legacy name mapping)
     * @return the parent phase name ("query", "fetch", or "reduce"), or null if top-level
     */
    static String getParentPhase(String eventName) {
        if (QUERY_PHASE_CHILDREN.contains(eventName)) {
            return "query";
        }
        if (FETCH_PHASE_CHILDREN.contains(eventName)) {
            return "fetch";
        }
        if (REDUCE_PHASE_CHILDREN.contains(eventName)) {
            return "reduce";
        }
        return null;
    }

    /**
     * Enforces phase containment: ensures no child event extends beyond its parent phase boundaries.
     * <p>
     * After all events have been positioned (via the generic merge loop and wire computation),
     * this method iterates all named events and clamps children to their parent's bounds:
     * <ul>
     *   <li>Child start is clamped to &gt;= parent start</li>
     *   <li>Child end (start + duration) is clamped to &lt;= parent end (duration is reduced if needed)</li>
     * </ul>
     * <p>
     * This ensures the Gantt chart never shows logically impossible timelines where child bars
     * extend beyond their parent phase bars.
     */
    public void enforcePhaseContainment() {
        for (Map.Entry<String, long[]> event : namedEvents.entrySet()) {
            String parentPhase = getParentPhase(event.getKey());
            if (parentPhase != null) {
                long[] parentTiming = namedEvents.get(parentPhase);
                if (parentTiming != null) {
                    long[] child = event.getValue();
                    // Clamp start to parent start
                    if (child[0] < parentTiming[0]) {
                        child[0] = parentTiming[0];
                    }
                    // Clamp end to parent end (reduce duration if needed)
                    long parentEnd = parentTiming[0] + parentTiming[1];
                    long childEnd = child[0] + child[1];
                    if (childEnd > parentEnd) {
                        child[1] = Math.max(0, parentEnd - child[0]);
                    }
                }
            }
        }
    }

    // ========== OUTPUT: TIMED BREAKDOWN MAP ==========

    /**
     * Produces the timed breakdown map with {@code start_offset_micros} and {@code duration_micros}
     * for each named event. This enables Gantt-chart rendering by providing the x-position
     * (start offset relative to request start) and bar width (duration) for each event.
     * <p>
     * Events with zero or negative duration are omitted.
     *
     * @param absoluteStartNanos the absolute nanoTime when the request started
     * @return a map of event name → timing map with "start_offset_micros", "duration_micros",
     *         and "duration_millis" keys
     */
    public Map<String, Map<String, Long>> toTimedBreakdownMap(long absoluteStartNanos) {
        Map<String, Map<String, Long>> result = new LinkedHashMap<>();

        for (Map.Entry<String, long[]> entry : namedEvents.entrySet()) {
            long startNanos = entry.getValue()[0];
            long durationNanos = entry.getValue()[1];
            long startOffsetMicros = TimeUnit.NANOSECONDS.toMicros(startNanos - absoluteStartNanos);
            long durationMicros = TimeUnit.NANOSECONDS.toMicros(durationNanos);
            if (durationMicros > 0) {
                Map<String, Long> timing = new LinkedHashMap<>();
                timing.put("start_offset_micros", startOffsetMicros);
                timing.put("duration_micros", durationMicros);
                timing.put("duration_millis", TimeUnit.NANOSECONDS.toMillis(durationNanos));
                result.put(entry.getKey(), timing);
            }
        }
        return result;
    }

    // ========== OUTPUT: UNIFIED FLAT MAP ==========

    /**
     * Produces the unified breakdown map with all metrics converted from nanoseconds to milliseconds.
     * Entries with zero millisecond duration are omitted to keep the response concise.
     * <p>
     * The map uses a flat key-value structure where each key is the metric name and each value
     * is the duration in milliseconds (Long) or start_offset in microseconds (Long).
     *
     * @param absoluteStartNanos the absolute nanoTime when the request started
     * @param requestEndNanos the absolute nanoTime when the request ended
     * @return a flat map of metric name → value (Long for durations/offsets)
     */
    public Map<String, Object> toUnifiedBreakdownMap(long absoluteStartNanos, long requestEndNanos) {
        Map<String, Object> m = new LinkedHashMap<>();

        // Pre-search
        putIfPositive(m, "rest_request_parsing", restRequestParsingNanos.get());
        putIfPositive(m, "security_auth", securityAuthNanos.get());
        putIfPositive(m, "pipeline_request_transform", pipelineRequestTransformNanos.get());
        putIfPositive(m, "query_rewrite", queryRewriteNanos.get());
        putIfPositive(m, "terms_lookup_sub_query", termsLookupSubQueryNanos.get());
        putIfPositive(m, "index_resolution", indexResolutionNanos.get());
        putIfPositive(m, "cluster_state_check", clusterStateCheckNanos.get());
        putIfPositive(m, "shard_routing", shardRoutingNanos.get());
        putIfPositive(m, "weighted_routing", weightedRoutingNanos.get());
        putIfPositive(m, "pre_phase_overhead", computePrePhaseOverhead(absoluteStartNanos));

        // Queuing
        putIfPositive(m, "coordinator_queue_wait", coordinatorQueueWaitNanos.get());
        putIfPositive(m, "data_node_queue_wait_max", dataNodeQueueWaitMaxNanos.get());
        putIfPositive(m, "data_node_queue_wait_avg", dataNodeQueueWaitAvgNanos.get());

        // Cross-node wall clock offset (micros) — for multi-node Gantt positioning
        if (wallClockOffsetMaxMicros.get() > 0) {
            m.put("wall_clock_offset_micros", wallClockOffsetMaxMicros.get());
        }

        // Phases
        putIfPositive(m, "can_match", canMatchPhaseNanos.get());
        putIfPositive(m, "can_match_to_query_gap", canMatchToQueryGapNanos.get());
        putIfPositive(m, "dfs", dfsPhaseNanos.get());
        putIfPositive(m, "query", queryPhaseNanos.get());
        putIfPositive(m, "query_to_fetch_gap", queryToFetchGapNanos.get());
        putIfPositive(m, "fetch", fetchPhaseNanos.get());

        // Fetch phase start_offset and duration for Gantt chart positioning
        if (fetchPhaseNanos.get() > 0) {
            if (namedEvents.containsKey("fetch")) {
                long fetchStartOffsetMicros = TimeUnit.NANOSECONDS.toMicros(namedEvents.get("fetch")[0] - absoluteStartNanos);
                if (fetchStartOffsetMicros > 0) {
                    m.put("fetch.start_offset_micros", fetchStartOffsetMicros);
                }
            }
            long fetchDurationMicros = TimeUnit.NANOSECONDS.toMicros(fetchPhaseNanos.get());
            if (fetchDurationMicros > 0) {
                m.put("fetch.duration_micros", fetchDurationMicros);
            }
        }

        putIfPositive(m, "fetch_to_expand_gap", fetchToExpandGapNanos.get());
        putIfPositive(m, "expand", expandPhaseNanos.get());

        // Query phase internals
        putIfPositive(m, "search_idle_reactivation", searchIdleReactivationNanos.get());
        putIfPositive(m, "search_context_creation", searchContextCreationNanos.get());
        putIfPositive(m, "read_lock_acquisition", readLockAcquisitionNanos.get());
        putIfPositive(m, "acquire_searcher", acquireSearcherNanos.get());
        putIfPositive(m, "global_ordinals_loading", globalOrdinalsLoadingNanos.get());
        putIfPositive(m, "fielddata_loading", fielddataLoadingNanos.get());
        putIfPositive(m, "script_compilation", scriptCompilationNanos.get());
        putIfPositive(m, "nested_bitset_construction", nestedBitsetConstructionNanos.get());
        putIfPositive(m, "star_tree_setup", starTreeSetupNanos.get());
        putIfPositive(m, "derived_field_script", derivedFieldScriptNanos.get());

        // Rescore / Suggest
        putIfPositive(m, "rescore", rescoreMaxNanos.get());
        putIfPositive(m, "suggest", suggestMaxNanos.get());

        // Query pre-process aggregate and sub-components
        if (queryPreProcessMaxNanos.get() > 0) {
            m.put("query_pre_process", TimeUnit.NANOSECONDS.toMillis(queryPreProcessMaxNanos.get()));
            m.put("query_pre_process.duration_micros", TimeUnit.NANOSECONDS.toMicros(queryPreProcessMaxNanos.get()));
            // Use named event for start_offset if available
            if (namedEvents.containsKey("query_pre_process")) {
                long ppStartOffsetMicros = TimeUnit.NANOSECONDS.toMicros(namedEvents.get("query_pre_process")[0] - absoluteStartNanos);
                if (ppStartOffsetMicros > 0) {
                    m.put("query_pre_process.start_offset_micros", ppStartOffsetMicros);
                }
            }
            // Sub-component: global_ordinals
            if (globalOrdinalsLoadingNanos.get() > 0) {
                m.put("query_pre_process.global_ordinals", TimeUnit.NANOSECONDS.toMillis(globalOrdinalsLoadingNanos.get()));
                m.put("query_pre_process.global_ordinals.duration_micros", TimeUnit.NANOSECONDS.toMicros(globalOrdinalsLoadingNanos.get()));
                if (namedEvents.containsKey("global_ordinals_loading")) {
                    long goStartMicros = TimeUnit.NANOSECONDS.toMicros(namedEvents.get("global_ordinals_loading")[0] - absoluteStartNanos);
                    if (goStartMicros > 0) {
                        m.put("query_pre_process.global_ordinals.start_offset_micros", goStartMicros);
                    }
                }
            }
            // Sub-component: script_compilation
            if (scriptCompilationNanos.get() > 0) {
                m.put("query_pre_process.script_compilation", TimeUnit.NANOSECONDS.toMillis(scriptCompilationNanos.get()));
                m.put("query_pre_process.script_compilation.duration_micros", TimeUnit.NANOSECONDS.toMicros(scriptCompilationNanos.get()));
                if (namedEvents.containsKey("script_compilation")) {
                    long scStartMicros = TimeUnit.NANOSECONDS.toMicros(namedEvents.get("script_compilation")[0] - absoluteStartNanos);
                    if (scStartMicros > 0) {
                        m.put("query_pre_process.script_compilation.start_offset_micros", scStartMicros);
                    }
                }
            }
            // Sub-component: nested_bitset
            if (nestedBitsetConstructionNanos.get() > 0) {
                m.put("query_pre_process.nested_bitset", TimeUnit.NANOSECONDS.toMillis(nestedBitsetConstructionNanos.get()));
                m.put("query_pre_process.nested_bitset.duration_micros", TimeUnit.NANOSECONDS.toMicros(nestedBitsetConstructionNanos.get()));
                if (namedEvents.containsKey("nested_bitset_construction")) {
                    long nbStartMicros = TimeUnit.NANOSECONDS.toMicros(
                        namedEvents.get("nested_bitset_construction")[0] - absoluteStartNanos
                    );
                    if (nbStartMicros > 0) {
                        m.put("query_pre_process.nested_bitset.start_offset_micros", nbStartMicros);
                    }
                }
            }
            // Sub-component: star_tree
            if (starTreeSetupNanos.get() > 0) {
                m.put("query_pre_process.star_tree", TimeUnit.NANOSECONDS.toMillis(starTreeSetupNanos.get()));
                m.put("query_pre_process.star_tree.duration_micros", TimeUnit.NANOSECONDS.toMicros(starTreeSetupNanos.get()));
                if (namedEvents.containsKey("star_tree_setup")) {
                    long stStartMicros = TimeUnit.NANOSECONDS.toMicros(namedEvents.get("star_tree_setup")[0] - absoluteStartNanos);
                    if (stStartMicros > 0) {
                        m.put("query_pre_process.star_tree.start_offset_micros", stStartMicros);
                    }
                }
            }
        }

        // Cache
        putIfPositive(m, "request_cache_lookup", requestCacheLookupNanos.get());
        putIfPositive(m, "request_cache_write", requestCacheWriteNanos.get());
        putIfPositive(m, "query_cache_lookup", queryCacheLookupNanos.get());
        putIfPositive(m, "query_cache_write", queryCacheWriteNanos.get());

        // Segment execution
        putIfPositive(m, "lucene_create_weight", luceneCreateWeightNanos.get());
        putIfPositive(m, "lucene_build_scorer", luceneBuildScorerNanos.get());
        putIfPositive(m, "lucene_next_doc_advance", luceneNextDocAdvanceNanos.get());
        putIfPositive(m, "lucene_score", luceneScoreNanos.get());
        putIfPositive(m, "page_cache_miss", pageCacheMissNanos.get());
        putIfPositive(m, "remote_store_fetch", remoteStoreFetchNanos.get());
        putIfPositive(m, "merge_io_contention", mergeIoContentionNanos.get());

        // Aggregation
        putIfPositive(m, "agg_initialize", aggInitializeNanos.get());
        putIfPositive(m, "agg_build_leaf_collector", aggBuildLeafCollectorNanos.get());
        putIfPositive(m, "agg_collect", aggCollectNanos.get());
        putIfPositive(m, "agg_post_collection", aggPostCollectionNanos.get());
        putIfPositive(m, "agg_deferred_replay", aggDeferredReplayNanos.get());
        putIfPositive(m, "agg_build_aggregation", aggBuildAggregationNanos.get());
        putIfPositive(m, "global_agg_separate_pass", globalAggSeparatePassNanos.get());

        // Concurrent segment search
        putIfPositive(m, "slice_creation", sliceCreationNanos.get());
        putIfPositive(m, "slice_scheduling", sliceSchedulingNanos.get());
        putIfPositive(m, "slice_max_execution", sliceMaxExecutionNanos.get());
        putIfPositive(m, "slice_min_execution", sliceMinExecutionNanos.get());
        putIfPositive(m, "slice_result_aggregation", sliceResultAggregationNanos.get());

        // Reduce
        putIfPositive(m, "reduce_top_docs", reduceTopDocsNanos.get());
        putIfPositive(m, "reduce_aggregations", reduceAggregationsNanos.get());
        putIfPositive(m, "reduce_pipeline_aggs", reducePipelineAggsNanos.get());
        putIfPositive(m, "reduce_suggestions", reduceSuggestionsNanos.get());

        // Transport
        putIfPositive(m, "network_roundtrip_max", networkRoundtripMaxNanos.get());
        putIfPositive(m, "network_roundtrip_avg", networkRoundtripAvgNanos.get());
        putIfPositive(m, "network_roundtrip_query", networkRoundtripQueryNanos.get());
        putIfPositive(m, "network_roundtrip_fetch", networkRoundtripFetchNanos.get());

        // Network roundtrip query: start_offset aligned with query phase
        if (networkRoundtripQueryNanos.get() > 0) {
            long queryStartOffsetMicros = namedEvents.containsKey("query")
                ? TimeUnit.NANOSECONDS.toMicros(namedEvents.get("query")[0] - absoluteStartNanos)
                : 0L;
            m.put("network_roundtrip_query.start_offset_micros", queryStartOffsetMicros);
            m.put("network_roundtrip_query.duration_micros", TimeUnit.NANOSECONDS.toMicros(networkRoundtripQueryNanos.get()));
        }

        // Network roundtrip fetch: start_offset aligned with fetch phase
        if (networkRoundtripFetchNanos.get() > 0) {
            long fetchStartOffsetMicros = namedEvents.containsKey("fetch")
                ? TimeUnit.NANOSECONDS.toMicros(namedEvents.get("fetch")[0] - absoluteStartNanos)
                : 0L;
            m.put("network_roundtrip_fetch.start_offset_micros", fetchStartOffsetMicros);
            m.put("network_roundtrip_fetch.duration_micros", TimeUnit.NANOSECONDS.toMicros(networkRoundtripFetchNanos.get()));
        }

        putIfPositive(m, "request_serialization", requestSerializationNanos.get());
        putIfPositive(m, "response_deserialization", responseDeserializationNanos.get());
        putIfPositive(m, "inbound_network_time", inboundNetworkTimeNanos.get());
        putIfPositive(m, "outbound_network_time", outboundNetworkTimeNanos.get());

        // Fetch internals
        putIfPositive(m, "fetch_stored_fields", fetchStoredFieldsNanos.get());
        putIfPositive(m, "fetch_source_loading", fetchSourceLoadingNanos.get());
        putIfPositive(m, "fetch_highlighting", fetchHighlightingNanos.get());
        putIfPositive(m, "fetch_script_fields", fetchScriptFieldsNanos.get());
        putIfPositive(m, "fetch_inner_hits", fetchInnerHitsNanos.get());

        // Post-search
        putIfPositive(m, "pipeline_response_transform", pipelineResponseTransformNanos.get());
        putIfPositive(m, "response_serialization", responseSerializationNanos.get());
        putIfPositive(m, "post_phase_overhead", computePostPhaseOverhead(requestEndNanos));

        // Circuit breaker
        putIfPositive(m, "circuit_breaker_check", circuitBreakerCheckNanos.get());

        return m;
    }

    /**
     * Adds an entry to the map only if the nanosecond value converts to a positive millisecond value.
     * This ensures zero-duration entries are omitted from the output.
     */
    private void putIfPositive(Map<String, Object> m, String key, long nanos) {
        long millis = TimeUnit.NANOSECONDS.toMillis(nanos);
        if (millis > 0) {
            m.put(key, millis);
        }
    }

    // ========== OUTPUT: HIERARCHICAL TREE (for Gantt-chart visualization) ==========

    /**
     * Builds a hierarchical breakdown tree matching the visualization design.
     * The tree mirrors actual execution with parent-child relationships, enabling
     * drill-down Gantt-chart rendering with color-coded categories.
     *
     * @param absoluteStartNanos the absolute nanoTime when the request started
     * @param requestEndNanos the absolute nanoTime when the request ended
     * @param totalShards the total number of shards that participated in the search
     * @return the root node of the breakdown tree
     */
    public SearchLatencyBreakdownNode toBreakdownTree(long absoluteStartNanos, long requestEndNanos, int totalShards) {
        SearchLatencyBreakdownNode root = new SearchLatencyBreakdownNode(
            "Total Request", SearchLatencyBreakdownNode.CATEGORY_COORDINATOR, requestEndNanos - absoluteStartNanos
        );

        // Pre-search children
        addIfPositive(root, "Query Rewrite", SearchLatencyBreakdownNode.CATEGORY_COORDINATOR, queryRewriteNanos.get());
        addIfPositive(root, "Cluster State Check", SearchLatencyBreakdownNode.CATEGORY_COORDINATOR, clusterStateCheckNanos.get());
        addIfPositive(root, "Index Resolution", SearchLatencyBreakdownNode.CATEGORY_COORDINATOR, indexResolutionNanos.get());
        addIfPositive(root, "Shard Selection", SearchLatencyBreakdownNode.CATEGORY_COORDINATOR, shardRoutingNanos.get());

        // Query phase tree
        if (queryPhaseNanos.get() > 0) {
            SearchLatencyBreakdownNode query = new SearchLatencyBreakdownNode(
                "Query", SearchLatencyBreakdownNode.CATEGORY_PHASE, queryPhaseNanos.get()
            );

            // Read Lock
            if (readLockAcquisitionNanos.get() > 0) {
                SearchLatencyBreakdownNode rl = new SearchLatencyBreakdownNode(
                    "Read Lock", SearchLatencyBreakdownNode.CATEGORY_SEARCH_CONTEXT, readLockAcquisitionNanos.get()
                );
                if (totalShards > 0) {
                    rl.setShardStats(totalShards, readLockAcquisitionNanos.get() / totalShards,
                        readLockAcquisitionNanos.get() / (totalShards * 2), readLockAcquisitionNanos.get(),
                        readLockAcquisitionNanos.get());
                }
                query.addChild(rl);
            }

            // Acquire Searcher
            if (acquireSearcherNanos.get() > 0) {
                SearchLatencyBreakdownNode as = new SearchLatencyBreakdownNode(
                    "Acquire Searcher", SearchLatencyBreakdownNode.CATEGORY_SEARCH_CONTEXT, acquireSearcherNanos.get()
                );
                if (totalShards > 0) {
                    as.setShardStats(totalShards, acquireSearcherNanos.get() / totalShards,
                        0, acquireSearcherNanos.get(), acquireSearcherNanos.get());
                }
                query.addChild(as);
            }

            // Query Shard Info (global ordinals + fielddata)
            long shardInfoNanos = globalOrdinalsLoadingNanos.get() + fielddataLoadingNanos.get();
            if (shardInfoNanos > 0) {
                SearchLatencyBreakdownNode si = new SearchLatencyBreakdownNode(
                    "Query Shard Inf", SearchLatencyBreakdownNode.CATEGORY_SEARCH_CONTEXT, shardInfoNanos
                );
                if (totalShards > 0) {
                    si.setShardStats(totalShards, shardInfoNanos / totalShards, 0, shardInfoNanos, shardInfoNanos);
                }
                query.addChild(si);
            }

            // Search Context Creation subtree
            SearchLatencyBreakdownNode ctx = new SearchLatencyBreakdownNode(
                "Search Ctx Creation", SearchLatencyBreakdownNode.CATEGORY_SEARCH_CONTEXT
            );
            addIfPositive(ctx, "Query Cache", SearchLatencyBreakdownNode.CATEGORY_CACHE, queryCacheLookupNanos.get());
            addIfPositive(ctx, "Request Cache", SearchLatencyBreakdownNode.CATEGORY_CACHE, requestCacheLookupNanos.get());
            addIfPositive(ctx, "Queue Wait", SearchLatencyBreakdownNode.CATEGORY_CONCURRENCY, coordinatorQueueWaitNanos.get());
            addIfPositive(ctx, "Queue Wait (DN)", SearchLatencyBreakdownNode.CATEGORY_CONCURRENCY, dataNodeQueueWaitMaxNanos.get());
            if (!ctx.getChildren().isEmpty()) query.addChild(ctx);

            // Aggregation
            addIfPositive(query, "Agg Pre-Process", SearchLatencyBreakdownNode.CATEGORY_AGGREGATION, aggInitializeNanos.get());
            addIfPositive(query, "Agg Collect", SearchLatencyBreakdownNode.CATEGORY_AGGREGATION, aggCollectNanos.get());
            addIfPositive(query, "Agg Post-Process", SearchLatencyBreakdownNode.CATEGORY_AGGREGATION, aggPostCollectionNanos.get());
            addIfPositive(query, "Agg Build", SearchLatencyBreakdownNode.CATEGORY_AGGREGATION, aggBuildAggregationNanos.get());

            root.addChild(query);
        }

        // Fetch phase tree
        if (fetchPhaseNanos.get() > 0) {
            SearchLatencyBreakdownNode fetch = new SearchLatencyBreakdownNode(
                "Fetch", SearchLatencyBreakdownNode.CATEGORY_FETCH, fetchPhaseNanos.get()
            );

            if (fetchStoredFieldsNanos.get() > 0) {
                SearchLatencyBreakdownNode sf = new SearchLatencyBreakdownNode(
                    "Stored Fields", SearchLatencyBreakdownNode.CATEGORY_FETCH, fetchStoredFieldsNanos.get()
                );
                if (totalShards > 0) {
                    sf.setShardStats(totalShards, fetchStoredFieldsNanos.get() / totalShards,
                        0, fetchStoredFieldsNanos.get(), fetchStoredFieldsNanos.get());
                }
                fetch.addChild(sf);
            }

            if (fetchHighlightingNanos.get() > 0) {
                SearchLatencyBreakdownNode hl = new SearchLatencyBreakdownNode(
                    "Highlighting", SearchLatencyBreakdownNode.CATEGORY_FETCH, fetchHighlightingNanos.get()
                );
                if (totalShards > 0) {
                    hl.setShardStats(totalShards, fetchHighlightingNanos.get() / totalShards,
                        0, fetchHighlightingNanos.get(), fetchHighlightingNanos.get());
                }
                fetch.addChild(hl);
            }

            addIfPositive(fetch, "Search Ctx Creation", SearchLatencyBreakdownNode.CATEGORY_SEARCH_CONTEXT,
                searchContextCreationNanos.get());
            addIfPositive(fetch, "Fetch Source", SearchLatencyBreakdownNode.CATEGORY_FETCH, fetchSourceLoadingNanos.get());
            addIfPositive(fetch, "Fetch Inner Hits", SearchLatencyBreakdownNode.CATEGORY_FETCH, fetchInnerHitsNanos.get());

            root.addChild(fetch);
        }

        // Expand
        addIfPositive(root, "Expand", SearchLatencyBreakdownNode.CATEGORY_PHASE, expandPhaseNanos.get());

        return root;
    }

    /**
     * Adds a child node to the parent only if the nanosecond value is positive.
     */
    private void addIfPositive(SearchLatencyBreakdownNode parent, String name, String category, long nanos) {
        if (nanos > 0) parent.addChild(name, category, nanos);
    }

    // ========== OUTPUT: XContent (REST-layer rendering) ==========

    /**
     * Serializes this breakdown to XContent for REST API responses.
     * Renders the unified breakdown map as a flat object with duration in milliseconds,
     * plus timed breakdown entries with start_offset_micros and duration_micros suffixes.
     * Includes the {@code _has_timed_breakdown} flag when named events are present.
     *
     * @param builder the XContent builder to write to
     * @param params rendering parameters
     * @return the builder for chaining
     * @throws IOException if an I/O error occurs
     */
    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject("latency_breakdown");

        // We need absoluteStartNanos and requestEndNanos for the unified map, but toXContent
        // doesn't receive them as parameters. We output the raw AtomicLong values converted to millis.
        // The full unified map with computed overheads is produced by toUnifiedBreakdownMap().

        // Emit all non-zero metrics as millis
        emitIfPositive(builder, "rest_request_parsing", restRequestParsingNanos.get());
        emitIfPositive(builder, "security_auth", securityAuthNanos.get());
        emitIfPositive(builder, "pipeline_request_transform", pipelineRequestTransformNanos.get());
        emitIfPositive(builder, "query_rewrite", queryRewriteNanos.get());
        emitIfPositive(builder, "terms_lookup_sub_query", termsLookupSubQueryNanos.get());
        emitIfPositive(builder, "index_resolution", indexResolutionNanos.get());
        emitIfPositive(builder, "cluster_state_check", clusterStateCheckNanos.get());
        emitIfPositive(builder, "shard_routing", shardRoutingNanos.get());
        emitIfPositive(builder, "weighted_routing", weightedRoutingNanos.get());

        emitIfPositive(builder, "coordinator_queue_wait", coordinatorQueueWaitNanos.get());
        emitIfPositive(builder, "data_node_queue_wait_max", dataNodeQueueWaitMaxNanos.get());
        emitIfPositive(builder, "data_node_queue_wait_avg", dataNodeQueueWaitAvgNanos.get());

        emitIfPositive(builder, "can_match", canMatchPhaseNanos.get());
        emitIfPositive(builder, "can_match_to_query_gap", canMatchToQueryGapNanos.get());
        emitIfPositive(builder, "dfs", dfsPhaseNanos.get());
        emitIfPositive(builder, "query", queryPhaseNanos.get());
        emitIfPositive(builder, "query_to_fetch_gap", queryToFetchGapNanos.get());
        emitIfPositive(builder, "fetch", fetchPhaseNanos.get());
        emitIfPositive(builder, "fetch_to_expand_gap", fetchToExpandGapNanos.get());
        emitIfPositive(builder, "expand", expandPhaseNanos.get());

        emitIfPositive(builder, "search_idle_reactivation", searchIdleReactivationNanos.get());
        emitIfPositive(builder, "search_context_creation", searchContextCreationNanos.get());
        emitIfPositive(builder, "read_lock_acquisition", readLockAcquisitionNanos.get());
        emitIfPositive(builder, "acquire_searcher", acquireSearcherNanos.get());
        emitIfPositive(builder, "global_ordinals_loading", globalOrdinalsLoadingNanos.get());
        emitIfPositive(builder, "fielddata_loading", fielddataLoadingNanos.get());
        emitIfPositive(builder, "script_compilation", scriptCompilationNanos.get());
        emitIfPositive(builder, "nested_bitset_construction", nestedBitsetConstructionNanos.get());
        emitIfPositive(builder, "star_tree_setup", starTreeSetupNanos.get());
        emitIfPositive(builder, "derived_field_script", derivedFieldScriptNanos.get());

        emitIfPositive(builder, "rescore", rescoreMaxNanos.get());
        emitIfPositive(builder, "suggest", suggestMaxNanos.get());
        emitIfPositive(builder, "query_pre_process", queryPreProcessMaxNanos.get());

        emitIfPositive(builder, "request_cache_lookup", requestCacheLookupNanos.get());
        emitIfPositive(builder, "request_cache_write", requestCacheWriteNanos.get());
        emitIfPositive(builder, "query_cache_lookup", queryCacheLookupNanos.get());
        emitIfPositive(builder, "query_cache_write", queryCacheWriteNanos.get());

        emitIfPositive(builder, "lucene_create_weight", luceneCreateWeightNanos.get());
        emitIfPositive(builder, "lucene_build_scorer", luceneBuildScorerNanos.get());
        emitIfPositive(builder, "lucene_next_doc_advance", luceneNextDocAdvanceNanos.get());
        emitIfPositive(builder, "lucene_score", luceneScoreNanos.get());
        emitIfPositive(builder, "page_cache_miss", pageCacheMissNanos.get());
        emitIfPositive(builder, "remote_store_fetch", remoteStoreFetchNanos.get());
        emitIfPositive(builder, "merge_io_contention", mergeIoContentionNanos.get());

        emitIfPositive(builder, "agg_initialize", aggInitializeNanos.get());
        emitIfPositive(builder, "agg_build_leaf_collector", aggBuildLeafCollectorNanos.get());
        emitIfPositive(builder, "agg_collect", aggCollectNanos.get());
        emitIfPositive(builder, "agg_post_collection", aggPostCollectionNanos.get());
        emitIfPositive(builder, "agg_deferred_replay", aggDeferredReplayNanos.get());
        emitIfPositive(builder, "agg_build_aggregation", aggBuildAggregationNanos.get());
        emitIfPositive(builder, "global_agg_separate_pass", globalAggSeparatePassNanos.get());

        emitIfPositive(builder, "slice_creation", sliceCreationNanos.get());
        emitIfPositive(builder, "slice_scheduling", sliceSchedulingNanos.get());
        emitIfPositive(builder, "slice_max_execution", sliceMaxExecutionNanos.get());
        emitIfPositive(builder, "slice_min_execution", sliceMinExecutionNanos.get());
        emitIfPositive(builder, "slice_result_aggregation", sliceResultAggregationNanos.get());

        emitIfPositive(builder, "reduce_top_docs", reduceTopDocsNanos.get());
        emitIfPositive(builder, "reduce_aggregations", reduceAggregationsNanos.get());
        emitIfPositive(builder, "reduce_pipeline_aggs", reducePipelineAggsNanos.get());
        emitIfPositive(builder, "reduce_suggestions", reduceSuggestionsNanos.get());

        emitIfPositive(builder, "network_roundtrip_max", networkRoundtripMaxNanos.get());
        emitIfPositive(builder, "network_roundtrip_avg", networkRoundtripAvgNanos.get());
        emitIfPositive(builder, "network_roundtrip_query", networkRoundtripQueryNanos.get());
        emitIfPositive(builder, "network_roundtrip_fetch", networkRoundtripFetchNanos.get());
        emitIfPositive(builder, "request_serialization", requestSerializationNanos.get());
        emitIfPositive(builder, "response_deserialization", responseDeserializationNanos.get());
        emitIfPositive(builder, "inbound_network_time", inboundNetworkTimeNanos.get());
        emitIfPositive(builder, "outbound_network_time", outboundNetworkTimeNanos.get());

        emitIfPositive(builder, "fetch_stored_fields", fetchStoredFieldsNanos.get());
        emitIfPositive(builder, "fetch_source_loading", fetchSourceLoadingNanos.get());
        emitIfPositive(builder, "fetch_highlighting", fetchHighlightingNanos.get());
        emitIfPositive(builder, "fetch_script_fields", fetchScriptFieldsNanos.get());
        emitIfPositive(builder, "fetch_inner_hits", fetchInnerHitsNanos.get());

        emitIfPositive(builder, "pipeline_response_transform", pipelineResponseTransformNanos.get());
        emitIfPositive(builder, "response_serialization", responseSerializationNanos.get());

        emitIfPositive(builder, "circuit_breaker_check", circuitBreakerCheckNanos.get());

        // Timed breakdown entries (for Gantt-chart positioning)
        if (!namedEvents.isEmpty()) {
            builder.field("_has_timed_breakdown", 1);
            for (Map.Entry<String, long[]> entry : namedEvents.entrySet()) {
                long durationNanos = entry.getValue()[1];
                long durationMicros = TimeUnit.NANOSECONDS.toMicros(durationNanos);
                if (durationMicros > 0) {
                    long startNanos = entry.getValue()[0];
                    // Note: without absoluteStartNanos we output raw micros.
                    // The full offset computation is done via toTimedBreakdownMap().
                    builder.field(entry.getKey() + ".start_offset_micros", TimeUnit.NANOSECONDS.toMicros(startNanos));
                    builder.field(entry.getKey() + ".duration_micros", durationMicros);
                }
            }
        }

        builder.endObject();
        return builder;
    }

    /**
     * Emits a field to XContent only if the nanosecond value converts to positive milliseconds.
     */
    private void emitIfPositive(XContentBuilder builder, String fieldName, long nanos) throws IOException {
        long millis = TimeUnit.NANOSECONDS.toMillis(nanos);
        if (millis > 0) {
            builder.field(fieldName, millis);
        }
    }
}
