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

package org.opensearch.action.search;

import org.apache.lucene.search.TotalHits;
import org.opensearch.Version;
import org.opensearch.common.Nullable;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.common.unit.TimeValue;
import org.opensearch.common.xcontent.StatusToXContentObject;
import org.opensearch.core.ParseField;
import org.opensearch.core.action.ActionResponse;
import org.opensearch.core.common.Strings;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.MediaTypeRegistry;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.core.xcontent.XContentParseException;
import org.opensearch.core.xcontent.XContentParser;
import org.opensearch.core.xcontent.XContentParser.Token;
import org.opensearch.rest.action.RestActions;
import org.opensearch.search.GenericSearchExtBuilder;
import org.opensearch.search.SearchExtBuilder;
import org.opensearch.search.SearchHit;
import org.opensearch.search.SearchHits;
import org.opensearch.search.aggregations.Aggregations;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.internal.InternalSearchResponse;
import org.opensearch.search.pipeline.ProcessorExecutionDetail;
import org.opensearch.search.profile.ProfileShardResult;
import org.opensearch.search.profile.SearchProfileShardResults;
import org.opensearch.search.suggest.Suggest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.function.Supplier;

import static org.opensearch.action.search.SearchResponseSections.EXT_FIELD;
import static org.opensearch.action.search.SearchResponseSections.PROCESSOR_RESULT_FIELD;
import static org.opensearch.core.xcontent.XContentParserUtils.ensureExpectedToken;

/**
 * A response of a search request.
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class SearchResponse extends ActionResponse implements StatusToXContentObject {

    private static final ParseField SCROLL_ID = new ParseField("_scroll_id");
    private static final ParseField POINT_IN_TIME_ID = new ParseField("pit_id");
    private static final ParseField TOOK = new ParseField("took");
    private static final ParseField TIMED_OUT = new ParseField("timed_out");
    private static final ParseField TERMINATED_EARLY = new ParseField("terminated_early");
    private static final ParseField NUM_REDUCE_PHASES = new ParseField("num_reduce_phases");

    private final SearchResponseSections internalResponse;
    private final String scrollId;
    private final String pointInTimeId;
    private final int totalShards;
    private final int successfulShards;
    private final int skippedShards;
    private final ShardSearchFailure[] shardFailures;
    private final Clusters clusters;
    private final long tookInMillis;
    private final PhaseTook phaseTook;
    // Optional latency timeline; set at build time, null when phase_took is off or on older-version peers.
    private SearchLatencyBreakdown latencyBreakdown;

    public SearchResponse(StreamInput in) throws IOException {
        super(in);
        internalResponse = new InternalSearchResponse(in);
        totalShards = in.readVInt();
        successfulShards = in.readVInt();
        int size = in.readVInt();
        if (size == 0) {
            shardFailures = ShardSearchFailure.EMPTY_ARRAY;
        } else {
            shardFailures = new ShardSearchFailure[size];
            for (int i = 0; i < shardFailures.length; i++) {
                shardFailures[i] = ShardSearchFailure.readShardSearchFailure(in);
            }
        }
        clusters = new Clusters(in);
        scrollId = in.readOptionalString();
        tookInMillis = in.readVLong();
        if (in.getVersion().onOrAfter(Version.V_2_12_0)) {
            phaseTook = in.readOptionalWriteable(PhaseTook::new);
        } else {
            phaseTook = null;
        }
        skippedShards = in.readVInt();
        pointInTimeId = in.readOptionalString();
        if (in.getVersion().onOrAfter(Version.V_3_10_0)) {
            latencyBreakdown = in.readOptionalWriteable(SearchLatencyBreakdown::new);
        }
    }

    public SearchResponse(
        SearchResponseSections internalResponse,
        String scrollId,
        int totalShards,
        int successfulShards,
        int skippedShards,
        long tookInMillis,
        ShardSearchFailure[] shardFailures,
        Clusters clusters
    ) {
        this(internalResponse, scrollId, totalShards, successfulShards, skippedShards, tookInMillis, null, shardFailures, clusters, null);
    }

    public SearchResponse(
        SearchResponseSections internalResponse,
        String scrollId,
        int totalShards,
        int successfulShards,
        int skippedShards,
        long tookInMillis,
        ShardSearchFailure[] shardFailures,
        Clusters clusters,
        String pointInTimeId
    ) {
        this(
            internalResponse,
            scrollId,
            totalShards,
            successfulShards,
            skippedShards,
            tookInMillis,
            null,
            shardFailures,
            clusters,
            pointInTimeId
        );
    }

    public SearchResponse(
        SearchResponseSections internalResponse,
        String scrollId,
        int totalShards,
        int successfulShards,
        int skippedShards,
        long tookInMillis,
        PhaseTook phaseTook,
        ShardSearchFailure[] shardFailures,
        Clusters clusters,
        String pointInTimeId
    ) {
        this.internalResponse = internalResponse;
        this.scrollId = scrollId;
        this.pointInTimeId = pointInTimeId;
        this.clusters = clusters;
        this.totalShards = totalShards;
        this.successfulShards = successfulShards;
        this.skippedShards = skippedShards;
        this.tookInMillis = tookInMillis;
        this.phaseTook = phaseTook;
        this.shardFailures = shardFailures;
        assert skippedShards <= totalShards : "skipped: " + skippedShards + " total: " + totalShards;
        assert scrollId == null || pointInTimeId == null : "SearchResponse can't have both scrollId ["
            + scrollId
            + "] and searchContextId ["
            + pointInTimeId
            + "]";
    }

    @Override
    public RestStatus status() {
        return RestStatus.status(successfulShards, totalShards, shardFailures);
    }

    public SearchResponseSections getInternalResponse() {
        return internalResponse;
    }

    /**
     * The search hits.
     */
    public SearchHits getHits() {
        return internalResponse.hits();
    }

    public Aggregations getAggregations() {
        return internalResponse.aggregations();
    }

    public Suggest getSuggest() {
        return internalResponse.suggest();
    }

    /**
     * Has the search operation timed out.
     */
    public boolean isTimedOut() {
        return internalResponse.timedOut();
    }

    /**
     * Has the search operation terminated early due to reaching
     * <code>terminateAfter</code>
     */
    public Boolean isTerminatedEarly() {
        return internalResponse.terminatedEarly();
    }

    /**
     * Returns the number of reduce phases applied to obtain this search response
     */
    public int getNumReducePhases() {
        return internalResponse.getNumReducePhases();
    }

    /**
     * How long the search took.
     */
    public TimeValue getTook() {
        return new TimeValue(tookInMillis);
    }

    /**
     * How long the request took in each search phase.
     */
    public PhaseTook getPhaseTook() {
        return phaseTook;
    }

    /**
     * The per-phase latency breakdown for Gantt-style rendering, or {@code null} when phase timing output
     * was not requested (or when received from a peer older than the introducing version).
     */
    public SearchLatencyBreakdown getLatencyBreakdown() {
        return latencyBreakdown;
    }

    /**
     * Sets the latency breakdown. Package-private so the only producer is the coordinator's response build
     * path, which sources it from {@code SearchRequestContext.getLatencyBreakdown()} — {@code null} unless the
     * {@code phase_took} opt-in is enabled. This keeps the field gated at its single entry point; leaving it
     * unset keeps it out of both the wire format and XContent, including on error/empty responses.
     */
    void setLatencyBreakdown(SearchLatencyBreakdown latencyBreakdown) {
        this.latencyBreakdown = latencyBreakdown;
    }

    /**
     * The total number of shards the search was executed on.
     */
    public int getTotalShards() {
        return totalShards;
    }

    /**
     * The successful number of shards the search was executed on.
     */
    public int getSuccessfulShards() {
        return successfulShards;
    }

    /**
     * The number of shards skipped due to pre-filtering
     */
    public int getSkippedShards() {
        return skippedShards;
    }

    /**
     * The failed number of shards the search was executed on.
     */
    public int getFailedShards() {
        // we don't return totalShards - successfulShards, we don't count "no shards available" as a failed shard, just don't
        // count it in the successful counter
        return shardFailures.length;
    }

    /**
     * The failures that occurred during the search.
     */
    public ShardSearchFailure[] getShardFailures() {
        return this.shardFailures;
    }

    /**
     * If scrolling was enabled ({@link SearchRequest#scroll(org.opensearch.search.Scroll)}, the
     * scroll id that can be used to continue scrolling.
     */
    public String getScrollId() {
        return scrollId;
    }

    /**
     * Returns the encoded string of the search context that the search request is used to executed
     */
    public String pointInTimeId() {
        return pointInTimeId;
    }

    /**
     * If profiling was enabled, this returns an object containing the profile results from
     * each shard.  If profiling was not enabled, this will return null
     *
     * @return The profile results or an empty map
     */
    @Nullable
    public Map<String, ProfileShardResult> getProfileResults() {
        return internalResponse.profile();
    }

    /**
     * Returns info about what clusters the search was executed against. Available only in responses obtained
     * from a Cross Cluster Search request, otherwise <code>null</code>
     * @see Clusters
     */
    public Clusters getClusters() {
        return clusters;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject();
        innerToXContent(builder, params);
        builder.endObject();
        return builder;
    }

    public XContentBuilder innerToXContent(XContentBuilder builder, Params params) throws IOException {
        if (scrollId != null) {
            builder.field(SCROLL_ID.getPreferredName(), scrollId);
        }
        if (pointInTimeId != null) {
            builder.field(POINT_IN_TIME_ID.getPreferredName(), pointInTimeId);
        }
        builder.field(TOOK.getPreferredName(), tookInMillis);
        if (phaseTook != null) {
            phaseTook.toXContent(builder, params);
        }
        // Non-null only when phase_took was opted into (see setLatencyBreakdown); emitted like phase_took above.
        if (latencyBreakdown != null) {
            latencyBreakdown.toXContent(builder, params);
        }
        builder.field(TIMED_OUT.getPreferredName(), isTimedOut());
        if (isTerminatedEarly() != null) {
            builder.field(TERMINATED_EARLY.getPreferredName(), isTerminatedEarly());
        }
        if (getNumReducePhases() != 1) {
            builder.field(NUM_REDUCE_PHASES.getPreferredName(), getNumReducePhases());
        }
        RestActions.buildBroadcastShardsHeader(
            builder,
            params,
            getTotalShards(),
            getSuccessfulShards(),
            getSkippedShards(),
            getFailedShards(),
            getShardFailures()
        );
        clusters.toXContent(builder, params);
        internalResponse.toXContent(builder, params);

        return builder;
    }

    public static SearchResponse fromXContent(XContentParser parser) throws IOException {
        ensureExpectedToken(Token.START_OBJECT, parser.nextToken(), parser);
        parser.nextToken();
        return innerFromXContent(parser);
    }

    public static SearchResponse innerFromXContent(XContentParser parser) throws IOException {
        ensureExpectedToken(Token.FIELD_NAME, parser.currentToken(), parser);
        String currentFieldName = parser.currentName();
        SearchHits hits = null;
        Aggregations aggs = null;
        Suggest suggest = null;
        SearchProfileShardResults profile = null;
        boolean timedOut = false;
        Boolean terminatedEarly = null;
        int numReducePhases = 1;
        long tookInMillis = -1;
        PhaseTook phaseTook = null;
        int successfulShards = -1;
        int totalShards = -1;
        int skippedShards = 0; // 0 for BWC
        String scrollId = null;
        String searchContextId = null;
        List<ShardSearchFailure> failures = new ArrayList<>();
        Clusters clusters = Clusters.EMPTY;
        List<SearchExtBuilder> extBuilders = new ArrayList<>();
        List<ProcessorExecutionDetail> processorResult = new ArrayList<>();
        for (Token token = parser.nextToken(); token != Token.END_OBJECT; token = parser.nextToken()) {
            if (token == Token.FIELD_NAME) {
                currentFieldName = parser.currentName();
            } else if (token.isValue()) {
                if (SCROLL_ID.match(currentFieldName, parser.getDeprecationHandler())) {
                    scrollId = parser.text();
                } else if (POINT_IN_TIME_ID.match(currentFieldName, parser.getDeprecationHandler())) {
                    searchContextId = parser.text();
                } else if (TOOK.match(currentFieldName, parser.getDeprecationHandler())) {
                    tookInMillis = parser.longValue();
                } else if (TIMED_OUT.match(currentFieldName, parser.getDeprecationHandler())) {
                    timedOut = parser.booleanValue();
                } else if (TERMINATED_EARLY.match(currentFieldName, parser.getDeprecationHandler())) {
                    terminatedEarly = parser.booleanValue();
                } else if (NUM_REDUCE_PHASES.match(currentFieldName, parser.getDeprecationHandler())) {
                    numReducePhases = parser.intValue();
                } else {
                    parser.skipChildren();
                }
            } else if (token == Token.START_OBJECT) {
                if (SearchHits.Fields.HITS.equals(currentFieldName)) {
                    hits = SearchHits.fromXContent(parser);
                } else if (Aggregations.AGGREGATIONS_FIELD.equals(currentFieldName)) {
                    aggs = Aggregations.fromXContent(parser);
                } else if (Suggest.NAME.equals(currentFieldName)) {
                    suggest = Suggest.fromXContent(parser);
                } else if (SearchProfileShardResults.PROFILE_FIELD.equals(currentFieldName)) {
                    profile = SearchProfileShardResults.fromXContent(parser);
                } else if (RestActions._SHARDS_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    while ((token = parser.nextToken()) != Token.END_OBJECT) {
                        if (token == Token.FIELD_NAME) {
                            currentFieldName = parser.currentName();
                        } else if (token.isValue()) {
                            if (RestActions.FAILED_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                                parser.intValue(); // we don't need it but need to consume it
                            } else if (RestActions.SUCCESSFUL_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                                successfulShards = parser.intValue();
                            } else if (RestActions.TOTAL_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                                totalShards = parser.intValue();
                            } else if (RestActions.SKIPPED_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                                skippedShards = parser.intValue();
                            } else {
                                parser.skipChildren();
                            }
                        } else if (token == Token.START_ARRAY) {
                            if (RestActions.FAILURES_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                                while ((token = parser.nextToken()) != Token.END_ARRAY) {
                                    failures.add(ShardSearchFailure.fromXContent(parser));
                                }
                            } else {
                                parser.skipChildren();
                            }
                        } else {
                            parser.skipChildren();
                        }
                    }
                } else if (PhaseTook.PHASE_TOOK.match(currentFieldName, parser.getDeprecationHandler())) {
                    Map<String, Long> phaseTookMap = new HashMap<>();

                    while ((token = parser.nextToken()) != Token.END_OBJECT) {
                        if (token == Token.FIELD_NAME) {
                            currentFieldName = parser.currentName();
                        } else if (token.isValue()) {
                            try {
                                SearchPhaseName.valueOf(currentFieldName.toUpperCase(Locale.ROOT));
                                phaseTookMap.put(currentFieldName, parser.longValue());
                            } catch (final IllegalArgumentException ex) {
                                parser.skipChildren();
                            }
                        } else {
                            parser.skipChildren();
                        }
                    }
                    phaseTook = new PhaseTook(phaseTookMap);
                } else if (Clusters._CLUSTERS_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    int successful = -1;
                    int total = -1;
                    int skipped = -1;
                    while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                        if (token == XContentParser.Token.FIELD_NAME) {
                            currentFieldName = parser.currentName();
                        } else if (token.isValue()) {
                            if (Clusters.SUCCESSFUL_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                                successful = parser.intValue();
                            } else if (Clusters.TOTAL_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                                total = parser.intValue();
                            } else if (Clusters.SKIPPED_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                                skipped = parser.intValue();
                            } else {
                                parser.skipChildren();
                            }
                        } else {
                            parser.skipChildren();
                        }
                    }
                    clusters = new Clusters(total, successful, skipped);
                } else if (EXT_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    String extSectionName = null;
                    while ((token = parser.nextToken()) != XContentParser.Token.END_OBJECT) {
                        if (token == XContentParser.Token.FIELD_NAME) {
                            extSectionName = parser.currentName();
                        } else {
                            SearchExtBuilder searchExtBuilder;
                            try {
                                searchExtBuilder = parser.namedObject(SearchExtBuilder.class, extSectionName, null);
                                if (!searchExtBuilder.getWriteableName().equals(extSectionName)) {
                                    throw new IllegalStateException(
                                        "The parsed ["
                                            + searchExtBuilder.getClass().getName()
                                            + "] object has a "
                                            + "different writeable name compared to the name of the section that it was parsed from: found ["
                                            + searchExtBuilder.getWriteableName()
                                            + "] expected ["
                                            + extSectionName
                                            + "]"
                                    );
                                }
                            } catch (XContentParseException e) {
                                searchExtBuilder = GenericSearchExtBuilder.fromXContent(parser);
                            }
                            extBuilders.add(searchExtBuilder);
                        }
                    }
                } else if (PROCESSOR_RESULT_FIELD.match(currentFieldName, parser.getDeprecationHandler())) {
                    while ((token = parser.nextToken()) != Token.END_ARRAY) {
                        ProcessorExecutionDetail detail = ProcessorExecutionDetail.fromXContent(parser);
                        processorResult.add(detail);
                    }
                } else {
                    parser.skipChildren();
                }
            }
        }
        SearchResponseSections searchResponseSections = new SearchResponseSections(
            hits,
            aggs,
            suggest,
            timedOut,
            terminatedEarly,
            profile,
            numReducePhases,
            extBuilders,
            processorResult
        );
        return new SearchResponse(
            searchResponseSections,
            scrollId,
            totalShards,
            successfulShards,
            skippedShards,
            tookInMillis,
            phaseTook,
            failures.toArray(ShardSearchFailure.EMPTY_ARRAY),
            clusters,
            searchContextId
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        internalResponse.writeTo(out);
        out.writeVInt(totalShards);
        out.writeVInt(successfulShards);

        out.writeVInt(shardFailures.length);
        for (ShardSearchFailure shardSearchFailure : shardFailures) {
            shardSearchFailure.writeTo(out);
        }
        clusters.writeTo(out);
        out.writeOptionalString(scrollId);
        out.writeVLong(tookInMillis);
        if (out.getVersion().onOrAfter(Version.V_2_12_0)) {
            out.writeOptionalWriteable(phaseTook);
        }
        out.writeVInt(skippedShards);
        out.writeOptionalString(pointInTimeId);
        if (out.getVersion().onOrAfter(Version.V_3_10_0)) {
            out.writeOptionalWriteable(latencyBreakdown);
        }
    }

    @Override
    public String toString() {
        return Strings.toString(MediaTypeRegistry.JSON, this);
    }

    /**
     * Holds info about the clusters that the search was executed on: how many in total, how many of them were successful
     * and how many of them were skipped.
     *
     * @opensearch.api
     */
    @PublicApi(since = "1.0.0")
    public static class Clusters implements ToXContentFragment, Writeable {

        public static final Clusters EMPTY = new Clusters(0, 0, 0);

        static final ParseField _CLUSTERS_FIELD = new ParseField("_clusters");
        static final ParseField SUCCESSFUL_FIELD = new ParseField("successful");
        static final ParseField SKIPPED_FIELD = new ParseField("skipped");
        static final ParseField TOTAL_FIELD = new ParseField("total");

        private final int total;
        private final int successful;
        private final int skipped;

        public Clusters(int total, int successful, int skipped) {
            assert total >= 0 && successful >= 0 && skipped >= 0 : "total: "
                + total
                + " successful: "
                + successful
                + " skipped: "
                + skipped;
            assert successful <= total && skipped == total - successful : "total: "
                + total
                + " successful: "
                + successful
                + " skipped: "
                + skipped;
            this.total = total;
            this.successful = successful;
            this.skipped = skipped;
        }

        private Clusters(StreamInput in) throws IOException {
            this(in.readVInt(), in.readVInt(), in.readVInt());
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeVInt(total);
            out.writeVInt(successful);
            out.writeVInt(skipped);
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            if (total > 0) {
                builder.startObject(_CLUSTERS_FIELD.getPreferredName());
                builder.field(TOTAL_FIELD.getPreferredName(), total);
                builder.field(SUCCESSFUL_FIELD.getPreferredName(), successful);
                builder.field(SKIPPED_FIELD.getPreferredName(), skipped);
                builder.endObject();
            }
            return builder;
        }

        /**
         * Returns how many total clusters the search was requested to be executed on
         */
        public int getTotal() {
            return total;
        }

        /**
         * Returns how many total clusters the search was executed successfully on
         */
        public int getSuccessful() {
            return successful;
        }

        /**
         * Returns how many total clusters were during the execution of the search request
         */
        public int getSkipped() {
            return skipped;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            Clusters clusters = (Clusters) o;
            return total == clusters.total && successful == clusters.successful && skipped == clusters.skipped;
        }

        @Override
        public int hashCode() {
            return Objects.hash(total, successful, skipped);
        }

        @Override
        public String toString() {
            return "Clusters{total=" + total + ", successful=" + successful + ", skipped=" + skipped + '}';
        }
    }

    /**
     * Holds info about the clusters that the search was executed on: how many in total, how many of them were successful
     * and how many of them were skipped.
     *
     * @opensearch.api
     */
    @PublicApi(since = "1.0.0")
    public static class PhaseTook implements ToXContentFragment, Writeable {
        static final ParseField PHASE_TOOK = new ParseField("phase_took");
        private final Map<String, Long> phaseTookMap;

        public PhaseTook(Map<String, Long> phaseTookMap) {
            this.phaseTookMap = phaseTookMap;
        }

        private PhaseTook(StreamInput in) throws IOException {
            this(in.readMap(StreamInput::readString, StreamInput::readLong));
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeMap(phaseTookMap, StreamOutput::writeString, StreamOutput::writeLong);
        }

        public Map<String, Long> getPhaseTookMap() {
            return phaseTookMap;
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            builder.startObject(PHASE_TOOK.getPreferredName());

            for (SearchPhaseName searchPhaseName : SearchPhaseName.values()) {
                if (phaseTookMap.containsKey(searchPhaseName.getName())) {
                    builder.field(searchPhaseName.getName(), phaseTookMap.get(searchPhaseName.getName()));
                } else {
                    builder.field(searchPhaseName.getName(), 0);
                }
            }
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            PhaseTook phaseTook = (PhaseTook) o;

            if (phaseTook.phaseTookMap.equals(phaseTookMap)) {
                return true;
            } else {
                return false;
            }
        }

        @Override
        public int hashCode() {
            return Objects.hash(phaseTookMap);
        }
    }

    /**
     * A coordinator-side latency timeline for Gantt-style rendering. Each search phase and coordinator event
     * carries a start offset and duration in microseconds, derived from timestamps the coordinator already
     * captures plus a few per-request timers, so it adds no per-shard or per-document overhead.
     *
     * @opensearch.api
     */
    @PublicApi(since = "3.10.0")
    public static class SearchLatencyBreakdown implements ToXContentFragment, Writeable {
        static final ParseField LATENCY_BREAKDOWN = new ParseField("latency_breakdown");
        static final String START_OFFSET_MICROS = "start_offset_micros";
        static final String DURATION_MICROS = "duration_micros";

        // All times in microseconds. Coordinator event values are [startOffsetMicros, durationMicros].
        private final Map<String, Long> phaseStartOffsetMicrosMap;
        private final Map<String, Long> phaseDurationMicrosMap;
        private final Map<String, long[]> coordinatorEventMap;

        public SearchLatencyBreakdown(
            Map<String, Long> phaseStartOffsetMicrosMap,
            Map<String, Long> phaseDurationMicrosMap,
            Map<String, long[]> coordinatorEventMap
        ) {
            this.phaseStartOffsetMicrosMap = Collections.unmodifiableMap(new HashMap<>(phaseStartOffsetMicrosMap));
            this.phaseDurationMicrosMap = Collections.unmodifiableMap(new HashMap<>(phaseDurationMicrosMap));
            this.coordinatorEventMap = Collections.unmodifiableMap(new HashMap<>(coordinatorEventMap));
        }

        private SearchLatencyBreakdown(StreamInput in) throws IOException {
            this(
                in.readMap(StreamInput::readString, StreamInput::readLong),
                in.readMap(StreamInput::readString, StreamInput::readLong),
                in.readMap(StreamInput::readString, i -> new long[] { i.readLong(), i.readLong() })
            );
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeMap(phaseStartOffsetMicrosMap, StreamOutput::writeString, StreamOutput::writeLong);
            out.writeMap(phaseDurationMicrosMap, StreamOutput::writeString, StreamOutput::writeLong);
            out.writeMap(coordinatorEventMap, StreamOutput::writeString, (o, v) -> {
                o.writeLong(v[0]);
                o.writeLong(v[1]);
            });
        }

        /** Read-only map of phase name to start offset in microseconds. */
        public Map<String, Long> getPhaseStartOffsetMicrosMap() {
            return phaseStartOffsetMicrosMap;
        }

        /** Read-only map of phase name to duration in microseconds. */
        public Map<String, Long> getPhaseDurationMicrosMap() {
            return phaseDurationMicrosMap;
        }

        /** Read-only map of coordinator event name to {@code [startOffsetMicros, durationMicros]}. */
        public Map<String, long[]> getCoordinatorEventMap() {
            return coordinatorEventMap;
        }

        private boolean phaseExecuted(String phase) {
            return phaseStartOffsetMicrosMap.containsKey(phase) && phaseDurationMicrosMap.containsKey(phase);
        }

        /** Executed phase names ordered by actual start offset (not enum declaration order). */
        private List<String> executedPhasesByStartOffset() {
            List<String> phases = new ArrayList<>();
            for (SearchPhaseName phaseName : SearchPhaseName.values()) {
                if (phaseExecuted(phaseName.getName())) {
                    phases.add(phaseName.getName());
                }
            }
            phases.sort(Comparator.comparingLong(phaseStartOffsetMicrosMap::get));
            return phases;
        }

        /** Largest gap between one executed phase's end and the next's start (the query&rarr;fetch reduce), or null. */
        private long[] deriveReduceAndCoordinate() {
            long bestGapStartMicros = -1;
            long bestGapMicros = -1;
            long prevEndMicros = -1;
            // Walk phases in temporal order so adjacent-phase gaps are real, regardless of enum declaration order.
            for (String phase : executedPhasesByStartOffset()) {
                long thisStart = phaseStartOffsetMicrosMap.get(phase);
                long gap = thisStart - prevEndMicros;
                if (prevEndMicros >= 0 && gap > 0 && gap > bestGapMicros) {
                    bestGapMicros = gap;
                    bestGapStartMicros = prevEndMicros;
                }
                prevEndMicros = thisStart + phaseDurationMicrosMap.get(phase);
            }
            if (bestGapMicros <= 0) {
                return null;
            }
            return new long[] { bestGapStartMicros, bestGapMicros };
        }

        /** Span from the last coordinator event's end to the first executed phase's start (routing + fan-out), or null. */
        private long[] deriveCoordinatorDispatch() {
            long lastEventEndMicros = 0;
            for (long[] v : coordinatorEventMap.values()) {
                lastEventEndMicros = Math.max(lastEventEndMicros, v[0] + v[1]);
            }
            long firstPhaseStartMicros = -1;
            for (SearchPhaseName phaseName : SearchPhaseName.values()) {
                String phase = phaseName.getName();
                if (phaseExecuted(phase) == false) {
                    continue;
                }
                long startOffsetMicros = phaseStartOffsetMicrosMap.get(phase);
                if (firstPhaseStartMicros < 0 || startOffsetMicros < firstPhaseStartMicros) {
                    firstPhaseStartMicros = startOffsetMicros;
                }
            }
            if (firstPhaseStartMicros < 0 || firstPhaseStartMicros <= lastEventEndMicros) {
                return null;
            }
            return new long[] { lastEventEndMicros, firstPhaseStartMicros - lastEventEndMicros };
        }

        @Override
        public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
            Map<String, long[]> timeline = new LinkedHashMap<>();
            timeline.putAll(coordinatorEventMap);

            long[] reduceGap = deriveReduceAndCoordinate();
            if (reduceGap != null) {
                timeline.put(CoordinatorLatencyEventName.REDUCE_AND_COORDINATE.getName(), reduceGap);
            }
            long[] dispatch = deriveCoordinatorDispatch();
            if (dispatch != null) {
                timeline.put(CoordinatorLatencyEventName.COORDINATOR_DISPATCH.getName(), dispatch);
            }
            // Emit only phases that actually ran; unexecuted phases are omitted rather than shown as zero bars.
            for (String phase : executedPhasesByStartOffset()) {
                timeline.put(phase, new long[] { phaseStartOffsetMicrosMap.get(phase), phaseDurationMicrosMap.get(phase) });
            }

            List<Map.Entry<String, long[]>> ordered = new ArrayList<>(timeline.entrySet());
            ordered.sort(Comparator.<Map.Entry<String, long[]>>comparingLong(en -> en.getValue()[0]).thenComparing(Map.Entry::getKey));

            builder.startObject(LATENCY_BREAKDOWN.getPreferredName());
            for (Map.Entry<String, long[]> en : ordered) {
                builder.startObject(en.getKey());
                builder.field(START_OFFSET_MICROS, en.getValue()[0]);
                builder.field(DURATION_MICROS, en.getValue()[1]);
                builder.endObject();
            }
            builder.endObject();
            return builder;
        }

        @Override
        public boolean equals(Object o) {
            if (this == o) {
                return true;
            }
            if (o == null || getClass() != o.getClass()) {
                return false;
            }
            SearchLatencyBreakdown that = (SearchLatencyBreakdown) o;
            return phaseStartOffsetMicrosMap.equals(that.phaseStartOffsetMicrosMap)
                && phaseDurationMicrosMap.equals(that.phaseDurationMicrosMap)
                && coordinatorEventMapEquals(that.coordinatorEventMap);
        }

        private boolean coordinatorEventMapEquals(Map<String, long[]> other) {
            if (coordinatorEventMap.size() != other.size()) {
                return false;
            }
            for (Map.Entry<String, long[]> e : coordinatorEventMap.entrySet()) {
                long[] o = other.get(e.getKey());
                if (o == null || o.length != e.getValue().length || o[0] != e.getValue()[0] || o[1] != e.getValue()[1]) {
                    return false;
                }
            }
            return true;
        }

        @Override
        public int hashCode() {
            int result = Objects.hash(phaseStartOffsetMicrosMap, phaseDurationMicrosMap);
            for (Map.Entry<String, long[]> e : coordinatorEventMap.entrySet()) {
                result = 31 * result + e.getKey().hashCode() + Long.hashCode(e.getValue()[0]) + Long.hashCode(e.getValue()[1]);
            }
            return result;
        }
    }

    static SearchResponse empty(Supplier<Long> tookInMillisSupplier, Clusters clusters) {
        SearchHits searchHits = new SearchHits(new SearchHit[0], new TotalHits(0L, TotalHits.Relation.EQUAL_TO), Float.NaN);
        InternalSearchResponse internalSearchResponse = new InternalSearchResponse(
            searchHits,
            InternalAggregations.EMPTY,
            null,
            null,
            false,
            null,
            0
        );
        return new SearchResponse(
            internalSearchResponse,
            null,
            0,
            0,
            0,
            tookInMillisSupplier.get(),
            ShardSearchFailure.EMPTY_ARRAY,
            clusters,
            null
        );
    }
}
