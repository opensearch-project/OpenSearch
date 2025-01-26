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

import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.ParseField;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;
import org.opensearch.search.SearchExtBuilder;
import org.opensearch.search.SearchHits;
import org.opensearch.search.aggregations.Aggregations;
import org.opensearch.search.pipeline.ProcessorExecutionDetail;
import org.opensearch.search.profile.ProfileShardResult;
import org.opensearch.search.profile.SearchProfileShardResults;
import org.opensearch.search.stream.OSTicket;
import org.opensearch.search.suggest.Suggest;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * Base class that holds the various sections which a search response is
 * composed of (hits, aggs, suggestions etc.) and allows to retrieve them.
 * <p>
 * The reason why this class exists is that the high level REST client uses its own classes
 * to parse aggregations into, which are not serializable. This is the common part that can be
 * shared between core and client.
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class SearchResponseSections implements ToXContentFragment {

    public static final ParseField EXT_FIELD = new ParseField("ext");
    public static final ParseField PROCESSOR_RESULT_FIELD = new ParseField("processor_results");
    public static final String TICKET_FIELD = "ticket";

    protected final SearchHits hits;
    protected final Aggregations aggregations;
    protected final Suggest suggest;
    protected final SearchProfileShardResults profileResults;
    protected final List<OSTicket> tickets;
    protected final boolean timedOut;
    protected final Boolean terminatedEarly;
    protected final int numReducePhases;
    protected final List<SearchExtBuilder> searchExtBuilders = new ArrayList<>();
    protected final List<ProcessorExecutionDetail> processorResult = new ArrayList<>();

    public SearchResponseSections(
        SearchHits hits,
        Aggregations aggregations,
        Suggest suggest,
        boolean timedOut,
        Boolean terminatedEarly,
        SearchProfileShardResults profileResults,
        int numReducePhases
    ) {
        this(hits, aggregations, suggest, timedOut, terminatedEarly, profileResults, numReducePhases, Collections.emptyList());
    }

    public SearchResponseSections(
        SearchHits hits,
        Aggregations aggregations,
        Suggest suggest,
        boolean timedOut,
        Boolean terminatedEarly,
        SearchProfileShardResults profileResults,
        int numReducePhases,
        List<SearchExtBuilder> searchExtBuilders,
        List<ProcessorExecutionDetail> processorResult,
        List<OSTicket> tickets
    ) {
        this.hits = hits;
        this.aggregations = aggregations;
        this.suggest = suggest;
        this.profileResults = profileResults;
        this.timedOut = timedOut;
        this.terminatedEarly = terminatedEarly;
        this.numReducePhases = numReducePhases;
        this.processorResult.addAll(processorResult);
        this.searchExtBuilders.addAll(Objects.requireNonNull(searchExtBuilders, "searchExtBuilders must not be null"));
        this.tickets = tickets;
    }

    public SearchResponseSections(
        SearchHits hits,
        Aggregations aggregations,
        Suggest suggest,
        boolean timedOut,
        Boolean terminatedEarly,
        SearchProfileShardResults profileResults,
        int numReducePhases,
        List<SearchExtBuilder> searchExtBuilders,
        List<ProcessorExecutionDetail> processorResult
    ) {
        this(
            hits,
            aggregations,
            suggest,
            timedOut,
            terminatedEarly,
            profileResults,
            numReducePhases,
            searchExtBuilders,
            Collections.emptyList(),
            null
        );
    }

    public SearchResponseSections(
        SearchHits hits,
        Aggregations aggregations,
        Suggest suggest,
        boolean timedOut,
        Boolean terminatedEarly,
        SearchProfileShardResults profileResults,
        int numReducePhases,
        List<SearchExtBuilder> searchExtBuilders
    ) {
        this(
            hits,
            aggregations,
            suggest,
            timedOut,
            terminatedEarly,
            profileResults,
            numReducePhases,
            searchExtBuilders,
            Collections.emptyList(),
            null
        );
    }

    public final boolean timedOut() {
        return this.timedOut;
    }

    public final Boolean terminatedEarly() {
        return this.terminatedEarly;
    }

    public final SearchHits hits() {
        return hits;
    }

    public final Aggregations aggregations() {
        return aggregations;
    }

    public final Suggest suggest() {
        return suggest;
    }

    /**
     * Returns the number of reduce phases applied to obtain this search response
     */
    public final int getNumReducePhases() {
        return numReducePhases;
    }

    /**
     * Returns the profile results for this search response (including all shards).
     * An empty map is returned if profiling was not enabled
     *
     * @return Profile results
     */
    public final Map<String, ProfileShardResult> profile() {
        if (profileResults == null) {
            return Collections.emptyMap();
        }
        return profileResults.getShardResults();
    }

    /**
     * Returns the profile results for this search response (including all shards).
     * An empty map is returned if profiling was not enabled
     *
     * @return Profile results
     */
    public final List<OSTicket> tickets() {
        if (tickets == null) {
            return Collections.emptyList();
        }
        return tickets;
    }

    @Override
    public final XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        hits.toXContent(builder, params);
        if (aggregations != null) {
            aggregations.toXContent(builder, params);
        }
        if (suggest != null) {
            suggest.toXContent(builder, params);
        }
        if (profileResults != null) {
            profileResults.toXContent(builder, params);
        }
        if (!searchExtBuilders.isEmpty()) {
            builder.startObject(EXT_FIELD.getPreferredName());
            for (SearchExtBuilder searchExtBuilder : searchExtBuilders) {
                searchExtBuilder.toXContent(builder, params);
            }
            builder.endObject();
        }

        if (!processorResult.isEmpty()) {
            builder.field(PROCESSOR_RESULT_FIELD.getPreferredName(), processorResult);
        }

        if (tickets != null && !tickets.isEmpty()) {
            builder.startArray(TICKET_FIELD);
            for (OSTicket ticket : tickets) {
                ticket.toXContent(builder, params);
            }
            builder.endArray();
        }
        return builder;
    }

    public List<SearchExtBuilder> getSearchExtBuilders() {
        return Collections.unmodifiableList(this.searchExtBuilders);
    }

    public List<ProcessorExecutionDetail> getProcessorResult() {
        return processorResult;
    }

    protected void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException();
    }
}
