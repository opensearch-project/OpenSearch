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

package org.opensearch.search.internal;

import org.opensearch.Version;
import org.opensearch.action.search.SearchResponseSections;
import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.search.SearchExtBuilder;
import org.opensearch.search.SearchHits;
import org.opensearch.search.aggregations.InternalAggregations;
import org.opensearch.search.profile.SearchProfileShardResults;
import org.opensearch.search.suggest.Suggest;

import java.io.IOException;
import java.util.Collections;
import java.util.List;

/**
 * {@link SearchResponseSections} subclass that can be serialized over the wire.
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class InternalSearchResponse extends SearchResponseSections implements Writeable, ToXContentFragment {
    public static InternalSearchResponse empty() {
        return empty(true);
    }

    public static InternalSearchResponse empty(boolean withTotalHits) {
        return new InternalSearchResponse(SearchHits.empty(withTotalHits), null, null, null, false, null, 1);
    }

    public InternalSearchResponse(
        SearchHits hits,
        InternalAggregations aggregations,
        Suggest suggest,
        SearchProfileShardResults profileResults,
        boolean timedOut,
        Boolean terminatedEarly,
        int numReducePhases
    ) {
        this(hits, aggregations, suggest, profileResults, timedOut, terminatedEarly, numReducePhases, Collections.emptyList());
    }

    public InternalSearchResponse(
        SearchHits hits,
        InternalAggregations aggregations,
        Suggest suggest,
        SearchProfileShardResults profileResults,
        boolean timedOut,
        Boolean terminatedEarly,
        int numReducePhases,
        List<SearchExtBuilder> searchExtBuilderList
    ) {
        super(hits, aggregations, suggest, timedOut, terminatedEarly, profileResults, numReducePhases, searchExtBuilderList);
    }

    public InternalSearchResponse(StreamInput in) throws IOException {
        super(
            new SearchHits(in),
            in.readBoolean() ? InternalAggregations.readFrom(in) : null,
            in.readBoolean() ? new Suggest(in) : null,
            in.readBoolean(),
            in.readOptionalBoolean(),
            in.readOptionalWriteable(SearchProfileShardResults::new),
            in.readVInt(),
            readSearchExtBuildersOnOrAfter(in)
        );
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        hits.writeTo(out);
        out.writeOptionalWriteable((InternalAggregations) aggregations);
        out.writeOptionalWriteable(suggest);
        out.writeBoolean(timedOut);
        out.writeOptionalBoolean(terminatedEarly);
        out.writeOptionalWriteable(profileResults);
        out.writeVInt(numReducePhases);
        writeSearchExtBuildersOnOrAfter(out, searchExtBuilders);
    }

    private static List<SearchExtBuilder> readSearchExtBuildersOnOrAfter(StreamInput in) throws IOException {
        return (in.getVersion().onOrAfter(Version.V_2_10_0)) ? in.readNamedWriteableList(SearchExtBuilder.class) : Collections.emptyList();
    }

    private static void writeSearchExtBuildersOnOrAfter(StreamOutput out, List<SearchExtBuilder> searchExtBuilders) throws IOException {
        if (out.getVersion().onOrAfter(Version.V_2_10_0)) {
            out.writeNamedWriteableList(searchExtBuilders);
        }
    }
}
