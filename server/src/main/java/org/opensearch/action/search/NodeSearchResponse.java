/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.search;

import org.opensearch.common.annotation.InternalApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.transport.TransportResponse;
import org.opensearch.search.SearchPhaseResult;
import org.opensearch.search.SearchService;
import org.opensearch.search.fetch.QueryFetchSearchResult;
import org.opensearch.search.query.QuerySearchResult;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;

/**
 * Response node-level search response in reaction to a {@link NodeSearchRequest}.
 *
 * @opensearch.internal
 */
@InternalApi
public class NodeSearchResponse<Result extends SearchPhaseResult> extends TransportResponse {

    private final List<Result> results;
    private final List<Exception> failures;

    public NodeSearchResponse(List<Result> results, List<Exception> failures) {
        assert results.size() == failures.size() : "results and failures must have the same size";
        this.results = results;
        this.failures = failures;
    }

    private NodeSearchResponse(StreamInput in, Writeable.Reader<? extends Result> resultReader) throws IOException {
        final int size = in.readVInt();
        this.results = new ArrayList<>(size);
        this.failures = new ArrayList<>(size);
        for (int i = 0; i < size; i++) {
            if (in.readBoolean()) {
                results.add(resultReader.read(in));
                failures.add(null);
            } else {
                results.add(null);
                failures.add(in.readException());
            }
        }
    }

    static Writeable.Reader<NodeSearchResponse<SearchPhaseResult>> queryThenFetchReader(NodeSearchRequest request) {
        final Writeable.Reader<? extends SearchPhaseResult> resultReader = request.totalShardsAcrossAllNodes() == 1
            ? QueryFetchSearchResult::new
            : QuerySearchResult::new;
        return in -> new NodeSearchResponse<>(in, resultReader);
    }

    static NodeSearchResponse<SearchService.CanMatchResponse> readCanMatch(StreamInput in) throws IOException {
        return new NodeSearchResponse<>(in, SearchService.CanMatchResponse::new);
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVInt(results.size());
        for (int i = 0; i < results.size(); i++) {
            final Result result = results.get(i);
            out.writeBoolean(result != null);
            if (result != null) {
                result.writeTo(out);
            } else {
                out.writeException(failures.get(i));
            }
        }
    }

    public List<Result> results() {
        return results;
    }

    public List<Exception> failures() {
        return failures;
    }
}
