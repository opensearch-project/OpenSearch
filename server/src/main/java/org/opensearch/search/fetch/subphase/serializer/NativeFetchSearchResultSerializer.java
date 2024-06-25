/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.search.fetch.subphase.serializer;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.search.SearchHits;

import java.io.IOException;

public class NativeFetchSearchResultSerializer implements FetchSearchResultSerializer<StreamInput, StreamOutput> {

    private SearchHits hits;

    @Override
    public void readFetchSearchResult(StreamInput in) throws IOException {
        hits = new SearchHits(in);
    }

    @Override
    public void writeFetchSearchResult(StreamOutput out) throws IOException {
        hits.writeTo(out);
    }

    public SearchHits getHits() {
        return hits;
    }

    public void setHits(SearchHits hits) {
        this.hits = hits;
    }

}
