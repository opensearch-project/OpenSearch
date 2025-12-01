/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.stats;

import org.opensearch.common.annotation.PublicApi;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.concurrent.atomic.LongAdder;

/**
 * Tracks rest category class codes from search requests
 *
 * @opensearch.api
 */
@PublicApi(since = "3.4.0")
public class SearchResponseStatusStats extends AbstractStatusStats {

    public SearchResponseStatusStats() {
        super();
    }

    public SearchResponseStatusStats(StreamInput in) throws IOException {
        super(in);
    }

    /**
     * Increment counter for status
     *
     * @param status {@link RestStatus}
     */
    public void inc(final RestStatus status) {
        add(status, 1L);
    }

    /**
     * Increment counter for status by count
     *
     * @param status {@link RestStatus}
     * @param delta The value to add
     */
    public void add(final RestStatus status, final long delta) {
        super.add(status, delta);
    }

    /**
     * Accumulate stats from the passed Object
     *
     * @param stats Instance storing {@link SearchResponseStatusStats}
     */
    public void add(final SearchResponseStatusStats stats) {
        if (null == stats) {
            return;
        }

        for (int i = 0; i < statusCounter.length; ++i) {
            statusCounter[i].add(stats.statusCounter[i].longValue());
        }
    }

    public LongAdder[] getSearchResponseStatusCounter() {
        return statusCounter;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.SEARCH_RESPONSE_STATUS);

        super.toXContent(builder, params);
        return builder.endObject();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
    }

    /**
     * For this function, I just want to retrieve a point in time snapshot of the SearchResponseStatusStats.
     */
    public SearchResponseStatusStats getSnapshot() {
        SearchResponseStatusStats curSnapshot = new SearchResponseStatusStats();
        curSnapshot.add(this);
        return curSnapshot;
    }

    /**
     * Fields for parsing and toXContent
     *
     * @opensearch.internal
     */
    static final class Fields {
        static final String SEARCH_RESPONSE_STATUS = "search_response_status";
    }
}
