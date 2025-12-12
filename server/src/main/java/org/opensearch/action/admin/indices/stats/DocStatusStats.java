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
 * Tracks item level rest category class codes during indexing
 *
 * @opensearch.api
 */
@PublicApi(since = "3.4.0")
public class DocStatusStats extends AbstractStatusStats {

    public DocStatusStats() {
        super();
    }

    public DocStatusStats(StreamInput in) throws IOException {
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
    @Override
    public void add(final RestStatus status, final long delta) {
        super.add(status, delta);
    }

    /**
     * Accumulate stats from the passed Object
     *
     * @param stats Instance storing {@link DocStatusStats}
     */
    public void add(final DocStatusStats stats) {
        if (null == stats) {
            return;
        }

        for (int i = 0; i < statusCounter.length; ++i) {
            statusCounter[i].add(stats.statusCounter[i].longValue());
        }
    }

    public LongAdder[] getDocStatusCounter() {
        return statusCounter;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.DOC_STATUS);

        super.toXContent(builder, params);
        return builder.endObject();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        super.writeTo(out);
    }

    /**
     * For this function, I just want to retrieve a point in time snapshot of the DocStatusStats.
     */
    public DocStatusStats getSnapshot() {
        DocStatusStats curSnapshot = new DocStatusStats();
        curSnapshot.add(this);
        return curSnapshot;
    }

    /**
     * Fields for parsing and toXContent
     *
     * @opensearch.internal
     */
    static final class Fields {
        static final String DOC_STATUS = "doc_status";
    }
}
