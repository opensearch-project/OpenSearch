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
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.LongAdder;

/**
 * Tracks item level rest category class codes during indexing
 *
 * @opensearch.api
 */
@PublicApi(since = "1.0.0")
public class DocStatusStats implements Writeable, ToXContentFragment {
    final LongAdder[] docStatusCounter;

    public DocStatusStats() {
        docStatusCounter = new LongAdder[5];
        for (int i = 0; i < docStatusCounter.length; ++i) {
            docStatusCounter[i] = new LongAdder();
        }
    }

    public DocStatusStats(StreamInput in) throws IOException {
        docStatusCounter = in.readArray(i -> {
            LongAdder adder = new LongAdder();
            adder.add(i.readLong());
            return adder;

        }, LongAdder[]::new);

        assert docStatusCounter.length == 5 : "Length of incoming array should be 5! Got " + docStatusCounter.length;
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
        docStatusCounter[status.getStatusFamilyCode() - 1].add(delta);
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

        for (int i = 0; i < docStatusCounter.length; ++i) {
            docStatusCounter[i].add(stats.docStatusCounter[i].longValue());
        }
    }

    public LongAdder[] getDocStatusCounter() {
        return docStatusCounter;
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.DOC_STATUS);

        Map<String, Long> errorTypeCounts = new HashMap<>();

        for (int i = 0; i < docStatusCounter.length; ++i) {
            long count = docStatusCounter[i].longValue();

            if (count > 0) {
                RestStatus familyStatus = RestStatus.fromCode((i + 1) * 100);
                String errorType = familyStatus.getErrorType();
                errorTypeCounts.put(errorType, errorTypeCounts.getOrDefault(errorType, (long) 0) + count);
            }
        }

        String successType = RestStatus.ACCEPTED.getErrorType();
        String userFailureType = RestStatus.BAD_REQUEST.getErrorType();
        String systemErrorType = RestStatus.INTERNAL_SERVER_ERROR.getErrorType();
        builder.field(successType, errorTypeCounts.getOrDefault(successType, (long) 0));
        builder.field(userFailureType, errorTypeCounts.getOrDefault(userFailureType, (long) 0));
        builder.field(systemErrorType, errorTypeCounts.getOrDefault(systemErrorType, (long) 0));

        return builder.endObject();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeArray((o, v) -> o.writeLong(v.longValue()), docStatusCounter);
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
