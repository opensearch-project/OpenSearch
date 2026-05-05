/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.action.admin.indices.stats;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.rest.RestStatus;
import org.opensearch.core.rest.StatusType;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.atomic.LongAdder;

/**
 * Abstract class to track rest category class codes
 *
 * @opensearch.internal
 */
public abstract class AbstractStatusStats implements Writeable, ToXContentFragment {
    final protected LongAdder[] statusCounter;

    public AbstractStatusStats() {
        statusCounter = new LongAdder[5];
        for (int i = 0; i < statusCounter.length; ++i) {
            statusCounter[i] = new LongAdder();
        }
    }

    public AbstractStatusStats(StreamInput in) throws IOException {
        statusCounter = in.readArray(i -> {
            LongAdder adder = new LongAdder();
            adder.add(i.readLong());
            return adder;

        }, LongAdder[]::new);

        assert statusCounter.length == 5 : "Length of incoming array should be 5! Got " + statusCounter.length;
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
        statusCounter[status.getStatusFamilyCode() - 1].add(delta);
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {

        Map<String, Long> errorTypeCounts = new HashMap<>();

        for (int i = 0; i < statusCounter.length; ++i) {
            long count = statusCounter[i].longValue();

            if (count > 0) {
                RestStatus familyStatus = RestStatus.fromCode((i + 1) * 100);
                String errorType = familyStatus.getStatusType();
                errorTypeCounts.put(errorType, errorTypeCounts.getOrDefault(errorType, (long) 0) + count);
            }
        }

        String successType = StatusType.SUCCESS.toString();
        String userFailureType = StatusType.USER_ERROR.toString();
        String systemErrorType = StatusType.SYSTEM_FAILURE.toString();
        builder.field(successType, errorTypeCounts.getOrDefault(successType, (long) 0));
        builder.field(userFailureType, errorTypeCounts.getOrDefault(userFailureType, (long) 0));
        builder.field(systemErrorType, errorTypeCounts.getOrDefault(systemErrorType, (long) 0));

        return builder;
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeArray((o, v) -> o.writeLong(v.longValue()), statusCounter);
    }
}
