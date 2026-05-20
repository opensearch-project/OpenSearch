/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.common.cache.policy;

import org.opensearch.common.annotation.ExperimentalApi;
import org.opensearch.core.common.bytes.BytesReference;
import org.opensearch.core.common.io.stream.NamedWriteableAwareStreamInput;
import org.opensearch.core.common.io.stream.NamedWriteableRegistry;
import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.search.internal.ShardSearchContextId;
import org.opensearch.search.query.QuerySearchResult;

import java.io.IOException;

/**
 * A class containing a QuerySearchResult used in a cache, as well as information needed for all cache policies
 * to decide whether to admit a given BytesReference. Also handles serialization/deserialization of the underlying QuerySearchResult,
 * which is all that is needed outside the cache. At policy checking time, this spares us from having to create an entire
 * short-lived QuerySearchResult object just to read a few values.
 * @opensearch.internal
 */
public class CachedQueryResult {
    private final PolicyValues policyValues;
    private final QuerySearchResult qsr;

    public CachedQueryResult(QuerySearchResult qsr, long tookTimeNanos) {
        this.qsr = qsr;
        this.policyValues = new PolicyValues(tookTimeNanos);
    }

    // Retrieve only took time from a serialized CQR, without creating a short-lived QuerySearchResult or CachedQueryResult object.
    public static PolicyValues getPolicyValues(BytesReference serializedCQR) throws IOException {
        StreamInput in = serializedCQR.streamInput();
        return new PolicyValues(in);
    }

    // Retrieve only the QSR from a serialized CQR, and load it into an existing QSR object discarding the took time which isn't needed
    // outside the cache
    public static void loadQSR(
        BytesReference serializedCQR,
        QuerySearchResult qsr,
        ShardSearchContextId id,
        NamedWriteableRegistry registry
    ) throws IOException {
        StreamInput in = new NamedWriteableAwareStreamInput(serializedCQR.streamInput(), registry);
        PolicyValues pv = new PolicyValues(in); // Read and discard PolicyValues
        qsr.readFromWithId(id, in);
    }

    public void writeToNoId(StreamOutput out) throws IOException {
        policyValues.writeTo(out);
        qsr.writeToNoId(out);
    }

    /**
     * A class containing information needed for all cache policies
     *  to decide whether to admit a given value.
     *
     *  @opensearch.experimental
     */
    @ExperimentalApi
    public static class PolicyValues implements Writeable {
        final long tookTimeNanos;
        // More values can be added here as they're needed for future policies

        public PolicyValues(long tookTimeNanos) {
            this.tookTimeNanos = tookTimeNanos;
        }

        public PolicyValues(StreamInput in) throws IOException {
            this.tookTimeNanos = in.readZLong();
        }

        public long getTookTimeNanos() {
            return tookTimeNanos;
        }

        @Override
        public void writeTo(StreamOutput out) throws IOException {
            out.writeZLong(tookTimeNanos);
        }
    }
}
