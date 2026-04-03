/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

package org.opensearch.storage.slowlogs;

import org.opensearch.core.common.io.stream.StreamInput;
import org.opensearch.core.common.io.stream.StreamOutput;
import org.opensearch.core.common.io.stream.Writeable;
import org.opensearch.core.xcontent.ToXContentFragment;
import org.opensearch.core.xcontent.XContentBuilder;

import java.io.IOException;

/**
 * Node-level aggregate stats for prefetch operations.
 * Tracks success/failure counts for stored fields and doc values prefetch.
 * Serialized over transport between nodes for node stats API.
 */
public class PrefetchStats implements Writeable, ToXContentFragment {

    private final long storedFieldsPrefetchSuccess;
    private final long storedFieldsPrefetchFailure;
    private final long docValuesPrefetchSuccess;
    private final long docValuesPrefetchFailure;

    /**
     * Constructs a new PrefetchStats with the given counters.
     * @param storedFieldsPrefetchSuccess count of successful stored fields prefetches
     * @param storedFieldsPrefetchFailure count of failed stored fields prefetches
     * @param docValuesPrefetchSuccess count of successful doc values prefetches
     * @param docValuesPrefetchFailure count of failed doc values prefetches
     */
    public PrefetchStats(
        long storedFieldsPrefetchSuccess,
        long storedFieldsPrefetchFailure,
        long docValuesPrefetchSuccess,
        long docValuesPrefetchFailure
    ) {
        this.storedFieldsPrefetchSuccess = storedFieldsPrefetchSuccess;
        this.storedFieldsPrefetchFailure = storedFieldsPrefetchFailure;
        this.docValuesPrefetchSuccess = docValuesPrefetchSuccess;
        this.docValuesPrefetchFailure = docValuesPrefetchFailure;
    }

    /**
     * Constructs PrefetchStats from stream input.
     * @param in the stream input to read from
     * @throws IOException if an I/O error occurs
     */
    public PrefetchStats(StreamInput in) throws IOException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        throw new UnsupportedOperationException("Not yet implemented");
    }

    /** Returns the count of successful stored fields prefetch operations. */
    public long getStoredFieldsPrefetchSuccess() {
        return storedFieldsPrefetchSuccess;
    }

    /** Returns the count of failed stored fields prefetch operations. */
    public long getStoredFieldsPrefetchFailure() {
        return storedFieldsPrefetchFailure;
    }

    /** Returns the count of successful doc values prefetch operations. */
    public long getDocValuesPrefetchSuccess() {
        return docValuesPrefetchSuccess;
    }

    /** Returns the count of failed doc values prefetch operations. */
    public long getDocValuesPrefetchFailure() {
        return docValuesPrefetchFailure;
    }

    static final class Fields {
        static final String PREFETCH_STATS = "prefetch_stats";
        static final String STORED_FIELDS_PREFETCH_SUCCESS = "stored_fields_prefetch_success_count";
        static final String STORED_FIELDS_PREFETCH_FAILURE = "stored_fields_prefetch_failure_count";
        static final String DOC_VALUES_PREFETCH_SUCCESS = "doc_values_prefetch_success_count";
        static final String DOC_VALUES_PREFETCH_FAILURE = "doc_values_prefetch_failure_count";
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        throw new UnsupportedOperationException("Not yet implemented");
    }
}
