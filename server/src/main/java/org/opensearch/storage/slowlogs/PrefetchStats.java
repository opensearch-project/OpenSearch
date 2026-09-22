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
 * Stats for prefetch operations on tiered storage.
 *
 * @opensearch.experimental
 */
public class PrefetchStats implements Writeable, ToXContentFragment {

    private final long storedFieldsPrefetchSuccess;
    private final long storedFieldsPrefetchFailure;
    private final long docValuesPrefetchSuccess;
    private final long docValuesPrefetchFailure;

    /**
     * Creates a new PrefetchStats instance.
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
     * Creates a new PrefetchStats instance from a stream.
     * @param in the stream input
     * @throws IOException if an I/O error occurs
     */
    public PrefetchStats(StreamInput in) throws IOException {
        storedFieldsPrefetchSuccess = in.readVLong();
        storedFieldsPrefetchFailure = in.readVLong();
        docValuesPrefetchSuccess = in.readVLong();
        docValuesPrefetchFailure = in.readVLong();
    }

    @Override
    public void writeTo(StreamOutput out) throws IOException {
        out.writeVLong(storedFieldsPrefetchSuccess);
        out.writeVLong(storedFieldsPrefetchFailure);
        out.writeVLong(docValuesPrefetchSuccess);
        out.writeVLong(docValuesPrefetchFailure);
    }

    /**
     * Returns the count of successful stored fields prefetches.
     * @return the success count
     */
    public long getStoredFieldsPrefetchSuccess() {
        return storedFieldsPrefetchSuccess;
    }

    /**
     * Returns the count of failed stored fields prefetches.
     * @return the failure count
     */
    public long getStoredFieldsPrefetchFailure() {
        return storedFieldsPrefetchFailure;
    }

    /**
     * Returns the count of successful doc values prefetches.
     * @return the success count
     */
    public long getDocValuesPrefetchSuccess() {
        return docValuesPrefetchSuccess;
    }

    /**
     * Returns the count of failed doc values prefetches.
     * @return the failure count
     */
    public long getDocValuesPrefetchFailure() {
        return docValuesPrefetchFailure;
    }

    /**
     * Field names for XContent serialization.
     *
     * @opensearch.experimental
     */
    static final class Fields {
        static final String PREFETCH_STATS = "prefetch_stats";
        static final String STORED_FIELDS_PREFETCH_SUCCESS = "stored_fields_prefetch_success_count";
        static final String STORED_FIELDS_PREFETCH_FAILURE = "stored_fields_prefetch_failure_count";
        static final String DOC_VALUES_PREFETCH_SUCCESS = "doc_values_prefetch_success_count";
        static final String DOC_VALUES_PREFETCH_FAILURE = "doc_values_prefetch_failure_count";
    }

    @Override
    public XContentBuilder toXContent(XContentBuilder builder, Params params) throws IOException {
        builder.startObject(Fields.PREFETCH_STATS);
        builder.field(Fields.STORED_FIELDS_PREFETCH_SUCCESS, getStoredFieldsPrefetchSuccess());
        builder.field(Fields.STORED_FIELDS_PREFETCH_FAILURE, getStoredFieldsPrefetchFailure());
        builder.field(Fields.DOC_VALUES_PREFETCH_SUCCESS, getDocValuesPrefetchSuccess());
        builder.field(Fields.DOC_VALUES_PREFETCH_FAILURE, getDocValuesPrefetchFailure());
        builder.endObject();
        return builder;
    }
}
