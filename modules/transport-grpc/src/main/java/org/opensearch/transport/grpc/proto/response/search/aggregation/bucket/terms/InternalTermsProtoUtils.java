/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.response.search.aggregation.bucket.terms;

import org.opensearch.search.aggregations.bucket.terms.InternalTerms;

import java.util.function.Consumer;

/**
 * Utility class for common terms aggregation response serialization to Protocol Buffer format.
 *
 * <p>This utility mirrors the REST API serialization logic from {@link InternalTerms}, which contains:
 * <ul>
 *   <li>{@link InternalTerms#doXContentCommon} - terms-level serialization</li>
 *   <li>{@link InternalTerms.Bucket#toXContent} - bucket-level serialization</li>
 * </ul>
 *
 * <p>Just as {@link InternalTerms} contains both terms-level and bucket-level serialization logic,
 * this utility provides helpers for both levels to maintain structural parity with REST.
 *
 * @see InternalTerms
 * @see InternalTerms#doXContentCommon
 * @see InternalTerms.Bucket#toXContent
 */
public class InternalTermsProtoUtils {

    private InternalTermsProtoUtils() {
        // Utility class
    }

    // ========================================
    // Terms-level helpers (from InternalTerms.doXContentCommon)
    // ========================================

    /**
     * Sets the doc_count_error_upper_bound field in the protobuf builder (terms-level).
     *
     * <p>Mirrors {@link InternalTerms#doXContentCommon} which sets doc_count_error_upper_bound.
     * This value represents the maximum error in document counts due to shard-level
     * size limitations in distributed terms aggregation.
     *
     * @param docCountError The document count error value from InternalTerms
     * @param setter Consumer that sets the value in the protobuf builder
     */
    public static void setDocCountErrorUpperBound(long docCountError, Consumer<Long> setter) {
        setter.accept(docCountError);
    }

    /**
     * Sets the sum_other_doc_count field in the protobuf builder.
     *
     * <p>Mirrors {@link InternalTerms#doXContentCommon} which sets sum_other_doc_count.
     * This value represents the sum of document counts for terms not included
     * in the top results due to size limitations.
     *
     * @param sumOtherDocCount The sum of other doc counts from InternalTerms
     * @param setter Consumer that sets the value in the protobuf builder
     */
    public static void setSumOtherDocCount(long sumOtherDocCount, Consumer<Long> setter) {
        setter.accept(sumOtherDocCount);
    }

    // ========================================
    // Bucket-level helpers (from InternalTerms.Bucket.toXContent)
    // ========================================

    /**
     * Sets the doc_count field in the protobuf bucket builder.
     *
     * <p>Mirrors {@link InternalTerms.Bucket#toXContent} which sets doc_count.
     *
     * @param docCount The document count from the bucket
     * @param setter Consumer that sets the value in the protobuf bucket builder
     */
    public static void setDocCount(long docCount, Consumer<Long> setter) {
        setter.accept(docCount);
    }

    /**
     * Conditionally sets the doc_count_error_upper_bound field in the protobuf bucket builder.
     *
     * <p>Mirrors {@link InternalTerms.Bucket#toXContent} which conditionally sets doc_count_error_upper_bound
     * when showDocCountError is enabled.
     *
     * @param bucket The InternalTerms bucket
     * @param setter Consumer that sets the value in the protobuf bucket builder
     */
    public static void setDocCountErrorUpperBoundIfApplicable(
        InternalTerms.Bucket<?> bucket,
        Consumer<Long> setter
    ) {
        if (bucket.showDocCountError()) {
            setter.accept(bucket.getDocCountError());
        }
    }
}
