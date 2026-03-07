/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */
package org.opensearch.transport.grpc.proto.request.search.aggregation.bucket.terms;

import org.opensearch.protobufs.TermsInclude;
import org.opensearch.search.aggregations.bucket.terms.IncludeExclude;
import org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder;

/**
 * Utility class for converting Protocol Buffer include/exclude messages to OpenSearch IncludeExclude objects.
 *
 * <p>This utility mirrors {@link IncludeExclude#parseInclude} and {@link IncludeExclude#parseExclude}
 * but for Protocol Buffer messages instead of XContent (JSON/YAML).
 *
 * @see IncludeExclude
 * @see IncludeExclude#parseInclude
 * @see IncludeExclude#parseExclude
 * @see IncludeExclude#merge
 * @see org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder#PARSER
 */
public class IncludeExcludeProtoUtils {

    private IncludeExcludeProtoUtils() {
        // Utility class
    }

    /**
     * Converts protobuf TermsInclude to OpenSearch IncludeExclude for include field.
     *
     * <p>Mirrors {@link IncludeExclude#parseInclude} which supports three formats:
     * string (regex), array (term list), or object (partition).
     *
     * @param includeProto The protobuf TermsInclude to convert
     * @return An IncludeExclude instance for include (exclude part is null), or null if not set
     * @throws IllegalArgumentException if partition/num_partitions are missing or format is invalid
     */
    public static IncludeExclude parseInclude(TermsInclude includeProto) {
        switch (includeProto.getTermsIncludeCase()) {
            case TERMS:
                // Array format: ["term1", "term2", ...] -> new IncludeExclude(includeSet, null)
                String[] includeValues = includeProto.getTerms().getStringArrayList().toArray(new String[0]);
                return new IncludeExclude(includeValues, null);

            case PARTITION:
                // Object format: {"partition": N, "num_partitions": M}
                // Validate required fields (mirrors REST validation)
                int numPartitions = includeProto.getPartition().getNumPartitions();
                int partition = includeProto.getPartition().getPartition();

                // Partition can be 0 (first partition) but should not be negative
                if (partition < 0) {
                    throw new IllegalArgumentException(
                        "Missing [partition] parameter for partition-based include"
                    );
                }

                // Mirrors IncludeExclude.parseInclude() validation
                if (numPartitions <= 0) {
                    throw new IllegalArgumentException(
                        "Missing [num_partitions] parameter for partition-based include"
                    );
                }

                return new IncludeExclude(partition, numPartitions);

            case TERMSINCLUDE_NOT_SET:
            default:
                throw new IllegalArgumentException(
                    "Unrecognized token for an include [" + includeProto.getTermsIncludeCase() + "]"
                );
        }
    }

    /**
     * Converts protobuf exclude string array to OpenSearch IncludeExclude for exclude field.
     *
     * <p>Mirrors {@link IncludeExclude#parseExclude} which supports string (regex) or array (term list) formats.
     *
     * @param excludeList The protobuf exclude string list
     * @return An IncludeExclude instance for exclude (include part is null), or null if list is empty
     * @throws IllegalArgumentException if excludeList is null
     */
    public static IncludeExclude parseExclude(java.util.List<String> excludeList) {
        // Array format: ["term1", "term2", ...] -> new IncludeExclude(null, excludeSet)
        String[] excludeArray = excludeList.toArray(new String[0]);
        return new IncludeExclude(null, excludeArray);
    }
}
