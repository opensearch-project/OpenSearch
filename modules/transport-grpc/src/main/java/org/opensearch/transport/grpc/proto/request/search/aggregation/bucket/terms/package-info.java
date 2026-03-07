/*
 * SPDX-License-Identifier: Apache-2.0
 */

/**
 * Protocol Buffer utilities for terms bucket aggregation requests.
 * Contains converters from Protocol Buffer terms aggregation requests to OpenSearch terms aggregation builders.
 * <p>
 * Terms aggregations group documents by unique field values, returning the top terms by document count.
 * This package includes utilities for:
 * <ul>
 *   <li>{@link org.opensearch.transport.grpc.proto.request.search.aggregation.bucket.terms.TermsAggregationProtoUtils} -
 *       Main converter for terms aggregation requests, mirroring {@link org.opensearch.search.aggregations.bucket.terms.TermsAggregationBuilder}</li>
 *   <li>{@link org.opensearch.transport.grpc.proto.request.search.aggregation.bucket.terms.IncludeExcludeProtoUtils} -
 *       Utility for parsing include/exclude filters, mirroring {@link org.opensearch.search.aggregations.bucket.terms.IncludeExclude}</li>
 * </ul>
 *
 * @see org.opensearch.search.aggregations.bucket.terms
 */
package org.opensearch.transport.grpc.proto.request.search.aggregation.bucket.terms;
