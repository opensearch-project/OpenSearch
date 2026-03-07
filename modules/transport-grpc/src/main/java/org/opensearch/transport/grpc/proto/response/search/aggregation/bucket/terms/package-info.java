/*
 * SPDX-License-Identifier: Apache-2.0
 */

/**
 * Protocol Buffer utilities for terms bucket aggregation responses.
 * Contains converters from OpenSearch internal terms aggregation results to Protocol Buffer messages.
 * <p>
 * Terms aggregations group documents by unique field values, returning the top terms by document count.
 * This package includes:
 * <ul>
 *   <li>{@link org.opensearch.transport.grpc.proto.response.search.aggregation.bucket.terms.InternalTermsProtoUtils} -
 *       Common helper methods for terms-level and bucket-level serialization, mirroring
 *       {@link org.opensearch.search.aggregations.bucket.terms.InternalTerms}</li>
 *   <li>{@link org.opensearch.transport.grpc.proto.response.search.aggregation.bucket.terms.DoubleTermsProtoUtils} -
 *       Converts {@link org.opensearch.search.aggregations.bucket.terms.DoubleTerms} to DoubleTermsAggregate protobuf</li>
 *   <li>{@link org.opensearch.transport.grpc.proto.response.search.aggregation.bucket.terms.LongTermsProtoUtils} -
 *       Converts {@link org.opensearch.search.aggregations.bucket.terms.LongTerms} to LongTermsAggregate protobuf</li>
 *   <li>{@link org.opensearch.transport.grpc.proto.response.search.aggregation.bucket.terms.StringTermsProtoUtils} -
 *       Converts {@link org.opensearch.search.aggregations.bucket.terms.StringTerms} to StringTermsAggregate protobuf</li>
 *   <li>{@link org.opensearch.transport.grpc.proto.response.search.aggregation.bucket.terms.UnsignedLongTermsProtoUtils} -
 *       Converts {@link org.opensearch.search.aggregations.bucket.terms.UnsignedLongTerms} to UnsignedLongTermsAggregate protobuf</li>
 *   <li>{@link org.opensearch.transport.grpc.proto.response.search.aggregation.bucket.terms.UnmappedTermsProtoUtils} -
 *       Converts {@link org.opensearch.search.aggregations.bucket.terms.UnmappedTerms} to UnmappedTermsAggregate protobuf</li>
 * </ul>
 *
 * @see org.opensearch.search.aggregations.bucket.terms
 */
package org.opensearch.transport.grpc.proto.response.search.aggregation.bucket.terms;
