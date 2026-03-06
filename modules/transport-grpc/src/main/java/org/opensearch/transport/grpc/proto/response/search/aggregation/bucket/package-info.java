/*
 * SPDX-License-Identifier: Apache-2.0
 */

/**
 * Protocol Buffer utilities for bucket aggregation responses.
 * Contains converters from OpenSearch internal bucket aggregation results to Protocol Buffer messages.
 * <p>
 * Bucket aggregations group documents into buckets based on criteria like field values, ranges, or filters.
 * Examples include terms, histogram, filters, and nested aggregations.
 *
 * @see org.opensearch.search.aggregations.bucket
 */
package org.opensearch.transport.grpc.proto.response.search.aggregation.bucket;
