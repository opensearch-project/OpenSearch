/*
 * SPDX-License-Identifier: Apache-2.0
 */

/**
 * Protocol Buffer utilities for common aggregation support functionality.
 * Contains shared converters and utilities used across multiple aggregation types.
 * <p>
 * This package provides base functionality for aggregations that share common configuration,
 * mirroring the support package in OpenSearch core aggregations.
 * <p>
 * Key utilities:
 * <ul>
 *   <li>{@link org.opensearch.transport.grpc.proto.request.search.aggregation.support.ValuesSourceAggregationProtoUtils} -
 *       Common field parsing for values-source aggregations (field, missing, script, format), mirroring
 *       {@link org.opensearch.search.aggregations.support.ValuesSourceAggregationBuilder}</li>
 * </ul>
 *
 * @see org.opensearch.search.aggregations.support
 */
package org.opensearch.transport.grpc.proto.request.search.aggregation.support;
