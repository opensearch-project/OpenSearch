/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * Protocol Buffer utilities for converting aggregation requests from proto to OpenSearch objects.
 *
 * <p>This package uses the converter/registry pattern via {@link org.opensearch.transport.grpc.spi.AggregationBuilderProtoConverterRegistry}
 * to route aggregation containers to their type-specific converters with plugin extensibility support.
 *
 * <p>Sub-packages:
 * <ul>
 *   <li>{@code metrics} - Metric aggregations (Min, Max, etc.)</li>
 * </ul>
 */
package org.opensearch.transport.grpc.proto.request.search.aggregation;
