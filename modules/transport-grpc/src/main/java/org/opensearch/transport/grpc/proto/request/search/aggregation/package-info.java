/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * Utilities for converting Protocol Buffer aggregation messages to OpenSearch AggregationBuilder objects.
 *
 * <p>This package provides converters for transforming gRPC/protobuf aggregation requests into
 * OpenSearch's internal aggregation representation. The main entry point is
 * {@link org.opensearch.transport.grpc.proto.request.search.aggregation.AggregationContainerProtoUtils},
 * which dispatches to type-specific converters.
 *
 * <p>The conversion pattern mirrors OpenSearch's REST-side aggregation parsing, where each
 * aggregation type has dedicated conversion logic that transforms protobuf fields into
 * AggregationBuilder method calls.
 */
package org.opensearch.transport.grpc.proto.request.search.aggregation;
