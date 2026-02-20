/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * Utilities for converting OpenSearch InternalAggregation objects to Protocol Buffer Aggregate messages.
 *
 * <p>This package provides converters for transforming OpenSearch's internal aggregation results into
 * gRPC/protobuf aggregation responses. The main entry point is
 * {@link org.opensearch.transport.grpc.proto.response.search.aggregation.AggregateProtoUtils},
 * which dispatches to type-specific converters.
 *
 * <p>The conversion pattern mirrors OpenSearch's request-side aggregation parsing, where each
 * aggregation type has dedicated conversion logic that transforms OpenSearch internal objects into
 * protobuf messages.
 */
package org.opensearch.transport.grpc.proto.response.search.aggregation;
