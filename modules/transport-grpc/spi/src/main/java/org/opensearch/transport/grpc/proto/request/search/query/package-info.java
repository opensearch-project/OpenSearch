/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * Service Provider Interface (SPI) for transport-grpc query builder converters.
 *
 * This package provides the core interfaces that external plugins can implement
 * to add custom query types to the gRPC transport layer. The primary interface
 * is {@link org.opensearch.transport.grpc.proto.request.search.query.QueryBuilderProtoConverter}
 * which allows conversion from protobuf query containers to OpenSearch QueryBuilder objects.
 *
 * External plugins should implement QueryBuilderProtoConverter and register their
 * implementations via Java's ServiceLoader mechanism by including their implementation
 * class names in META-INF/services/org.opensearch.transport.grpc.proto.request.search.query.QueryBuilderProtoConverter
 */
package org.opensearch.transport.grpc.proto.request.search.query;
