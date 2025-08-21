/*
 * SPDX-License-Identifier: Apache-2.0
 *
 * The OpenSearch Contributors require contributions made to
 * this file be licensed under the Apache-2.0 license or a
 * compatible open source license.
 */

/**
 * Service Provider Interface (SPI) for extending gRPC transport query conversion capabilities.
 * <p>
 * This package provides the core interfaces and registry for implementing custom query converters
 * that can transform protobuf query messages into OpenSearch QueryBuilder objects. External plugins
 * can implement the {@link org.opensearch.transport.grpc.spi.QueryBuilderProtoConverter} interface
 * to add support for custom query types in gRPC requests.
 * </p>
 * <p>
 * The SPI mechanism leverages OpenSearch's {@code ExtensiblePlugin} framework, which internally
 * uses Java's {@code ServiceLoader} for discovery. Plugins must:
 * </p>
 * <ul>
 *   <li>Implement {@link org.opensearch.transport.grpc.spi.QueryBuilderProtoConverter}</li>
 *   <li>Return the converter instance from their plugin's {@code createComponents()} method</li>
 *   <li>Create a {@code META-INF/services} file listing their converter implementation</li>
 *   <li>Declare {@code transport-grpc} in their plugin descriptor's {@code extended.plugins} list</li>
 * </ul>
 * <p>
 * For converters that need to handle nested queries (e.g., filter clauses), the registry injection
 * pattern allows access to built-in converters for standard query types like MatchAll, Term, and Terms.
 * </p>
 *
 * @since 3.2.0
 */
package org.opensearch.transport.grpc.spi;
